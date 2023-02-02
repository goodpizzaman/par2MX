//  This file is part of par2cmdline (a PAR 2.0 compatible file verification and
//  repair tool). See http://parchive.sourceforge.net for details of PAR 2.0.
//
//  Copyright (c) 2003 Peter Brian Clements
//
//  par2cmdline is free software; you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation; either version 2 of the License, or
//  (at your option) any later version.
//
//  par2cmdline is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

#include "par2cmdline.h"
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/sysctl.h>
#ifdef PROFILE
#include <TimeReporter.h>
#endif

#include "OSXStuff.h"

Par2Repairer::Par2Repairer(void)
{
  firstpacket = true;
  mainpacket = 0;
  creatorpacket = 0;

  blocksize = 0;
  sourceblockcount = 0;

  blocksallocated = false;

  availableblockcount = 0;
  missingblockcount = 0;

  completefilecount = 0;
  renamedfilecount = 0;
  damagedfilecount = 0;
  missingfilecount = 0;

  inputbuffer = 0;
  outputbuffer = 0;

  noiselevel = CommandLine::nlNormal;
	
	// Some stuff for the multi-thread operation
  previouslyReportedProgress = 0;
  genericSema = dispatch_semaphore_create(1);
}

Par2Repairer::~Par2Repairer(void)
{
  delete [] (u8*)inputbuffer;
  delete [] (u8*)outputbuffer;

  map<u32,RecoveryPacket*>::iterator rp = recoverypacketmap.begin();
  while (rp != recoverypacketmap.end())
  {
    delete (*rp).second;

    ++rp;
  }

  map<MD5Hash,Par2RepairerSourceFile*>::iterator sf = sourcefilemap.begin();
  while (sf != sourcefilemap.end())
  {
    Par2RepairerSourceFile *sourcefile = (*sf).second;
    delete sourcefile;

    ++sf;
  }

  delete mainpacket;
  delete creatorpacket;
	
  dispatch_release(genericSema);
}

Result Par2Repairer::Process(const CommandLine &commandline, bool dorepair)
{
#ifdef PROFILE
  TimeReporter::MarkTime("Start Par2Repairer::Process");
#endif

  // What noiselevel are we using
  noiselevel = commandline.GetNoiseLevel();

  struct rlimit rlp;		// Need this to allow for enough file handles
  int 	lFileHandlesNeeded;

  // Get filesnames from the command line
  string par2filename = commandline.GetParFilename();
  const list<CommandLine::ExtraFile> &extrafiles = commandline.GetExtraFiles();

  // Determine the searchpath from the location of the main PAR2 file
  string name;
  DiskFile::SplitFilename(par2filename, searchpath, name);

  // Load packets from the main PAR2 file
  if (!LoadPacketsFromFile(searchpath + name))
    return eLogicError;

  // Load packets from other PAR2 files with names based on the original PAR2 file
  if (!LoadPacketsFromOtherFiles(par2filename))
    return eLogicError;

  // Load packets from any other PAR2 files whose names are given on the command line
  if (!LoadPacketsFromExtraFiles(extrafiles))
    return eLogicError;

  if (noiselevel > CommandLine::nlQuiet)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << endl;
    dispatch_semaphore_signal(coutSema);
  }

  // Check that the packets are consistent and discard any that are not
  if (!CheckPacketConsistency())
    return eInsufficientCriticalData;

  // It appears that during repair the program opens all files simultaneously.
  // Suppose the maximum number of file handles is 256, this would mean you can have
  // about 250 files in a par set. This can be too small, so adjust if necessary. */
  if (getrlimit (RLIMIT_NOFILE, &rlp) != 0)
    return eLogicError;

  lFileHandlesNeeded = mainpacket->TotalFileCount() + 16;		// A few extra
  if (rlp.rlim_cur < (unsigned int) lFileHandlesNeeded)
  {
    rlp.rlim_cur = lFileHandlesNeeded;
    if (setrlimit (RLIMIT_NOFILE, &rlp) != 0)
      return eLogicError;
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << "Increased file limit to " << lFileHandlesNeeded << endl;
    dispatch_semaphore_signal(coutSema);
  }
  
  // Use the information in the main packet to get the source files
  // into the correct order and determine their filenames
  if (!CreateSourceFileList())
    return eLogicError;

  // Determine the total number of DataBlocks for the recoverable source files
  // The allocate the DataBlocks and assign them to each source file
  if (!AllocateSourceBlocks())
    return eLogicError;

  // Create a verification hash table for all files for which we have not
  // found a complete version of the file and for which we have
  // a verification packet
  if (!PrepareVerificationHashTable())
    return eLogicError;

  // Compute the table for the sliding CRC computation
  if (!ComputeWindowTable())
    return eLogicError;

  if (noiselevel > CommandLine::nlQuiet)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << endl << "Verifying source files:" << endl << endl;
    dispatch_semaphore_signal(coutSema);
  }

  // Attempt to verify all of the source files
  if (!VerifySourceFiles())
    return eFileIOError;

  // Find out how much data we have found
  UpdateVerificationResults();

  if (completefilecount < mainpacket->RecoverableFileCount())
  {
    if (noiselevel > CommandLine::nlQuiet)
    {
      dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
      cout << endl << "Scanning extra files:" << endl << endl;
      dispatch_semaphore_signal(coutSema);
    }

    // Scan any extra files specified on the command line
    if (!VerifyExtraFiles(extrafiles))
      return eLogicError;

    UpdateVerificationResults();
  }

  if (noiselevel > CommandLine::nlSilent)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << endl;
    dispatch_semaphore_signal(coutSema);
  }

#ifdef PROFILE
  TimeReporter::PrintTime("Verification finished", true);
#endif

  // Check the verification results and report the results
  if (!CheckVerificationResults())
    return eRepairNotPossible;

  // Are any of the files incomplete
  if (completefilecount<mainpacket->RecoverableFileCount())
  {
    // Do we want to carry out a repair
    if (dorepair)
    {
#ifdef PROFILE
      TimeReporter::MarkTime("Start of Repair processing");
#endif
      if (noiselevel > CommandLine::nlSilent)
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cout << endl;
        dispatch_semaphore_signal(coutSema);
      }

      // Rename any damaged or missnamed target files.
      if (!RenameTargetFiles())
        return eFileIOError;

      // Are we still missing any files
      if (completefilecount<mainpacket->RecoverableFileCount())
      {
        // Work out which files are being repaired, create them, and allocate
        // target DataBlocks to them, and remember them for later verification.
        if (!CreateTargetFiles())
          return eFileIOError;

        // Work out which data blocks are available, which need to be copied
        // directly to the output, and which need to be recreated, and compute
        // the appropriate Reed Solomon matrix.
        if (!ComputeRSmatrix())
        {
          // Delete all of the partly reconstructed files
          DeleteIncompleteTargetFiles();
          return eFileIOError;
        }

        if (noiselevel > CommandLine::nlSilent)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << endl;
          dispatch_semaphore_signal(coutSema);
        }

        // Allocate memory buffers for reading and writing data to disk.
        if (!AllocateBuffers(commandline.GetMemoryLimit()))
        {
          // Delete all of the partly reconstructed files
          DeleteIncompleteTargetFiles();
          return eMemoryError;
        }

        // Set the total amount of data to be processed.
        progress = 0;
        this->previouslyReportedProgress = -10000000;	// Big negative
        totaldata = blocksize * sourceblockcount * (missingblockcount > 0 ? missingblockcount : 1);

        // Start at an offset of 0 within a block.
        u64 blockoffset = 0;
        while (blockoffset < blocksize) // Continue until the end of the block.
        {
          // Work out how much data to process this time.
          size_t blocklength = (size_t)min((u64)chunksize, blocksize-blockoffset);

          // Read source data, process it through the RS matrix and write it to disk.
          if (!ProcessData(blockoffset, blocklength))
          {
            // Delete all of the partly reconstructed files
            DeleteIncompleteTargetFiles();
            return eFileIOError;
          }

          // Advance to the need offset within each block
          blockoffset += blocklength;
        }

#ifdef PROFILE
        TimeReporter::PrintTime("Repair finished", true);
#endif

        if (noiselevel > CommandLine::nlSilent)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << endl << "Verifying repaired files:" << endl << endl;
          dispatch_semaphore_signal(coutSema);
        }

        // Verify that all of the reconstructed target files are now correct
        if (!VerifyTargetFiles())
        {
          // Delete all of the partly reconstructed files
          DeleteIncompleteTargetFiles();
          return eFileIOError;
        }
      }

      // Are all of the target files now complete?
      if (completefilecount<mainpacket->RecoverableFileCount())
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cerr << "Repair Failed." << endl;
        dispatch_semaphore_signal(coutSema);
        return eRepairFailed;
      }
      else
      {
        if (noiselevel > CommandLine::nlSilent)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << endl << "Repair complete." << endl;
          dispatch_semaphore_signal(coutSema);
        }
      }
    }
    else
    {
      return eRepairPossible;
    }
  }

  return eSuccess;
}

// Load the packets from the specified file
bool Par2Repairer::LoadPacketsFromFile(string filename)
{
  // Skip the file if it has already been processed
  if (diskFileMap.Find(filename) != 0)
  {
    return true;
  }

  DiskFile *diskfile = new DiskFile;

  // Open the file
  if (!diskfile->Open(filename, true))  // true: expect to read all data of the file
  {
    // If we could not open the file, ignore the error and 
    // proceed to the next file
    delete diskfile;
    return true;
  }

  if (noiselevel > CommandLine::nlSilent)
  {
    string path;
    string name;
    DiskFile::SplitFilename(filename, path, name);
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << "Loading \"" << DiskFile::FS2UTF8(name) << "\"." << endl;
    dispatch_semaphore_signal(coutSema);
  }

  // How many useable packets have we found
  u32 packets = 0;

  // How many recovery packets were there
  u32 recoverypackets = 0;

  // How big is the file
  u64 filesize = diskfile->FileSize();
  if (filesize > 0)
  {
    // Allocate a buffer to read data into
    // The buffer should be large enough to hold a whole 
    // critical packet (i.e. file verification, file description, main,
    // and creator), but not necessarily a whole recovery packet.
    size_t buffersize = (size_t)min((u64)(1024*1024*10), filesize);
    u8 *buffer = new u8[buffersize];

#ifndef MPDL
    // Progress indicator
    u64 progress = 0;
#endif
  
    // Start at the beginning of the file
    u64 offset = 0;

    // Continue as long as there is at least enough for the packet header
    while (offset + sizeof(PACKET_HEADER) <= filesize)
    {
      // Define MPDL to suppress the percentages, because it slows things down considerably.
#ifndef MPDL
      if (noiselevel > CommandLine::nlQuiet)
      {
        // Update a progress indicator
        u32 oldfraction = (u32)(1000 * progress / filesize);
        u32 newfraction = (u32)(1000 * offset / filesize);
        if (oldfraction != newfraction)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "Loading: " << newfraction/10 << '.' << newfraction%10 << "%\r" << flush;
          dispatch_semaphore_signal(coutSema);
          progress = offset;
        }
      }
#endif
      // Attempt to read the next packet header
      PACKET_HEADER header;
      if (!this->ReadPacketHeader(diskfile, offset, buffer, buffersize, header))
        break;

      // We have found the magic. Now check the packet length.
      if (sizeof(PACKET_HEADER) > header.length || // packet length is too small
          0 != (header.length & 3) ||              // packet length is not a multiple of 4
          filesize < offset + header.length)       // packet would extend beyond the end of the file
      {
        offset++;
        continue;
      }

      // Compute the MD5 Hash of the packet
      MD5Context context;
      context.Update(&header.setid, sizeof(header)-offsetof(PACKET_HEADER, setid));

      // How much more do I need to read to get the whole packet
      u64 current = offset+sizeof(PACKET_HEADER); // Continue beyond packet header
      u64 limit = offset+header.length;
      while (current < limit)
      {
        size_t want = (size_t)min((u64)buffersize, limit-current);

        if (!diskfile->Read(current, buffer, want))
          break;

        context.Update(buffer, want);

        current += want;
      }

      // Did the whole packet get processed
      if (current<limit)
      {
        offset++;
        continue;
      }

      // Check the calculated packet hash against the value in the header
      MD5Hash hash;
      context.Final(hash);
      if (hash != header.hash)
      {
        offset++;
        continue;
      }

      // If this is the first packet that we have found then record the setid
      if (firstpacket)
      {
        setid = header.setid;
        firstpacket = false;
      }

      // Is the packet from the correct set
      if (setid == header.setid)
      {
        // Is it a packet type that we are interested in
        if (recoveryblockpacket_type == header.type)
        {
          if (LoadRecoveryPacket(diskfile, offset, header))
          {
            recoverypackets++;
            packets++;
          }
        }
        else if (fileverificationpacket_type == header.type)
        {
          if (LoadVerificationPacket(diskfile, offset, header))
          {
            packets++;
          }
        }
        else if (filedescriptionpacket_type == header.type)
        {
          if (LoadDescriptionPacket(diskfile, offset, header))
          {
            packets++;
          }
        }
        else if (mainpacket_type == header.type)
        {
          if (LoadMainPacket(diskfile, offset, header))
          {
            packets++;
          }
        }
        else if (creatorpacket_type == header.type)
        {
          if (LoadCreatorPacket(diskfile, offset, header))
          {
            packets++;
          }
        }
      }

      // Advance to the next packet
      offset += header.length;
    }

    delete [] buffer;
  }

  // We have finished with the file for now
  diskfile->Close();

  // Did we actually find any interesting packets
  if (packets > 0)
  {
    if (noiselevel > CommandLine::nlQuiet)
    {
      dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
      cout << "Loaded " << packets << " new packets";
      if (recoverypackets > 0) cout << " including " << recoverypackets << " recovery blocks";
      cout << endl;
      dispatch_semaphore_signal(coutSema);
    }

    // Remember that the file was processed
#ifdef DEBUG
    bool success = 
#endif
    diskFileMap.Insert(diskfile);
    assert(success);
  }
  else
  {
    if (noiselevel > CommandLine::nlQuiet)
    {
      dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
      cout << "No new packets found" << endl;
      dispatch_semaphore_signal(coutSema);
    }
    delete diskfile;
  }
  
  return true;
}

bool Par2Repairer::ReadPacketHeader(DiskFile *diskfile, u64 &offset, u8 *buffer, size_t buffersize,
                                    PACKET_HEADER &header)
{
  // Attempt to read the next packet header. Return true if successful.
  // If return true, then the offset points to the header start in the file.
  if (!diskfile->Read(offset, &header, sizeof(header)))
    return false;
  
  // Does this look like it might be a packet?
  if (packet_magic == header.magic)
    return true;

  // No, it doesn't look like a header. So skip ahead and try to find next occurrence.
  offset++;
  
  u64 filesize = diskfile->FileSize();
  
  // Is there still enough for at least a whole packet header
  while (offset + sizeof(PACKET_HEADER) <= filesize)
  {
    // How much can we read into the buffer
    size_t want = (size_t)min((u64)buffersize, filesize-offset);
    
    // Fill the buffer
    if (!diskfile->Read(offset, buffer, want))
    {
      offset = filesize;
      return false;   // I/O error
    }
    
    // Scan the buffer for the magic value
    u8 *current = buffer;
    u8 *limit = &buffer[want-sizeof(PACKET_HEADER)];
    while (current <= limit && packet_magic != ((PACKET_HEADER*)current)->magic)
    {
      current++;
    }
    
    // What file offset did we reach
    offset += current - buffer;
    
    // Did we find the magic
    if (current <= limit)
    {
      // Yep, we did
      memcpy(&header, current, sizeof(header));
      return true;
    }
  } // end while more space in the file
  
  // Apparently we stopped because the file was exhausted without finding anything
  return false;
}

// Finish loading a recovery packet
bool Par2Repairer::LoadRecoveryPacket(DiskFile *diskfile, u64 offset, PACKET_HEADER &header)
{
  RecoveryPacket *packet = new RecoveryPacket;

  // Load the packet from disk
  if (!packet->Load(diskfile, offset, header))
  {
    delete packet;
    return false;
  }

  // What is the exponent value of this recovery packet
  u32 exponent = packet->Exponent();

  // Try to insert the new packet into the recovery packet map
  pair<map<u32,RecoveryPacket*>::const_iterator, bool> location = recoverypacketmap.insert(pair<u32,RecoveryPacket*>(exponent, packet));

  // Did the insert fail
  if (!location.second)
  {
    // The packet must be a duplicate of one we already have
    delete packet;
    return false;
  }

  return true;
}

// Finish loading a file description packet
bool Par2Repairer::LoadDescriptionPacket(DiskFile *diskfile, u64 offset, PACKET_HEADER &header)
{
  DescriptionPacket *packet = new DescriptionPacket;

  // Load the packet from disk
  if (!packet->Load(diskfile, offset, header))
  {
    delete packet;
    return false;
  }

  // What is the fileid
  const MD5Hash &fileid = packet->FileId();

  // Look up the fileid in the source file map for an existing source file entry
  map<MD5Hash, Par2RepairerSourceFile*>::iterator sfmi = sourcefilemap.find(fileid);
  Par2RepairerSourceFile *sourcefile = (sfmi == sourcefilemap.end()) ? 0 :sfmi->second;

  // Was there an existing source file
  if (sourcefile)
  {
    // Does the source file already have a description packet
    if (sourcefile->GetDescriptionPacket())
    {
      // Yes. We don't need another copy
      delete packet;
      return false;
    }
    else
    {
      // No. Store the packet in the source file
      sourcefile->SetDescriptionPacket(packet);
      return true;
    }
  }
  else
  {
    // Create a new source file for the packet
    sourcefile = new Par2RepairerSourceFile(packet, NULL);

    // Record the source file in the source file map
    sourcefilemap.insert(pair<MD5Hash, Par2RepairerSourceFile*>(fileid, sourcefile));

    return true;
  }
}

// Finish loading a file verification packet
bool Par2Repairer::LoadVerificationPacket(DiskFile *diskfile, u64 offset, PACKET_HEADER &header)
{
  VerificationPacket *packet = new VerificationPacket;

  // Load the packet from disk
  if (!packet->Load(diskfile, offset, header))
  {
    delete packet;
    return false;
  }

  // What is the fileid
  const MD5Hash &fileid = packet->FileId();

  // Look up the fileid in the source file map for an existing source file entry
  map<MD5Hash, Par2RepairerSourceFile*>::iterator sfmi = sourcefilemap.find(fileid);
  Par2RepairerSourceFile *sourcefile = (sfmi == sourcefilemap.end()) ? 0 :sfmi->second;

  // Was there an existing source file
  if (sourcefile)
  {
    // Does the source file already have a verification packet
    if (sourcefile->GetVerificationPacket())
    {
      // Yes. We don't need another copy.
      delete packet;
      return false;
    }
    else
    {
      // No. Store the packet in the source file
      sourcefile->SetVerificationPacket(packet);

      return true;
    }
  }
  else
  {
    // Create a new source file for the packet
    sourcefile = new Par2RepairerSourceFile(NULL, packet);

    // Record the source file in the source file map
    sourcefilemap.insert(pair<MD5Hash, Par2RepairerSourceFile*>(fileid, sourcefile));

    return true;
  }
}

// Finish loading the main packet
bool Par2Repairer::LoadMainPacket(DiskFile *diskfile, u64 offset, PACKET_HEADER &header)
{
  // Do we already have a main packet
  if (0 != mainpacket)
    return false;

  MainPacket *packet = new MainPacket;

  // Load the packet from disk;
  if (!packet->Load(diskfile, offset, header))
  {
    delete packet;
    return false;
  }

  mainpacket = packet;

  return true;
}

// Finish loading the creator packet
bool Par2Repairer::LoadCreatorPacket(DiskFile *diskfile, u64 offset, PACKET_HEADER &header)
{
  // Do we already have a creator packet
  if (0 != creatorpacket)
    return false;

  CreatorPacket *packet = new CreatorPacket;

  // Load the packet from disk;
  if (!packet->Load(diskfile, offset, header))
  {
    delete packet;
    return false;
  }

  creatorpacket = packet;

  return true;
}

// Load packets from other PAR2 files with names based on the original PAR2 file
bool Par2Repairer::LoadPacketsFromOtherFiles(string filename)
{
  // Split the original PAR2 filename into path and name parts
  string path;
  string name;
  DiskFile::SplitFilename(filename, path, name);

  string::size_type where;

  // Trim ".par2" off of the end original name

  // Look for the last "." in the filename
  while (string::npos != (where = name.find_last_of('.')))
  {
    // Trim what follows the last .
    string tail = name.substr(where+1);
    name = name.substr(0,where);

    // Was what followed the last "." "par2"
    if (0 == stricmp(tail.c_str(), "par2"))
      break;
  }

  // If what is left ends in ".volNNN-NNN" or ".volNNN+NNN" strip that as well

  // Is there another "."
  if (string::npos != (where = name.find_last_of('.')))
  {
    // What follows the "."
    string tail = name.substr(where+1);

    // Scan what follows the last "." to see of it matches vol123-456 or vol123+456
    int n = 0;
    string::const_iterator p;
    for (p=tail.begin(); p!=tail.end(); ++p)
    {
      char ch = *p;

      if (0 == n)
      {
        if (tolower(ch) == 'v') { n++; } else { break; }
      }
      else if (1 == n)
      {
        if (tolower(ch) == 'o') { n++; } else { break; }
      }
      else if (2 == n)
      {
        if (tolower(ch) == 'l') { n++; } else { break; }
      }
      else if (3 == n)
      {
        if (isdigit(ch)) {} else if (ch == '-' || ch == '+') { n++; } else { break; }
      }
      else if (4 == n)
      {
        if (isdigit(ch)) {} else { break; }
      }
    }

    // If we matched then retain only what preceeds the "."
    if (p == tail.end())
    {
      name = name.substr(0,where);
    }
  }

  // Find files called "*.par2" or "name.*.par2"

  {
    string wildcard = name.empty() ? "*.par2" : name + ".*.par2";
    list<string> *files = DiskFile::FindFiles(path, wildcard);

    // Load packets from each file that was found
    for (list<string>::const_iterator s=files->begin(); s!=files->end(); ++s)
    {
      LoadPacketsFromFile(*s);
    }

    delete files;
  }

  {
    string wildcard = name.empty() ? "*.PAR2" : name + ".*.PAR2";
    list<string> *files = DiskFile::FindFiles(path, wildcard);

    // Load packets from each file that was found
    for (list<string>::const_iterator s=files->begin(); s!=files->end(); ++s)
    {
      LoadPacketsFromFile(*s);
    }

    delete files;
  }

  return true;
}

// Load packets from any other PAR2 files whose names are given on the command line
bool Par2Repairer::LoadPacketsFromExtraFiles(const list<CommandLine::ExtraFile> &extrafiles)
{
  for (ExtraFileIterator i=extrafiles.begin(); i!=extrafiles.end(); i++)
  {
    string filename = i->FileName();

    // If the filename contains ".par2" anywhere
    if (string::npos != filename.find(".par2") ||
        string::npos != filename.find(".PAR2"))
    {
      LoadPacketsFromFile(filename);
    }
  }

  return true;
}

// Check that the packets are consistent and discard any that are not
bool Par2Repairer::CheckPacketConsistency(void)
{
  // Do we have a main packet
  if (0 == mainpacket)
  {
    // If we don't have a main packet, then there is nothing more that we can do.
    // We cannot verify or repair any files.

    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cerr << "Main packet not found." << endl;
    dispatch_semaphore_signal(coutSema);
    return false;
  }

  // Remember the block size from the main packet
  blocksize = mainpacket->BlockSize();

  // Check that the recovery blocks have the correct amount of data
  // and discard any that don't
  {
    map<u32,RecoveryPacket*>::iterator rp = recoverypacketmap.begin();
    while (rp != recoverypacketmap.end())
    {
      if (rp->second->BlockSize() == blocksize)
      {
        ++rp;
      }
      else
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cerr << "Incorrect sized recovery block for exponent " << rp->second->Exponent() << " discarded" << endl;
        dispatch_semaphore_signal(coutSema);

        delete rp->second;
        map<u32,RecoveryPacket*>::iterator x = rp++;
        recoverypacketmap.erase(x);
      }
    }
  }

  // Check for source files that have no description packet or where the
  // verification packet has the wrong number of entries and discard them.
  {
    map<MD5Hash, Par2RepairerSourceFile*>::iterator sf = sourcefilemap.begin();
    while (sf != sourcefilemap.end())
    {
      // Do we have a description packet
      DescriptionPacket *descriptionpacket = sf->second->GetDescriptionPacket();
      if (descriptionpacket == 0)
      {
        // No description packet

        // Discard the source file
        delete sf->second;
        map<MD5Hash, Par2RepairerSourceFile*>::iterator x = sf++;
        sourcefilemap.erase(x);

        continue;
      }

      // Compute and store the block count from the filesize and blocksize
      sf->second->SetBlockCount(blocksize);

      // Do we have a verification packet
      VerificationPacket *verificationpacket = sf->second->GetVerificationPacket();
      if (verificationpacket == 0)
      {
        // No verification packet

        // That is ok, but we won't be able to use block verification.

        // Proceed to the next file.
        ++sf;

        continue;
      }

      // Work out the block count for the file from the file size
      // and compare that with the verification packet
      u64 filesize = descriptionpacket->FileSize();
      u32 blockcount = verificationpacket->BlockCount();

      if ((filesize + blocksize-1) / blocksize != (u64)blockcount)
      {
        // The block counts are different!

        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cerr << "Incorrectly sized verification packet for \"" << DiskFile::FS2UTF8(descriptionpacket->FileName())
             << "\" discarded" << endl;
        dispatch_semaphore_signal(coutSema);

        // Discard the source file

        delete sf->second;
        map<MD5Hash, Par2RepairerSourceFile*>::iterator x = sf++;
        sourcefilemap.erase(x);

        continue;
      }

      // Everything is ok.

      // Proceed to the next file
      ++sf;
    }
  }

  if (noiselevel > CommandLine::nlQuiet)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << "There are " 
         << mainpacket->RecoverableFileCount()
         << " recoverable files and "
         << mainpacket->TotalFileCount() - mainpacket->RecoverableFileCount()
         << " other files." 
         << endl;

    cout << "The block size used was "
         << blocksize
         << " bytes."
         << endl;
    dispatch_semaphore_signal(coutSema);
  }

  return true;
}

// Use the information in the main packet to get the source files
// into the correct order and determine their filenames
bool Par2Repairer::CreateSourceFileList(void)
{
  // For each FileId entry in the main packet
  for (u32 filenumber=0; filenumber<mainpacket->TotalFileCount(); filenumber++)
  {
    const MD5Hash &fileid = mainpacket->FileId(filenumber);

    // Look up the fileid in the source file map
    map<MD5Hash, Par2RepairerSourceFile*>::iterator sfmi = sourcefilemap.find(fileid);
    Par2RepairerSourceFile *sourcefile = (sfmi == sourcefilemap.end()) ? 0 :sfmi->second;

    if (sourcefile)
    {
      sourcefile->ComputeTargetFileName(searchpath);
    }

    sourcefiles.push_back(sourcefile);
  }

  return true;
}

// Determine the total number of DataBlocks for the recoverable source files
// The allocate the DataBlocks and assign them to each source file
bool Par2Repairer::AllocateSourceBlocks(void)
{
  sourceblockcount = 0;

  u32 filenumber = 0;
  vector<Par2RepairerSourceFile*>::iterator sf = sourcefiles.begin();

  // For each recoverable source file
  while (filenumber < mainpacket->RecoverableFileCount() && sf != sourcefiles.end())
  {
    // Do we have a source file
    Par2RepairerSourceFile *sourcefile = *sf;
    if (sourcefile)
    {
      sourceblockcount += sourcefile->BlockCount();
    }
    else
    {
      // No details for this source file so we don't know what the
      // total number of source blocks is
//      sourceblockcount = 0;
//      break;
    }

    ++sf;
    ++filenumber;
  }

  // Did we determine the total number of source blocks
  if (sourceblockcount > 0)
  {
    // Yes. 
    
    // Allocate all of the Source and Target DataBlocks (which will be used
    // to read and write data to disk).

    sourceblocks.resize(sourceblockcount);
    targetblocks.resize(sourceblockcount);

    // Which DataBlocks will be allocated first
    vector<DataBlock>::iterator sourceblock = sourceblocks.begin();
    vector<DataBlock>::iterator targetblock = targetblocks.begin();

    u64 totalsize = 0;
    u32 blocknumber = 0;

    filenumber = 0;
    sf = sourcefiles.begin();

    while (filenumber < mainpacket->RecoverableFileCount() && sf != sourcefiles.end())
    {
      Par2RepairerSourceFile *sourcefile = *sf;

      if (sourcefile)
      {
        totalsize += sourcefile->GetDescriptionPacket()->FileSize();
        u32 blockcount = sourcefile->BlockCount();

        // Allocate the source and target DataBlocks to the sourcefile
        sourcefile->SetBlocks(blocknumber, blockcount, sourceblock, targetblock, blocksize);

        blocknumber++;

        sourceblock += blockcount;
        targetblock += blockcount;
      }

      ++sf;
      ++filenumber;
    }

    blocksallocated = true;

    if (noiselevel > CommandLine::nlQuiet)
    {
      dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
      cout << "There are a total of "
           << sourceblockcount
           << " data blocks."
           << endl;

      cout << "The total size of the data files is "
           << totalsize
           << " bytes."
           << endl;
      dispatch_semaphore_signal(coutSema);
    }
  }

  return true;
}

// Create a verification hash table for all files for which we have not
// found a complete version of the file and for which we have
// a verification packet
bool Par2Repairer::PrepareVerificationHashTable(void)
{
  // Choose a size for the hash table
  verificationhashtable.SetLimit(sourceblockcount);

  // Will any files be block verifiable
  blockverifiable = false;

  // For each source file
  vector<Par2RepairerSourceFile*>::iterator sf = sourcefiles.begin();
  while (sf != sourcefiles.end())
  {
    // Get the source file
    Par2RepairerSourceFile *sourcefile = *sf;

    if (sourcefile)
    {
      // Do we have a verification packet
      if (0 != sourcefile->GetVerificationPacket())
      {
        // Yes. Load the verification entries into the hash table
        verificationhashtable.Load(sourcefile, blocksize);

        blockverifiable = true;
      }
      else
      {
        // No. We can only check the whole file
        unverifiablesourcefiles.push_back(sourcefile);
      }
    }

    ++sf;
  }

  return true;
}

// Compute the table for the sliding CRC computation
bool Par2Repairer::ComputeWindowTable(void)
{
  if (blockverifiable)
  {
    GenerateWindowTable(blocksize, windowtable);
    windowmask = ComputeWindowMask(blocksize);
  }

  return true;
}

static bool SortSourceFilesByFileName(Par2RepairerSourceFile *low,
                                      Par2RepairerSourceFile *high)
{
  return low->TargetFileName() < high->TargetFileName();
}

// Attempt to verify all of the source files
bool Par2Repairer::VerifySourceFiles(void)
{
  bool finalresult = true;

#ifdef DEBUG
  cerr << "trace: Start Par2Repairer::VerifySourceFiles" << endl;
#endif

  // Created a sorted list of the source files and verify them in that
  // order rather than the order they are in the main packet.
  vector<Par2RepairerSourceFile*> sortedfiles;

  u32 filenumber = 0;
  vector<Par2RepairerSourceFile*>::iterator sf = sourcefiles.begin();
  while (sf != sourcefiles.end())
  {
    // Do we have a source file
    Par2RepairerSourceFile *sourcefile = *sf;
    if (sourcefile)
    {
      sortedfiles.push_back(sourcefile);
    }
    else
    {
      // Was this one of the recoverable files
      if (filenumber < mainpacket->RecoverableFileCount())
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cerr << "No details available for recoverable file number " << filenumber+1 << "." << endl
             << "Recovery will not be possible." << endl;
        dispatch_semaphore_signal(coutSema);

        // Set error but let verification of other files continue
        finalresult = false;
      }
      else
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cerr << "No details available for non-recoverable file number " 
             << filenumber - mainpacket->RecoverableFileCount() + 1 << endl;
        dispatch_semaphore_signal(coutSema);
      }
    }

    ++sf;
  }

  sort(sortedfiles.begin(), sortedfiles.end(), SortSourceFilesByFileName);

  // Now basically, we want to run Verify1SourceFile for each file in sortedfiles, more or less in
  // the same order (that is why we sorted in the first place). However, multiple files can be
  // processed simultaneously; they are independent on each other. Grand Central makes this easy.
  // I found out that using a dispatch_apply causes GCD to do the job on as many threads as there 
  // are cores, and it uses the calling thread as one of the worker threads. Clever!
  
  // For use in the block, make a simple array of pointers. We must avoid using the stack-based 
  // C++ object (sortedfiles) in the block; it simply won't work.
  Par2RepairerSourceFile **lSourceFileArray = (Par2RepairerSourceFile **)malloc(sortedfiles.size() * 
                                                                                sizeof (Par2RepairerSourceFile *));
  for (unsigned int i = 0; i < sortedfiles.size(); i++)
  {
    lSourceFileArray[i] = sortedfiles[i];
  }
  
  // Note: cannot use finalresult as __block; that messes up the returned  value from this function!
  __block bool lAllFilesResult = true;    // Optimistic default
  dispatch_apply(sortedfiles.size(), dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
                 ^(size_t aIndex){
                   if (!this->Verify1SourceFile(lSourceFileArray[aIndex]))
                   {
                     dispatch_semaphore_wait(genericSema, DISPATCH_TIME_FOREVER);
                     lAllFilesResult = false;
                     dispatch_semaphore_signal(genericSema);
                   }
                 });
  free(lSourceFileArray);
	
  finalresult = finalresult && lAllFilesResult;
  return finalresult;
}

bool Par2Repairer::Verify1SourceFile (Par2RepairerSourceFile* aSourcefile)
{
  // This one runs simultaneously, in multiple threads, one per file.
	bool rv = true;
  
  // Having our own pool is a must; we use Cocoa and run on different threads
  void *lPool = OSXStuff::SetupAutoreleasePool();

  // What filename does the file use
  string filename = aSourcefile->TargetFileName();
  
  // Check to see if we have already used this file
  if (diskFileMap.Find(filename) != 0)
  {
    // The file has already been used!
    
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cerr << "Source file \"" << DiskFile::FS2UTF8(filename) << "\" is a duplicate." << endl;
    dispatch_semaphore_signal(coutSema);
    OSXStuff::ReleaseAutoreleasePool(lPool);
    return false;
  }
  
  DiskFile *diskfile = new DiskFile;
  
  // Does the target file exist
  if (diskfile->Open(filename, true))  // true: expect to read all data of the file
  {
    // Yes. Record that fact.
    aSourcefile->SetTargetExists(true);
    
    // Remember that the DiskFile is the target file
    aSourcefile->SetTargetFile(diskfile);
    
    // Remember that we have processed this file
#ifdef DEBUG
    bool success = 
#endif
    diskFileMap.Insert(diskfile);
    assert(success);
    
    // Do the actual verification
    if (!VerifyDataFile(diskfile, aSourcefile))
      rv = false;
    
    // We have finished with the file for now
    diskfile->Close();
    
  } // End file exists
  else
  {
    // The file does not exist.
    delete diskfile;
    
    if (noiselevel > CommandLine::nlSilent)
    {
      string path;
      string name;
      DiskFile::SplitFilename(filename, path, name);
      
      dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
      cout << "Target: \"" << DiskFile::FS2UTF8(name) << "\" - missing." << endl;
      dispatch_semaphore_signal(coutSema);
    }
  } // End file does not exist
  OSXStuff::ReleaseAutoreleasePool(lPool);
  return rv;
}

// Scan any extra files specified on the command line
bool Par2Repairer::VerifyExtraFiles(const list<CommandLine::ExtraFile> &extrafiles)
{
  // Each file in extrafiles is scanned in a separate thread, managed by GCD. In practice this
  // means one running thread per core.

#ifdef DEBUG
  cerr << "trace: Start Par2Repairer::VerifyExtraFiles" << endl;
#endif
  
  // Make a simple array of pointers; the GCD block cannot deal with std::list.
  CommandLine::ExtraFile const **lExtraFileArray = (const CommandLine::ExtraFile **)malloc(extrafiles.size() * 
                                                                                           sizeof (CommandLine::ExtraFile *));
  int i = 0;
  for (ExtraFileIterator lIter = extrafiles.begin(); lIter != extrafiles.end(); lIter++)
  {
    lExtraFileArray[i++] = &(*lIter);
  }
  
  __block bool lMustContinue = true;  // Shared by all dispatched blocks
  dispatch_apply(extrafiles.size(), dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
                 ^(size_t aIndex){
                   dispatch_semaphore_wait(genericSema, DISPATCH_TIME_FOREVER); // Protect lMustContinue
                   if (lMustContinue)
                   {
                     dispatch_semaphore_signal(genericSema);
                     if (this->Verify1ExtraFile(lExtraFileArray[aIndex]) == 1)
                     {
                       // Means we can stop verifying the other extra files. The only snag is, we 
                       // cannot stop the dispatch_apply, so just set a flag to trivialize the remainder.
                       dispatch_semaphore_wait(genericSema, DISPATCH_TIME_FOREVER);
                       lMustContinue = false;
                       dispatch_semaphore_signal(genericSema);
                     }
                   }
                   else
                   {
                     dispatch_semaphore_signal(genericSema);
                   }
                 });
  
  free (lExtraFileArray);
  
  return true;
}

int Par2Repairer::Verify1ExtraFile(const CommandLine::ExtraFile *aExtraFile)
{
  // Returns 1 if the file exists, and we discovered enough data to shortcut the search for
  // extra data. Otherwise, returns 0.
  int rv = 0;
  
  void *lPool = OSXStuff::SetupAutoreleasePool();  // Block uses Cocoa

  string filename = aExtraFile->FileName();

  // If the filename does not include ".par2" we are interested in it.
  if (string::npos == filename.find(".par2") &&
      string::npos == filename.find(".PAR2"))
  {
    filename = DiskFile::GetCanonicalPathname(filename);
    
    // Has this file already been dealt with
    if (diskFileMap.Find(filename) == 0)
    {
      DiskFile *diskfile = new DiskFile;
      
      // Does the file exist
      if (!diskfile->Open(filename, true))  // true: expect to read all data of the file
      {
        delete diskfile;
        OSXStuff::ReleaseAutoreleasePool(lPool);
        return 0;
      }
      
      // Remember that we have processed this file
#ifdef DEBUG
      bool success = 
#endif
      diskFileMap.Insert(diskfile);
      assert(success);
      
      // Do the actual verification
      VerifyDataFile(diskfile, 0);
      // Ignore errors
      
      // We have finished with the file for now
      diskfile->Close();
      
      dispatch_semaphore_wait(genericSema, DISPATCH_TIME_FOREVER);
      // Find out how much data we have found
      UpdateVerificationResults();
      
      /* If we have a complete set of files now, shortcut the loop. Just looking
       at completefilecount + renamedfilecount ensures we continue scanning files until
       we are sure a real repair is necessary. The alternative, calling CheckVerificationResults,
       results in a start of the repair as soon as we have enough data, even if some more files
       might easily have been renamed. As scanning is MUCH quicker than repairing, use the
       former approach. */
      //if (CheckVerificationResults (1)) // Silent verification (alternative)
      if (completefilecount + renamedfilecount >= mainpacket->RecoverableFileCount())
      {
        rv = 1;
      }
      dispatch_semaphore_signal(genericSema);
    } // end if did not find file in map
  } // end if filename does not contain par2
  OSXStuff::ReleaseAutoreleasePool(lPool);
  return rv;
}

// Attempt to match the data in the DiskFile with the source file
bool Par2Repairer::VerifyDataFile(DiskFile *diskfile, Par2RepairerSourceFile *sourcefile)
{
  MatchType matchtype; // What type of match was made
  MD5Hash hashfull;    // The MD5 Hash of the whole file
  MD5Hash hash16k;     // The MD5 Hash of the files 16k of the file

  // Are there any files that can be verified at the block level
  if (blockverifiable)
  {
    u32 count;

    // Scan the file at the block level.

    if (!ScanDataFile(diskfile,   // [in]      The file to scan
                      sourcefile, // [in/out]  Modified in the match is for another source file
                      matchtype,  // [out]
                      hashfull,   // [out]
                      hash16k,    // [out]
                      count))     // [out]
    {
#ifdef DEBUG
      cerr << "trace: ScanDataFile returned false in Par2Repairer::VerifyDataFile" << endl;
#endif
      return false;
    }

    switch (matchtype)
    {
    case eNoMatch:
      // No data was found at all.

      // Continue to next test.
      break;
    case ePartialMatch:
      {
        // We found some data.

        // Return them.
        return true;
      }
      break;
    case eFullMatch:
      {
        // We found a perfect match.

        sourcefile->SetCompleteFile(diskfile);

        // Return the match
        return true;
      }
      break;
    }
  }

  // We did not find a match for any blocks of data within the file, but if 
  // there are any files for which we did not have a verification packet
  // we can try a simple match of the hash for the whole file.

  // Are there any files that cannot be verified at the block level
  if (unverifiablesourcefiles.size() > 0)
  {
    // Would we have already computed the file hashes
    if (!blockverifiable)
    {
      u64 filesize = diskfile->FileSize();

      size_t buffersize = 1024*1024*10;
      if (buffersize > min(blocksize, filesize))
        buffersize = (size_t)min(blocksize, filesize);

      char *buffer = new char[buffersize];

      u64 offset = 0;

      MD5Context context;

      while (offset < filesize)
      {
        size_t want = (size_t)min((u64)buffersize, filesize-offset);

        if (!diskfile->Read(offset, buffer, want))
        {
          delete [] buffer;
#ifdef DEBUG
          cerr << "trace: diskfile->Read returned false in Par2Repairer::VerifyDataFile" << endl;
#endif          
          return false;
        }

        // Will the newly read data reach the 16k boundary
        if (offset < 16384 && offset + want >= 16384)
        {
          context.Update(buffer, (size_t)(16384-offset));

          // Compute the 16k hash
          MD5Context temp = context;
          temp.Final(hash16k);

          // Is there more data
          if (offset + want > 16384)
          {
            context.Update(&buffer[16384-offset], (size_t)(offset+want)-16384);
          }
        }
        else
        {
          context.Update(buffer, want);
        }

        offset += want;
      }

      // Compute the file hash
      MD5Hash hashfull;
      context.Final(hashfull);

      // If we did not have 16k of data, then the 16k hash
      // is the same as the full hash
      if (filesize < 16384)
      {
        hash16k = hashfull;
      }
    }

    list<Par2RepairerSourceFile*>::iterator sf = unverifiablesourcefiles.begin();

    // Compare the hash values of each source file for a match
    while (sf != unverifiablesourcefiles.end())
    {
      sourcefile = *sf;

      // Does the file match
      if (sourcefile->GetCompleteFile() == 0 &&
          diskfile->FileSize() == sourcefile->GetDescriptionPacket()->FileSize() &&
          hash16k == sourcefile->GetDescriptionPacket()->Hash16k() &&
          hashfull == sourcefile->GetDescriptionPacket()->HashFull())
      {
        if (noiselevel > CommandLine::nlSilent)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << DiskFile::FS2UTF8(diskfile->FileName()) << " is a perfect match for "
               << DiskFile::FS2UTF8(sourcefile->GetDescriptionPacket()->FileName()) << endl;
          dispatch_semaphore_signal(coutSema);
        }

        // Record that we have a perfect match for this source file
        sourcefile->SetCompleteFile(diskfile);

        if (blocksallocated)
        {
          // Allocate all of the DataBlocks for the source file to the DiskFile

          u64 offset = 0;
          u64 filesize = sourcefile->GetDescriptionPacket()->FileSize();

          vector<DataBlock>::iterator sb = sourcefile->SourceBlocks();

          while (offset < filesize)
          {
            DataBlock &datablock = *sb;

            datablock.SetLocation(diskfile, offset);
            datablock.SetLength(min(blocksize, filesize-offset));

            offset += blocksize;
            ++sb;
          }
        }

        // Return the match
        return true;
      }

      ++sf;
    }
  }

  return true;
}

// Perform a sliding window scan of the DiskFile looking for blocks of data that 
// might belong to any of the source files (for which a verification packet was
// available). If a block of data might be from more than one source file, prefer
// the one specified by the "sourcefile" parameter. If the first data block
// found is for a different source file then "sourcefile" is changed accordingly.
bool Par2Repairer::ScanDataFile(DiskFile                *diskfile,    // [in]
                                Par2RepairerSourceFile* &sourcefile,  // [in/out]
                                MatchType               &matchtype,   // [out]
                                MD5Hash                 &hashfull,    // [out]
                                MD5Hash                 &hash16k,     // [out]
                                u32                     &count)       // [out]
{
  // Remember which file we wanted to match
  Par2RepairerSourceFile *originalsourcefile = sourcefile;

  matchtype = eNoMatch;

  string path;
  string name;
  DiskFile::SplitFilename(diskfile->FileName(), path, name);

  // Is the file empty
  if (diskfile->FileSize() == 0)
  {
    // If the file is empty, then just return
    if (noiselevel > CommandLine::nlSilent)
    {
      if (originalsourcefile != 0)
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cout << "Target: \"" << DiskFile::FS2UTF8(name) << "\" - empty." << endl;
        dispatch_semaphore_signal(coutSema);
      }
      else
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cout << "File: \"" << DiskFile::FS2UTF8(name) << "\" - empty." << endl;
        dispatch_semaphore_signal(coutSema);
      }
    }
    return true;
  }

  // How many blocks have already been found
  u32 duplicatecount = 0;

  // Have we found data blocks in this file that belong to more than one target file
  bool multipletargets = false;

  // MacPAR deLuxe might have set an environment var with files that can be skipped
  if (diskfile->FileConsideredOK ())
  {
    matchtype = eFullMatch;
    count = sourcefile->GetVerificationPacket()->BlockCount();
    // diskfile->FileSize() != sourcefile->GetDescriptionPacket()->FileSize();
    hashfull = sourcefile->GetDescriptionPacket()->HashFull();
    hash16k = sourcefile->GetDescriptionPacket()->Hash16k();
    // We must set the datablocks in sourcefile to point to the supposedly matching file.
    // This copied from Par2Repairer::VerifyDataFile.
    u64 offset = 0;
    u64 filesize = sourcefile->GetDescriptionPacket()->FileSize();
    vector<DataBlock>::iterator sb = sourcefile->SourceBlocks();
    
    while (offset < filesize)
    {
      DataBlock &datablock = *sb;
      
      datablock.SetLocation(diskfile, offset);
      datablock.SetLength(min(blocksize, filesize-offset));
      
      offset += blocksize;
      ++sb;
    }
  }
  else
  {
#ifndef MPDL
    string shortname;
    if (name.size() > 56)
    {
      shortname = name.substr(0, 28) + "..." + name.substr(name.size()-28);
    }
    else
    {
      shortname = name;
    }
#endif
    // Create the checksummer for the file and start reading from it
    FileCheckSummer filechecksummer(diskfile, blocksize, windowtable, windowmask);
    if (!filechecksummer.Start())
    {
#ifdef DEBUG
      cerr << "trace: filechecksummer.Start returned false in Par2Repairer::ScanDataFile" << endl;
#endif          
      return false;
    }

    // Assume we will make a perfect match for the file
    matchtype = eFullMatch;

    // How many matches have we had
    count = 0;

    // Which block do we expect to find first
    const VerificationHashEntry *nextentry = 0;

#ifndef MPDL
    u64 progress = 0;
#endif

    // Whilst we have not reached the end of the file
    while (filechecksummer.Offset() < diskfile->FileSize())
    {
      // Define MPDL to suppress all percentages. This speeds up things considerably.
#ifndef MPDL
      if (noiselevel > CommandLine::nlQuiet)
      {
        // Update a progress indicator
        u32 oldfraction = (u32)(1000 * progress / diskfile->FileSize());
        u32 newfraction = (u32)(1000 * (progress = filechecksummer.Offset()) / diskfile->FileSize());
        if (oldfraction != newfraction)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "Scanning: \"" << DiskFile::FS2UTF8(shortname) << "\": " << newfraction/10 << '.'
               << newfraction%10 << "%\r" << flush;
          dispatch_semaphore_signal(coutSema);
        }
      }
#endif

      // If we fail to find a match, it might be because it was a duplicate of a block
      // that we have already found.
      bool duplicate;

      // Look for a match
      const VerificationHashEntry *currententry = verificationhashtable.FindMatch(nextentry, sourcefile, filechecksummer, duplicate);

      // Did we find a match
      if (currententry != 0)
      {
        // Is this the first match
        if (count == 0)
        {
          // Which source file was it
          sourcefile = currententry->SourceFile();

          // If the first match found was not actually the first block
          // for the source file, or it was not at the start of the
          // data file: then this is a partial match.
          if (!currententry->FirstBlock() || filechecksummer.Offset() != 0)
          {
            matchtype = ePartialMatch;
          }
        }
        else
        {
          // If the match found is not the one which was expected
          // then this is a partial match

          if (currententry != nextentry)
          {
            matchtype = ePartialMatch;
          }

          // Is the match from a different source file
          if (sourcefile != currententry->SourceFile())
          {
            multipletargets = true;
          }
        }

        if (blocksallocated)
        {
          // Record the match
          currententry->SetBlock(diskfile, filechecksummer.Offset());
        }

        // Update the number of matches found
        count++;

        // What entry do we expect next
        nextentry = currententry->Next();

        // Advance to the next block
        if (!filechecksummer.Jump(currententry->GetDataBlock()->GetLength()))
        {
#ifdef DEBUG
          cerr << "trace: filechecksummer.Jump(1) returned false in Par2Repairer::ScanDataFile" << endl;
#endif          
          return false;
        }
      }
      else
      {
        // This cannot be a perfect match
        matchtype = ePartialMatch;

        // Was this a duplicate match
        if (duplicate)
        {
          duplicatecount++;

          // What entry would we expect next
          nextentry = 0;

          // Advance one whole block
          if (!filechecksummer.Jump(blocksize))
          {
#ifdef DEBUG
            cerr << "trace: filechecksummer.Jump(2) returned false in Par2Repairer::ScanDataFile" << endl;
#endif          
            return false;
          }
        }
        else
        {
          // What entry do we expect next
          nextentry = 0;

          // Advance 1 byte
          if (!filechecksummer.Step())
          {
#ifdef DEBUG
            cerr << "trace: filechecksummer.Step returned false in Par2Repairer::ScanDataFile" << endl;
#endif          
            return false;
          }
        }
      }
    }

    // Get the Full and 16k hash values of the file
    filechecksummer.GetFileHashes(hashfull, hash16k);
  } // end if file not considered OK on basis of name alone
  // Did we make any matches at all
  if (count > 0)
  {
    // If this still might be a perfect match, check the
    // hashes, file size, and number of blocks to confirm.
    if (matchtype            != eFullMatch || 
        count                != sourcefile->GetVerificationPacket()->BlockCount() ||
        diskfile->FileSize() != sourcefile->GetDescriptionPacket()->FileSize() ||
        hashfull             != sourcefile->GetDescriptionPacket()->HashFull() ||
        hash16k              != sourcefile->GetDescriptionPacket()->Hash16k())
    {
      matchtype = ePartialMatch;

      if (noiselevel > CommandLine::nlSilent)
      {
        // Did we find data from multiple target files
        if (multipletargets)
        {
          // Were we scanning the target file or an extra file
          if (originalsourcefile != 0)
          {
            dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
            cout << "Target: \"" 
                 << DiskFile::FS2UTF8(name)
                 << "\" - damaged, found " 
                 << count 
                 << " data blocks from several target files." 
                 << endl;
            dispatch_semaphore_signal(coutSema);
          }
          else
          {
            dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
            cout << "File: \"" 
                 << DiskFile::FS2UTF8(name)
                 << "\" - found " 
                 << count 
                 << " data blocks from several target files." 
                 << endl;
            dispatch_semaphore_signal(coutSema);
          }
        }
        else
        {
          // Did we find data blocks that belong to the target file
          if (originalsourcefile == sourcefile)
          {
            dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
            cout << "Target: \"" 
                 << DiskFile::FS2UTF8(name)
                 << "\" - damaged. Found " 
                 << count 
                 << " of " 
                 << sourcefile->GetVerificationPacket()->BlockCount() 
                 << " data blocks." 
                 << endl;
            dispatch_semaphore_signal(coutSema);
          }
          // Were we scanning the target file or an extra file
          else if (originalsourcefile != 0)
          {
            string targetname;
            DiskFile::SplitFilename(sourcefile->TargetFileName(), path, targetname);

            dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
            cout << "Target: \"" 
                 << DiskFile::FS2UTF8(name)
                 << "\" - damaged. Found " 
                 << count 
                 << " of " 
                 << sourcefile->GetVerificationPacket()->BlockCount() 
                 << " data blocks from \"" 
                 << DiskFile::FS2UTF8(targetname)
                 << "\"."
                 << endl;
            dispatch_semaphore_signal(coutSema);
          }
          else
          {
            string targetname;
            DiskFile::SplitFilename(sourcefile->TargetFileName(), path, targetname);

            dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
            cout << "File: \"" 
                 << DiskFile::FS2UTF8(name)
                 << "\" - found " 
                 << count 
                 << " of " 
                 << sourcefile->GetVerificationPacket()->BlockCount() 
                 << " data blocks from \"" 
                 << DiskFile::FS2UTF8(targetname)
                 << "\"."
                 << endl;
            dispatch_semaphore_signal(coutSema);
          }
        }
      }
    }
    else
    {
      if (noiselevel > CommandLine::nlSilent)
      {
        // Did we match the target file
        if (originalsourcefile == sourcefile)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "Target: \"" << DiskFile::FS2UTF8(name) << "\" - found." << endl;
          dispatch_semaphore_signal(coutSema);
        }
        // Were we scanning the target file or an extra file
        else if (originalsourcefile != 0)
        {
          string targetname;
          DiskFile::SplitFilename(sourcefile->TargetFileName(), path, targetname);

          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "Target: \"" 
               << DiskFile::FS2UTF8(name)
               << "\" - is a match for \"" 
               << DiskFile::FS2UTF8(targetname)
               << "\"." 
               << endl;
          dispatch_semaphore_signal(coutSema);
        }
        else
        {
          string targetname;
          DiskFile::SplitFilename(sourcefile->TargetFileName(), path, targetname);

          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "File: \"" 
               << DiskFile::FS2UTF8(name)
               << "\" - is a match for \"" 
               << DiskFile::FS2UTF8(targetname)
               << "\"." 
               << endl;
          dispatch_semaphore_signal(coutSema);
        }
      }
    }
  }
  else
  {
    matchtype = eNoMatch;

    if (noiselevel > CommandLine::nlSilent)
    {
      // We found not data, but did the file actually contain blocks we
      // had already found in other files.
      if (duplicatecount > 0)
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cout << "File: \""
             << DiskFile::FS2UTF8(name)
             << "\" - found " 
             << duplicatecount
             << " duplicate data blocks."
             << endl;
        dispatch_semaphore_signal(coutSema);
      }
      else
      {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        cout << "File: \"" 
             << DiskFile::FS2UTF8(name)
             << "\" - no data found." 
             << endl;
        dispatch_semaphore_signal(coutSema);
      }
    }
  }

  return true;
}

// Find out how much data we have found
void Par2Repairer::UpdateVerificationResults(void)
{
  this->availableblockcount = 0;
  this->missingblockcount = 0;

  this->completefilecount = 0;
  this->renamedfilecount = 0;
  this->damagedfilecount = 0;
  this->missingfilecount = 0;

  u32 lFilenumber = 0;
  vector<Par2RepairerSourceFile*>::iterator lSf = sourcefiles.begin();

  // Check the recoverable files
  while (lSf != sourcefiles.end() && lFilenumber < mainpacket->TotalFileCount())
  {
    Par2RepairerSourceFile *lSourcefile = *lSf;

    if (lSourcefile)
    {
      // Was a perfect match for the file found
      if (lSourcefile->GetCompleteFile() != 0)
      {
        // Is it the target file or a different one
        if (lSourcefile->GetCompleteFile() == lSourcefile->GetTargetFile())
        {
          this->completefilecount++;
        }
        else
        {
          this->renamedfilecount++;
        }

        this->availableblockcount += lSourcefile->BlockCount();
      }
      else
      {
        // Count the number of blocks that have been found
        vector<DataBlock>::iterator lSb = lSourcefile->SourceBlocks();
        for (u32 lBlocknumber=0; lBlocknumber<lSourcefile->BlockCount(); ++lBlocknumber, ++lSb)
        {
          DataBlock &lDatablock = *lSb;
          
          if (lDatablock.IsSet())
            this->availableblockcount++;
        }

        // Does the target file exist
        if (lSourcefile->GetTargetExists())
        {
          this->damagedfilecount++;
        }
        else
        {
          this->missingfilecount++;
        }
      }
    }
    else
    {
      this->missingfilecount++;
    }

    ++lFilenumber;
    ++lSf;
  }

  this->missingblockcount = sourceblockcount - this->availableblockcount;
}

// Check the verification results and report the results 
bool Par2Repairer::CheckVerificationResults(int aSilent /* = 0*/)
{
  // Is repair needed
  if (completefilecount < mainpacket->RecoverableFileCount() ||
      renamedfilecount > 0 ||
      damagedfilecount > 0 ||
      missingfilecount > 0)
  {
    if (!aSilent)
    {
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
        if (noiselevel > CommandLine::nlSilent)
          cout << "Repair is required." << endl;
        if (noiselevel > CommandLine::nlQuiet)
        {
          if (renamedfilecount > 0) cout << renamedfilecount << " file(s) have the wrong name." << endl;
          if (missingfilecount > 0) cout << missingfilecount << " file(s) are missing." << endl;
          if (damagedfilecount > 0) cout << damagedfilecount << " file(s) exist but are damaged." << endl;
          if (completefilecount > 0) cout << completefilecount << " file(s) are ok." << endl;

          cout << "You have " << availableblockcount 
               << " out of " << sourceblockcount 
               << " data blocks available." << endl;
          if (recoverypacketmap.size() > 0)
            cout << "You have " << (u32)recoverypacketmap.size() 
                 << " recovery blocks available." << endl;
        }
        dispatch_semaphore_signal(coutSema);
    }

    // Is repair possible
    if (recoverypacketmap.size() >= missingblockcount)
    {
      if (!aSilent)
      {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          if (noiselevel > CommandLine::nlSilent)
            cout << "Repair is possible." << endl;

          if (noiselevel > CommandLine::nlQuiet)
          {
            if (recoverypacketmap.size() > missingblockcount)
              cout << "You have an excess of " 
                   << (u32)recoverypacketmap.size() - missingblockcount
                   << " recovery blocks." << endl;

            if (missingblockcount > 0)
              cout << missingblockcount
                   << " recovery blocks will be used to repair." << endl;
            else if (recoverypacketmap.size())
              cout << "None of the recovery blocks will be used for the repair." << endl;
          }
          dispatch_semaphore_signal(coutSema);
      }
      return true;
    }
    else
    {
      if (!aSilent)
      {
          if (noiselevel > CommandLine::nlSilent)
          {
            dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
            cout << "Repair is not possible." << endl;
            cout << "You need " << missingblockcount - recoverypacketmap.size()
                 << " more recovery blocks to be able to repair." << endl;
            dispatch_semaphore_signal(coutSema);
          }
      }
      return false;
    }
  }
  else
  {
    if (!aSilent)
    {
        if (noiselevel > CommandLine::nlSilent)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "All files are correct, repair is not required." << endl;
          dispatch_semaphore_signal(coutSema);
        }
    }
    return true;
  }

  return true;
}

// Rename any damaged or missnamed target files.
bool Par2Repairer::RenameTargetFiles(void)
{
  u32 filenumber = 0;
  vector<Par2RepairerSourceFile*>::iterator sf = sourcefiles.begin();

  // Rename any damaged target files
  while (sf != sourcefiles.end() && filenumber < mainpacket->TotalFileCount())
  {
    Par2RepairerSourceFile *sourcefile = *sf;

    // If the target file exists but is not a complete version of the file
    if (sourcefile->GetTargetExists() && 
        sourcefile->GetTargetFile() != sourcefile->GetCompleteFile())
    {
      DiskFile *targetfile = sourcefile->GetTargetFile();

      // Rename it
      diskFileMap.Remove(targetfile);

      if (!targetfile->Rename())
        return false;

#ifdef DEBUG
      bool success = 
#endif
      diskFileMap.Insert(targetfile);
      assert(success);

      // We no longer have a target file
      sourcefile->SetTargetExists(false);
      sourcefile->SetTargetFile(0);
    }

    ++sf;
    ++filenumber;
  }

  filenumber = 0;
  sf = sourcefiles.begin();

  // Rename any missnamed but complete versions of the files
  while (sf != sourcefiles.end() && filenumber < mainpacket->TotalFileCount())
  {
    Par2RepairerSourceFile *sourcefile = *sf;

    // If there is no targetfile and there is a complete version
    if (sourcefile->GetTargetFile() == 0 &&
        sourcefile->GetCompleteFile() != 0)
    {
      DiskFile *targetfile = sourcefile->GetCompleteFile();

      // Rename it
      diskFileMap.Remove(targetfile);

      if (!targetfile->Rename(sourcefile->TargetFileName()))
        return false;

#ifdef DEBUG
      bool success = 
#endif
      diskFileMap.Insert(targetfile);
      assert(success);

      // This file is now the target file
      sourcefile->SetTargetExists(true);
      sourcefile->SetTargetFile(targetfile);

      // We have one more complete file
      completefilecount++;
    }

    ++sf;
    ++filenumber;
  }

  return true;
}

// Work out which files are being repaired, create them, and allocate
// target DataBlocks to them, and remember them for later verification.
bool Par2Repairer::CreateTargetFiles(void)
{
  u32 filenumber = 0;
  vector<Par2RepairerSourceFile*>::iterator sf = sourcefiles.begin();

  // Create any missing target files
  while (sf != sourcefiles.end() && filenumber < mainpacket->TotalFileCount())
  {
    Par2RepairerSourceFile *sourcefile = *sf;

    // If the file does not exist
    if (!sourcefile->GetTargetExists())
    {
      DiskFile *targetfile = new DiskFile;
      string filename = sourcefile->TargetFileName();
      u64 filesize = sourcefile->GetDescriptionPacket()->FileSize();

      // Create the target file
      if (!targetfile->Create(filename, filesize))
      {
        delete targetfile;
        return false;
      }

      // This file is now the target file
      sourcefile->SetTargetExists(true);
      sourcefile->SetTargetFile(targetfile);

      // Remember this file
#ifdef DEBUG
      bool success = 
#endif
      diskFileMap.Insert(targetfile);
      assert(success);

      u64 offset = 0;
      vector<DataBlock>::iterator tb = sourcefile->TargetBlocks();

      // Allocate all of the target data blocks
      while (offset < filesize)
      {
        DataBlock &datablock = *tb;

        datablock.SetLocation(targetfile, offset);
        datablock.SetLength(min(blocksize, filesize-offset));

        offset += blocksize;
        ++tb;
      }

      // Add the file to the list of those that will need to be verified
      // once the repair has completed.
      verifylist.push_back(sourcefile);
    }

    ++sf;
    ++filenumber;
  }

  return true;
}

// Work out which data blocks are available, which need to be copied
// directly to the output, and which need to be recreated, and compute
// the appropriate Reed Solomon matrix.
bool Par2Repairer::ComputeRSmatrix(void)
{
  inputblocks.resize(sourceblockcount);   // The DataBlocks that will read from disk
  copyblocks.resize(availableblockcount); // Those DataBlocks which need to be copied
  outputblocks.resize(missingblockcount); // Those DataBlocks that will re recalculated

  vector<DataBlock*>::iterator inputblock  = inputblocks.begin();
  vector<DataBlock*>::iterator copyblock   = copyblocks.begin();
  vector<DataBlock*>::iterator outputblock = outputblocks.begin();

  // Build an array listing which source data blocks are present and which are missing
  vector<bool> present;
  present.resize(sourceblockcount);

  vector<DataBlock>::iterator sourceblock  = sourceblocks.begin();
  vector<DataBlock>::iterator targetblock  = targetblocks.begin();
  vector<bool>::iterator              pres = present.begin();

  // Iterate through all source blocks for all files
  while (sourceblock != sourceblocks.end())
  {
    // Was this block found
    if (sourceblock->IsSet())
    {
//      // Open the file the block was found in.
//      if (!sourceblock->Open())
//        return false;

      // Record that the block was found
      *pres = true;

      // Add the block to the list of those which will be read 
      // as input (and which might also need to be copied).
      *inputblock = &*sourceblock;
      *copyblock = &*targetblock;

      ++inputblock;
      ++copyblock;
    }
    else
    {
      // Record that the block was missing
      *pres = false;

      // Add the block to the list of those to be written
      *outputblock = &*targetblock;
      ++outputblock;
    }

    ++sourceblock;
    ++targetblock;
    ++pres;
  }

  // Set the number of source blocks and which of them are present
  if (!rs.SetInput(present))
    return false;

  // Start iterating through the available recovery packets
  map<u32,RecoveryPacket*>::iterator rp = recoverypacketmap.begin();

  // Continue to fill the remaining list of data blocks to be read
  while (inputblock != inputblocks.end())
  {
    // Get the next available recovery packet
    u32 exponent = rp->first;
    RecoveryPacket* recoverypacket = rp->second;

    // Get the DataBlock from the recovery packet
    DataBlock *recoveryblock = recoverypacket->GetDataBlock();

//    // Make sure the file is open
//    if (!recoveryblock->Open())
//      return false;

    // Add the recovery block to the list of blocks that will be read
    *inputblock = recoveryblock;

    // Record that the corresponding exponent value is the next one
    // to use in the RS matrix
    if (!rs.SetOutput(true, (u16)exponent))
      return false;

    ++inputblock;
    ++rp;
  }

  // If we need to, compute and solve the RS matrix
  if (missingblockcount == 0)
    return true;
  
  bool success = rs.Compute(noiselevel);

  return success;  
}

// Allocate memory buffers for reading and writing data to disk.
bool Par2Repairer::AllocateBuffers(size_t memorylimit)
{
  // Would single pass processing use too much memory
  if (blocksize * missingblockcount > memorylimit)
  {
    // Pick a size that is small enough
    chunksize = ~3 & (memorylimit / missingblockcount);
  }
  else
  {
    chunksize = (size_t)blocksize;
  }

  // Allocate the two buffers
  inputbuffer = new u8[(size_t)chunksize];
  outputbuffer = new u8[(size_t)chunksize * missingblockcount];

  if (inputbuffer == NULL || outputbuffer == NULL)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cerr << "Could not allocate buffer memory." << endl;
    dispatch_semaphore_signal(coutSema);
    return false;
  }
  
  return true;
}

// Read source data, process it through the RS matrix and write it to disk.
bool Par2Repairer::ProcessData(u64 blockoffset, size_t blocklength)
{
  u64 totalwritten = 0;

  // Clear the output buffer
  memset(outputbuffer, 0, (size_t)chunksize * missingblockcount);

  vector<DataBlock*>::iterator inputblock = inputblocks.begin();
  vector<DataBlock*>::iterator copyblock  = copyblocks.begin();
  u32                          inputindex = 0;

  DiskFile *lastopenfile = NULL;

  // Are there any blocks which need to be reconstructed
  if (missingblockcount > 0)
  {
    // For each input block
    while (inputblock != inputblocks.end())       
    {
      void *lPool = OSXStuff::SetupAutoreleasePool();   // Because we use Cocoa along the way
      // Are we reading from a new file?
      if (lastopenfile != (*inputblock)->GetDiskFile())
      {
        // Close the last file
        if (lastopenfile != NULL)
        {
          lastopenfile->Close();
        }

        // Open the new file
        lastopenfile = (*inputblock)->GetDiskFile();
        if (!lastopenfile->Open(false))  // false: don't expect to read all data of the file
        {
          OSXStuff::ReleaseAutoreleasePool(lPool);
          return false;
        }
      }

      // Read data from the current input block
      if (!(*inputblock)->ReadData(blockoffset, blocklength, inputbuffer))
      {
        OSXStuff::ReleaseAutoreleasePool(lPool);
        return false;
      }

      // Have we reached the last source data block
      if (copyblock != copyblocks.end())
      {
        // Does this block need to be copied to the target file
        if ((*copyblock)->IsSet())
        {
          size_t wrote;

          // Write the block back to disk in the new target file
          if (!(*copyblock)->WriteData(blockoffset, blocklength, inputbuffer, wrote))
          {
            OSXStuff::ReleaseAutoreleasePool(lPool);
            return false;
          }

          totalwritten += wrote;
        }
        ++copyblock;
      }

			// Function to process things in multiple threads if appropariate
			this->RepairMissingBlocks (blocklength, inputindex);

      ++inputblock;
      ++inputindex;
      OSXStuff::ReleaseAutoreleasePool(lPool);
    }
  }
  else
  {
    // Reconstruction is not required, we are just copying blocks between files

    // For each block that might need to be copied
    while (copyblock != copyblocks.end())
    {
      void *lPool = OSXStuff::SetupAutoreleasePool();   // Because we use Cocoa along the way
      // Does this block need to be copied
      if ((*copyblock)->IsSet())
      {
        // Are we reading from a new file?
        if (lastopenfile != (*inputblock)->GetDiskFile())
        {
          // Close the last file
          if (lastopenfile != NULL)
          {
            lastopenfile->Close();
          }

          // Open the new file
          lastopenfile = (*inputblock)->GetDiskFile();
          if (!lastopenfile->Open(false))  // false: don't expect to read all data of the file
          {
            OSXStuff::ReleaseAutoreleasePool(lPool);
            return false;
          }
        }

        // Read data from the current input block
        if (!(*inputblock)->ReadData(blockoffset, blocklength, inputbuffer))
        {
          OSXStuff::ReleaseAutoreleasePool(lPool);
          return false;
        }

        size_t wrote;
        if (!(*copyblock)->WriteData(blockoffset, blocklength, inputbuffer, wrote))
        {
          OSXStuff::ReleaseAutoreleasePool(lPool);
          return false;
        }
        totalwritten += wrote;
      }

      if (noiselevel > CommandLine::nlQuiet)
      {
        // Update a progress indicator
#ifndef MPDL
        u32 oldfraction = (u32)(1000 * progress / totaldata);
#endif
        progress += blocklength;
        u32 newfraction = (u32)(1000 * progress / totaldata);

#ifdef MPDL
				// Only report "Repairing" when a certain amount of progress has been made
				// since last time, or when the progress is 100%
				if ((newfraction - previouslyReportedProgress >= 10) || (newfraction == 1000))
				{
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "Processing: " << newfraction/10 << '.' << newfraction%10 << "%\r" << flush;
          dispatch_semaphore_signal(coutSema);
					previouslyReportedProgress = newfraction;
				}
#else	
        if (oldfraction != newfraction)
        {
          dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
          cout << "Processing: " << newfraction/10 << '.' << newfraction%10 << "%\r" << flush;
          dispatch_semaphore_signal(coutSema);
        }
#endif
      }

      ++copyblock;
      ++inputblock;
      OSXStuff::ReleaseAutoreleasePool(lPool);
    }
  }

  // Close the last file
  if (lastopenfile != NULL)
  {
    lastopenfile->Close();
  }

  if (noiselevel > CommandLine::nlQuiet)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << "Writing recovered data\r";
    dispatch_semaphore_signal(coutSema);
  }

  // For each output block that has been recomputed
  vector<DataBlock*>::iterator outputblock = outputblocks.begin();
  for (u32 outputindex=0; outputindex<missingblockcount;outputindex++)
  {
    // Select the appropriate part of the output buffer
    char *outbuf = &((char*)outputbuffer)[chunksize * outputindex];

    // Write the data to the target file
    size_t wrote;
    if (!(*outputblock)->WriteData(blockoffset, blocklength, outbuf, wrote))
      return false;
    totalwritten += wrote;

    ++outputblock;
  }

  if (noiselevel > CommandLine::nlQuiet)
  {
    dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
    cout << "Wrote " << totalwritten << " bytes to disk" << endl;
    dispatch_semaphore_signal(coutSema);
  }

  return true;
}

//-----------------------------------------------------------------------------
void Par2Repairer::RepairMissingBlocks (size_t blocklength, u32 inputindex)
{
	// Used from within ProcessData.
	/*
	 * I re-designed this part to become multi-threaded, so it can benefit from a machine
	 * with multiple processors (or multiple cores). To that purpose, it uses Grand Central
   * Dispatch.
   * Each dispatched GCD code block deals with (a portion of) one par2 data block, and all these
   * GCD blocks are dispatched asynchronously and simultaneously.
   * The data to be processed is in instance variables.
   *
	 * Thread synchronization is trivial. All GCD blocks use the same, immutable input
	 * buffer, and they all write to separate parts of the output buffer. The only shared
	 * resource is the progression, which is reported by each thread individually. This is
	 * protected using a simple semaphore.
	 * This function (RepairMissingBlocks) exits when all dispatched GCD blocks have finished.
   */

	if (missingblockcount > 0)
  {
    const int cNumBlocksPerThread = 1;  // 1 seems to give best (fastest) results
    int lNumGCDDispatches = ((this->missingblockcount - 1) / cNumBlocksPerThread) + 1;
    // dispatch_apply sees to it that the blocks are posted simultaneously, and the global queue
    // executes them simultaneously if possible. dispatch_apply exists after all block have been executed.
    dispatch_apply(lNumGCDDispatches, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
                   ^(size_t lCurrent){
                     this->RepairMissingBlockRange (blocklength, inputindex, lCurrent * cNumBlocksPerThread, 
                                                    (lCurrent + 1) * cNumBlocksPerThread);
    });
  }
}

//-----------------------------------------------------------------------------
void Par2Repairer::RepairMissingBlockRange (size_t blocklength, u32 inputindex, u32 aStartBlockNo, u32 aEndBlockNo)
{
	// This function is called in multiple threads.
  // aEndBlock could be beyond the last element
  assert (aStartBlockNo < this->missingblockcount);
  if (aEndBlockNo > this->missingblockcount)
  {
    aEndBlockNo = this->missingblockcount;
  }
	for (u32 outputindex=aStartBlockNo; outputindex<aEndBlockNo; outputindex++)
	{
		// Select the appropriate part of the output buffer
		void *outbuf = &((u8*)outputbuffer)[chunksize * outputindex];
		
		// Process the data
		rs.Process(blocklength, inputindex, inputbuffer, outputindex, outbuf);
		
		if (noiselevel > CommandLine::nlQuiet)
		{
			// Update a progress indicator. This is thread-safe with a simple mutex
			dispatch_semaphore_wait(genericSema, DISPATCH_TIME_FOREVER);
			progress += blocklength;
			u32 newfraction = (u32)(1000 * progress / totaldata);
			
			// Only report "Repairing" when a certain amount of progress has been made
			// since last time, or when the progress is 100%
			if ((newfraction - previouslyReportedProgress >= 10) || (newfraction == 1000))
			{
        dispatch_semaphore_wait(coutSema, DISPATCH_TIME_FOREVER);
				cout << "Repairing: " << newfraction/10 << '.' << newfraction%10 << "%\r" << flush;
        dispatch_semaphore_signal(coutSema);
				previouslyReportedProgress = newfraction;
			}
			dispatch_semaphore_signal(genericSema);
		}
	}
}

// Verify that all of the reconstructed target files are now correct.
// Do this in multiple threads if appropriate (1 thread per processor).
bool Par2Repairer::VerifyTargetFiles(void)
{
  bool finalresult = true;

  // Verify the target files in alphabetical order
  sort(verifylist.begin(), verifylist.end(), SortSourceFilesByFileName);

  // Make an array of simple object pointers, to be used inside the GCD block
  Par2RepairerSourceFile **lSourceFileArray = (Par2RepairerSourceFile **)malloc(verifylist.size() * 
                                                                                sizeof (Par2RepairerSourceFile *));
  for (unsigned int i = 0; i < verifylist.size(); i++)
  {
    lSourceFileArray[i] = verifylist[i];
  }
  
  // Note: cannot use finalresult as __block; that messes up the returned  value from this function!
  __block bool lAllFilesResult = true;    // Optimistic default
  dispatch_apply(verifylist.size(), dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
                 ^(size_t aIndex){
                   if (!this->Verify1TargetFile(verifylist[aIndex]))
                   {
                     dispatch_semaphore_wait(genericSema, DISPATCH_TIME_FOREVER);
                     lAllFilesResult = false;
                     dispatch_semaphore_signal(genericSema);
                   }
                 });
  free(lSourceFileArray);
  finalresult = lAllFilesResult;  // As noted above, cannot return lAllFilesResult directly
  
  // Find out how much data we have found
  UpdateVerificationResults();

  return finalresult;
}

bool Par2Repairer::Verify1TargetFile(Par2RepairerSourceFile *aSourceFile)
{
  // Verifies the target file that is associated with aSourceFile.
  // Return value indicates success or failure
  bool rv = false;
  void *lPool = OSXStuff::SetupAutoreleasePool();  // Block uses Cocoa

  DiskFile *targetfile = aSourceFile->GetTargetFile();
  
  // Close the file
  if (targetfile->IsOpen())
    targetfile->Close();
  
  // Mark all data blocks for the file as unknown
  vector<DataBlock>::iterator sb = aSourceFile->SourceBlocks();
  for (u32 blocknumber=0; blocknumber<aSourceFile->BlockCount(); blocknumber++)
  {
    sb->ClearLocation();
    ++sb;
  }
  
  // Say we don't have a complete version of the file
  aSourceFile->SetCompleteFile(0);
  
  // Re-open the target file
  if (targetfile->Open(true))  // true: expect to read all data of the file
  {  
    // Verify the file again
    if (VerifyDataFile(targetfile, aSourceFile))
    {
      rv = true;
    }
    // Close the file again
    targetfile->Close();
  }  
  OSXStuff::ReleaseAutoreleasePool(lPool);
  return rv;
}

// Delete all of the partly reconstructed files.
bool Par2Repairer::DeleteIncompleteTargetFiles(void)
{
  vector<Par2RepairerSourceFile*>::iterator sf = verifylist.begin();

  // Iterate through each file in the verification list
  while (sf != verifylist.end())
  {
    Par2RepairerSourceFile *sourcefile = *sf;
    if (sourcefile->GetTargetExists())
    {
      DiskFile *targetfile = sourcefile->GetTargetFile();

      // Close and delete the file
      if (targetfile->IsOpen())
        targetfile->Close();
      targetfile->Delete();

      // Forget the file
      diskFileMap.Remove(targetfile);
      delete targetfile;

      // There is no target file
      sourcefile->SetTargetExists(false);
      sourcefile->SetTargetFile(0);
    }

    ++sf;
  }

  return true;
}
