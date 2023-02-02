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

#ifndef __DISKFILE_H__
#define __DISKFILE_H__

// A disk file can be any type of file that par2cmdline needs to read or write data from or to.
// How to use the class: instantiate an object in any way; call Open to process a file for input,
// or Create to make a new file and at the same time open it for output. If the file to be created
// already exists, it will be overwritten.
// Use Read and Write in the usual way.
// At the end, call Close or just delete the object.

class DiskFile
{
public:
  DiskFile(void);
  ~DiskFile(void);

  // Create a file and set its length; also opens the file.
  bool Create(string filename, u64 filesize);

  // Write some data to the file
  bool Write(u64 offset, const void *buffer, size_t length);

  // Open the file. The tryToCacheData argument indicates wheteher the caller expects to
  // process all data in the file (as in a verify), and wants to cache it for possible
  // later use.
  bool Open(bool tryToCacheData);
  bool Open(string filename, bool tryToCacheData);
  bool Open(string filename, u64 filesize, bool tryToCacheData);

  // Check to see if the file is open
  bool IsOpen(void) const {return mFile != 0;}
  bool FileConsideredOK ();	// Tells whether the file can be considered OK without any verification
  static std::string Par2Representation (std::string aFilename);
  // FS2UTF8 makes a string with the UTF8 file representation of the file name in file system rep.
  // Probably they are the same, but take the safe road; we want to report everything to MPDL in UTF8.
  static std::string FS2UTF8(const char *aFilename);
  static std::string FS2UTF8(const std::string &aFilename);

  // Read data from the file
  bool Read(u64 offset, void *buffer, size_t length);
  
  // Close the file
  void Close(void);

  // Get the size of the file
  u64 FileSize(void) const {return filesize;}

  // Get the name of the file
  string FileName(void) const {return filename;}

  // Does the file exist
  bool Exists(void) const {return exists;}

  // Rename the file
  bool Rename(void); // Pick a filename automatically
  bool Rename(string filename);

  // Delete the file
  bool Delete(void);

public:
  static string GetCanonicalPathname(string filename);

  static void SplitFilename(string filename, string &path, string &name);
  static string TranslateFilename(string filename);

  static bool FileExists(string filename);
  static u64 GetFileSize(string filename);

  // Search the specified path for files which match the specified wildcard
  // and return their names in a list.
  static list<string>* FindFiles(string path, string wildcard);

private:
  string filename;
  u64    filesize;

  // OS file handle
  void  *mFile;			// Actually NSFileHandle; make sure C++ can compile

  // Current offset within the file
  u64    offset;

  // Does the file exist
  bool   exists;
  
  // We know that all input files are processed sequentially. Speed things up by maintaining
  // a buffer that holds the entire file. It is created by the FileCache singleton.
  // If there is not enough internal memory for this, the DiskFile class uses traditional I/O.
  void *mFullFileBuffer;    // Actually NSData
  
  bool ReadUsingFFBuffer(u64 aOffset, void *aBuffer, size_t aLength);
  bool ReadWithoutFFBuffer(u64 offset, void *buffer, size_t length);
};

// This class keeps track of which DiskFile objects exist
// and which file on disk they are associated with.
// It is used to avoid a file being processed twice.
class DiskFileMap
{
public:
  DiskFileMap(void);
  ~DiskFileMap(void);

  bool Insert(DiskFile *diskfile);
  void Remove(DiskFile *diskfile);
  DiskFile* Find(string filename) const;

protected:
  void *mDiskfilemap;   // Actually NSMutableDictionary. Map from filename to DiskFile
};

#endif // __DISKFILE_H__
