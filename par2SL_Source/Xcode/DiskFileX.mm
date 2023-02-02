/*
 *  diskfileX.cpp
 *  PBpar2
 *
 *  Created by Gerard Putter on Sun Feb 22 2004.
 *  Copyright (c) 2004 Gerard Putter. All rights reserved.
 *
 */

/* A Mac OSX implementation of the DiskFile class. It can deal with unicode file
   names the correct way. */

#import <Foundation/Foundation.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "par2cmdline.h"
#include "OSXStuff.h"
#include "FileCache.h"

#ifndef NDEBUG
// If DETAILEDTRACEFILE is defined, every input related operation (Open, Read, Write, Create, Close)
// results in a line in a log file. It does not use the system log facility, because the output
// could easily grow too large. So use with caution.
// The log file in question is created once, at the first call to TraceIO.
//#define DETAILEDTRACEFILE "./IOTrace.txt"
#endif

#ifdef DETAILEDTRACEFILE
dispatch_semaphore_t gGenericSema = dispatch_semaphore_create(1);
FILE	*gDetailedTraceFile = NULL;	// Thread safe, protected with gGenericSema

// Utility function, to provide the time stamp in the trace file, including a separator at the end
static std::string TimeStamp()
{
	NSDate *lNow = [NSDate date];
	int lMilliSeconds = 1000 * ([lNow timeIntervalSince1970] - floor([lNow timeIntervalSince1970]));
	NSString *lTimeStamp = [NSDateFormatter localizedStringFromDate:lNow
														  dateStyle:NSDateFormatterMediumStyle
														  timeStyle:NSDateFormatterMediumStyle];
	// Glue milliseconds to the time stamp
	NSString *lResult = [NSString stringWithFormat:@"%@.%03d - ", lTimeStamp, lMilliSeconds];
	return std::string([lResult cStringUsingEncoding:NSUTF8StringEncoding]);
}

static void TraceIO(const char *aText, ...)
{
	
	va_list     args;
	va_start(args, aText);
	dispatch_semaphore_wait(gGenericSema, DISPATCH_TIME_FOREVER);
	if (!gDetailedTraceFile)
	{
		gDetailedTraceFile = fopen(DETAILEDTRACEFILE, "w");
	}
	assert(gDetailedTraceFile);
	fprintf(gDetailedTraceFile, "%s", TimeStamp().c_str());
	vfprintf(gDetailedTraceFile, aText, args);
	dispatch_semaphore_signal(gGenericSema);	
	va_end(args);
}
#else
// Completely disappears in release build
static inline void TraceIO(const char *aText, ...) {}
#endif

#define MaxOffset 0x7fffffffffffffffULL
#define MaxLength 0xffffffffUL

/* Given a file name as it may appear in the par2 file, convert it to Unicode.
   Of course the problem is we don't know how it was encoded: code page 1250, UTF-8, 
   or whatever. Par2 not using Unicode is a BIG mistake. This function first sees if
   an encoding is indicated by the environment variable MPDL_FILENAME_ENCODING.
   If so, it uses that encoding. If not, it tries to make the best of it.
   The encoding is an integer number, as defined in NSString.h.

   An interesting aspect is that the parameter can be a full path. If directories
   are present, they refer to existing directories in the file system, and therefore
   can be converted to Unicode effortlessly. The string encoding (if present) only applies
   to the actual file name.
*/
NSString *Par2Filename2Unicode (const char *aFilename)
{
	// Split the file name in a directory path and a pure file name
	std::string lDirPath;
	std::string lFilename;
	NSString *lFilenameUnicode = nil;
	NSString *lDirPathUnicode;
	
	DiskFile::SplitFilename (aFilename, lDirPath, lFilename);

	// See if the environment variable is there
	const char *lEncodingStr = getenv ("MPDL_FILENAME_ENCODING");
	if (lEncodingStr)
	{
		// Apply the predefined encoding. We trust the environment variable is OK
		NSStringEncoding lEncoding = (NSStringEncoding) atoi(lEncodingStr);
		NSData *lFilenameAsData = [NSData dataWithBytes : lFilename.c_str ()
												 length : lFilename.length ()];
		lFilenameUnicode = [[[NSString alloc] initWithData : lFilenameAsData
												  encoding : lEncoding] autorelease];
	}
	else
	{
		// We don't have a predefined encoding; OK, then try and make the best of it.
		// First try file system encoding. This can always be done in a safe way.
		lFilenameUnicode = [[NSFileManager defaultManager] stringWithFileSystemRepresentation : lFilename.c_str ()
																					   length : lFilename.length ()];
		if (!lFilenameUnicode)
		{
			// That didn't work. OK, then try the Code Page 1252 approach. This is 
			// applicable when the par2 file came from a Windows system (big chance).
			NSData *lFilenameAsData = [NSData dataWithBytes : lFilename.c_str ()
													 length : lFilename.length ()];
			lFilenameUnicode = [[[NSString alloc]
					initWithData : lFilenameAsData encoding : NSWindowsCP1252StringEncoding] autorelease];
		}
	} // End if we don't have a predefined encoding
	
	// If our efforts didn't result in a file name, apply a default encoding. Actually, this shouldn't happen.
	if (!lFilenameUnicode)
		lFilenameUnicode = [NSString stringWithCString : lFilename.c_str () encoding:[NSString defaultCStringEncoding]];

	// It appears some files can be represented by different names. For example, \U00dc
	// is character Ü, but U\U0308 is also Ü (as two characters). The next function call
	// normalizes this differences and yields the actual name of the file, if it exists. 
	// If not, the path remains unchanged.
	lFilenameUnicode = [[lFilenameUnicode stringByDeletingLastPathComponent]
		stringByAppendingPathComponent : [[NSFileManager defaultManager] displayNameAtPath : lFilenameUnicode]];
	
	// Finally make the full path complete by prepending the Unicode rep of the directory path.
	lDirPathUnicode = [[NSFileManager defaultManager] stringWithFileSystemRepresentation : lDirPath.c_str ()
																				  length : lDirPath.length ()];
	assert (lDirPathUnicode);	// We know lDirPath is valid and not NULL, so neither should be lDirPathUnicode
    return [lDirPathUnicode stringByAppendingPathComponent : lFilenameUnicode];
}

//-----------------------------------------------------------------------------
NSString *FileSystemFilename2Unicode (const char *aFilename)
{
	// Similar to Par2Filename2Unicode, but in this case we know the encoding of aFilename
	// is the OSX encoding.
	return [[NSFileManager defaultManager] stringWithFileSystemRepresentation : aFilename
																	   length : strlen (aFilename)];
}

//-----------------------------------------------------------------------------
DiskFile::DiskFile(void) : filename ()
{
	filesize = 0;
	offset = 0;		// Actually not used?
	mFile = nil;
	exists = false;
	mFullFileBuffer = nil;
}

//-----------------------------------------------------------------------------
DiskFile::~DiskFile(void)
{
	[((NSMutableData *)mFullFileBuffer) release];
	[((NSFileHandle *)mFile) release];
}

//-----------------------------------------------------------------------------
// Create a file and set its length; also opens the file.
bool DiskFile::Create(string _filename, u64 _filesize)
{
	TraceIO("Create file \"%s\" with size %llu\n", _filename.c_str(), (unsigned long long) _filesize);

	assert(((NSFileHandle *)mFile) == nil);

	filename = _filename;
	filesize = _filesize;
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (_filename.c_str ());
	if ([[NSFileManager defaultManager] createFileAtPath : lUnicodeFilename 
												contents : nil attributes : nil])
	{
		mFile = [[NSFileHandle fileHandleForWritingAtPath : lUnicodeFilename] retain];
	}
	
	if (((NSFileHandle *)mFile) == nil)
	{
		cerr << "Could not create: " << _filename << endl;
		return false;
	}
	
	if (_filesize > MaxOffset)
	{
		cerr << "Requested file size for " << _filename << " is too large." << endl;
		return false;
	}
	
	if (_filesize > 0)
	{
		BOOL lSeekResult;
		NS_DURING
			[((NSFileHandle *)mFile) seekToFileOffset : (unsigned long long)_filesize - 1];
			lSeekResult = TRUE;
		NS_HANDLER
			lSeekResult = FALSE;
		NS_ENDHANDLER
		
		if (!lSeekResult)
		{
			[((NSFileHandle *)mFile) release];
			mFile = 0;
			[[NSFileManager defaultManager] removeItemAtPath:lUnicodeFilename error:nil];
			
			// For some ridiculous reason the compiler choked on 
			//		cerr << "Could not create: " << _filename
			// But only in the 64-bit compile and on this cerr line and the next. All the others went OK!
			cerr << "Could not create: " << _filename.c_str () << endl;
			
			return false;
		}
		
		// Write 1 byte to make file full size:
		BOOL lWriteResult;
		NS_DURING
			[((NSFileHandle *)mFile) writeData : [NSData dataWithBytesNoCopy : &_filesize length : 1 freeWhenDone : FALSE]];
			lWriteResult = TRUE;
		NS_HANDLER
			lWriteResult = FALSE;
		NS_ENDHANDLER
		
		if (!lWriteResult)
		{
			[((NSFileHandle *)mFile) release];
			mFile = 0;
			[[NSFileManager defaultManager] removeItemAtPath:lUnicodeFilename error:nil];
			
			// For some ridiculous reason the compiler choked on 
			//		cerr << "Could not create: " << _filename
			// But only in the 64-bit compile and on this cerr line and the previous. All the others went OK!
			cerr << "Could not set end of file: " << _filename.c_str () << endl;

			return false;
		}
	}
	
	offset = filesize;
	
	exists = true;

	return true;
}

//-----------------------------------------------------------------------------
// Write some data to the file
bool DiskFile::Write(u64 _offset, const void *buffer, size_t length)
{
	TraceIO("Write file \"%s\" offset %llu length %llu\n", filename.c_str(),
			(unsigned long long) _offset, (unsigned long long) length);

	assert(((NSFileHandle *)mFile) != nil);

	bool rv = false;

	if (offset != _offset)
	{
		if (_offset > MaxOffset)
		{
			cerr << "Could not write " << (u64)length << " bytes to " << filename << " at offset " << _offset << endl;
			return false;
		}
		
		bool lResult = true;
		NS_DURING
			[((NSFileHandle *)mFile) seekToFileOffset : (unsigned long long) _offset];
		NS_HANDLER
			lResult = false;
		NS_ENDHANDLER
		
		if (!lResult)
		{
			cerr << "Could not write " << (u64)length << " bytes to " << filename << " at offset " << _offset << endl;
			return false;
		}
		
		offset = _offset;
	}
	
	if (length > MaxLength)
	{
		cerr << "Could not write " << (u64)length << " bytes to " << filename << " at offset " << _offset << endl;
	}
	else
	{
		NS_DURING
			[((NSFileHandle *)mFile) writeData : [NSData dataWithBytesNoCopy : const_cast<void *> (buffer) 
													length : length freeWhenDone : FALSE]];
			rv = true;
		NS_HANDLER
			;
		NS_ENDHANDLER
		
		if (!rv)
		{
			cerr << "Could not write " << (u64)length << " bytes to " << filename << " at offset " << _offset << endl;
		}
		else
		{
			offset += length;
			if (filesize < offset)
			{
				filesize = offset;
			}
		}
	}
	
	return rv;
}

//-----------------------------------------------------------------------------
// Open the file
bool DiskFile::Open(bool tryToCacheData)
{
	string _filename = filename;
	return Open(_filename, tryToCacheData);
}

//-----------------------------------------------------------------------------
bool DiskFile::Open(string _filename, bool tryToCacheData)
{
	return Open(_filename, GetFileSize(_filename), tryToCacheData);
}

#ifdef FILE_CACHE_EXPERIMENT
// This version of the Open method uses an approach that at first seemed like a good idea: it
// reads the entire file into a buffer, so that the Read method can simply copy some data. It is
// clear that this is only appropriate if the caller knows that all data in the file will be 
// processe, hence the argument tryToCacheData (if true, the file is loaded as described). 
// For the verification run this resulted in a modest gain (about 4% faster). However, if a 
// subsequent repair was necessary, the result was disasterous: the repair time more than doubled.
// I suspect that this is caused by some form of file caching: during repair all files need to be
// opened, read, and processed again. However, this time not all of the file is necessary per 
// Open/Close. In the original scenario, using traditional I/O, somehow the system was able to
// provide blocks from the file without doing the actual I/O, even though I specify NO_CACHING. 
// In the "read all file" scenario this didn't happen.
// I tried to solve this with my own caching scheme (the FileCache class), but could not get it
// to work properly; filling up alomost all memory leads to trouble.
// So in the end, I decided to let the OS do its thing, and not use the new scheme. Left the code
// in, though.
//-----------------------------------------------------------------------------
bool DiskFile::Open(string _filename, u64 _filesize, bool tryToCacheData)
{
	TraceIO("Open file \"%s\"; tryToCacheData=%s\n", _filename.c_str(),
			tryToCacheData?"true":"false");
	bool rv = false;
	assert(mFile == nil && mFullFileBuffer == nil);	// File must not be "open" in any sense
	
	filename = _filename;
	filesize = _filesize;
	
	if (_filesize > MaxOffset)
	{
		cerr << "File size for " << _filename << " is too large." << endl;
		return false;
	}
	
	// First of all, see if the data for the file is readily available in the cache.
	NSData *lFileData = nil;
	
	if (tryToCacheData)
	{
		bool lCacheHit;
		lFileData = (NSData *) gFileCache->GetFileData(FS2UTF8(filename).c_str(), lCacheHit);
		if (lFileData)
		{
			TraceIO("Cache hit for data of file \"%s\"\n", filename.c_str());
		}
	}
	else
	{
		lFileData = (NSData *) gFileCache->GetFileDataConditional(FS2UTF8(_filename).c_str());
		if (lFileData)
		{
			TraceIO("Cache hit for data of file \"%s\"\n", filename.c_str());
		}
	}
	if (lFileData)
	{
		// Don't even bother opening the file.
		mFullFileBuffer = [lFileData retain];
		offset = 0;
		exists = true;
		return true;
	}
	
	// OK, we'll have to open the file the traditional way. If mFullFileBuffer is nil (which it is),
	// the class falls back on regular I/O, without using our full file buffer.
	TraceIO("Will NOT use full file buffer for file \"%s\"\n", filename.c_str());
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (_filename.c_str ());
	mFile = [[NSFileHandle fileHandleForReadingAtPath : lUnicodeFilename] retain];
	
	if (mFile != nil)
	{
		// Open succeeded
		// Set the file access to "no caching". This seems to speed up verification a bit.
		if (fcntl([(NSFileHandle *)mFile fileDescriptor], F_NOCACHE, 1) == -1)
		{
			NSLog (@"fcntl F_NOCACHE failed on file \"%s\"", _filename.c_str());
		}
		// Let the OS handle read ahead. I observed a noticeable performance improvement 
		// in the verification part.
		if (fcntl([(NSFileHandle *)mFile fileDescriptor], F_RDAHEAD, 1) == -1)
		{
			NSLog (@"fcntl F_RDAHEAD failed");
		}
		
		offset = 0;
		exists = true;
		rv = true;
	}
	
	return rv;
}
#else
//-----------------------------------------------------------------------------
bool DiskFile::Open(string _filename, u64 _filesize, bool tryToCacheData)
{
	// Note that in this version tryToCacheData is not used. See comments in the alternative Open.
	TraceIO("Open file \"%s\"; tryToCacheData=%s\n", _filename.c_str(), tryToCacheData?"true":"false");

	bool rv = false;
	assert(mFile == nil);	// File must not be open
	
	filename = _filename;
	filesize = _filesize;
	
	if (_filesize > MaxOffset)
	{
		cerr << "File size for " << _filename << " is too large." << endl;
		return false;
	}
	
	// OK, we'll have to open the file the traditional way (see comments in alternative Open).
	// If mFullFileBuffer is nil, the class falls back on regular I/O, without using our full
	// file buffer.
	mFullFileBuffer = nil;
	
	TraceIO("Will NOT use full file buffer for file \"%s\"\n", filename.c_str());
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (_filename.c_str ());
	mFile = [[NSFileHandle fileHandleForReadingAtPath : lUnicodeFilename] retain];
	
	if (mFile != nil)
	{
		// Open succeeded
		// Set the file access to "no caching". This speeds up verification
		if (fcntl([(NSFileHandle *)mFile fileDescriptor], F_NOCACHE, 1) == -1)
		{
			NSLog (@"fcntl F_NOCACHE failed on file \"%s\"", _filename.c_str());
		}
		// Let the OS handle read ahead. I observed a noticeable performance improvement 
		// in the verification part.
		if (fcntl([(NSFileHandle *)mFile fileDescriptor], F_RDAHEAD, 1) == -1)
		{
			NSLog (@"fcntl F_RDAHEAD failed");
		}
		
		offset = 0;
		exists = true;
		rv = true;
	}
	
	return rv;
}
#endif

//-----------------------------------------------------------------------------
// Read some data from the file, without our own read ahead buffer
bool DiskFile::ReadWithoutFFBuffer(u64 _offset, void *buffer, size_t length)
{
	TraceIO("Read without FF buffer, file \"%s\", offset %llu, length %llu\n", 
			filename.c_str(), (unsigned long long) _offset, (unsigned long long) length);

	bool rv = false;

	assert(((NSFileHandle *)mFile) != nil);
	
	if (offset != _offset)
	{
		if (_offset > MaxOffset)
		{
			cerr << "Could not read " << (u64)length << " bytes from " << filename << " at offset " << _offset << endl;
			return false;
		}
		
		bool lResult = true;
		NS_DURING
			[((NSFileHandle *)mFile) seekToFileOffset : (unsigned long long) _offset];
		NS_HANDLER
			lResult = false;
		NS_ENDHANDLER
		if (!lResult)
		{
			cerr << "Could not read " << (u64)length << " bytes from " << filename << " at offset " << _offset << endl;
			return false;
		}

		offset = _offset;
	} // If must change offset
	
	if (length > MaxLength)
	{
		cerr << "Could not read " << (u64)length << " bytes from " << filename << " at offset " << _offset << endl;
	}
	else
	{
		// Finally do the actual I/O. 
		// Avoid using Cocoa, for more efficiency in memory and CPU
		ssize_t lResult = read([((NSFileHandle *)mFile) fileDescriptor], buffer, length);
		if (lResult != (ssize_t) length)
		{
			cerr << "Could not read " << (u64)length << " bytes from " << filename << " at offset " << _offset << endl;
		}
		else
		{
			offset += length;
			rv = true;
		}
	}
	
	return rv;
}

//-----------------------------------------------------------------------------
bool DiskFile::ReadUsingFFBuffer(u64 aOffset, void *aBuffer, size_t aLength)
{
	// NOTE: not used at the moment. See comments in alternative Open.
	TraceIO("Read using FF buffer, file \"%s\", offset %llu, length %llu\n", 
			filename.c_str(), (unsigned long long) aOffset, (unsigned long long) aLength);

	assert (mFullFileBuffer);

	// Sanity checks
	if (aLength > MaxLength || aOffset > MaxOffset)
	{
		cerr << "Could not read " << (u64)aLength << " bytes from " << filename << " at offset "
			 << aOffset << "; offset or length exceed maximum values allowed" << endl;
		return false;
	}
	
	// Copy the requested bytes from the full file buffer
	NSRange lRangeToCopy = NSMakeRange(aOffset, aLength);
	[((NSMutableData *)this->mFullFileBuffer) getBytes:aBuffer range:lRangeToCopy];

	// Update the weird instance var "offset"
	offset = aOffset + aLength;

	return true;
}

//-----------------------------------------------------------------------------
// Read data from the file
bool DiskFile::Read(u64 _offset, void *buffer, size_t length)
{
	bool rv = false;
	
	// We will use the full file buffer if one has been allocated at Open.
	if (mFullFileBuffer)
	{
		rv = this->ReadUsingFFBuffer(_offset, buffer, length);
	}
	else
	{
		rv = this->ReadWithoutFFBuffer(_offset, buffer, length);
	}
	return rv;
}

//-----------------------------------------------------------------------------
// Close the file
void DiskFile::Close(void)
{
	TraceIO("Close file \"%s\"\n", filename.c_str());

	if (mFullFileBuffer)	// Note: in this version, mFullFileBuffer is not used
	{
#ifdef FILE_CACHE_EXPERIMENT
		// Tell the file cache we are finished with the data. No cache used in this version
		gFileCache->DoneWithFileData(FS2UTF8(filename).c_str());
#endif
		[((NSMutableData *)mFullFileBuffer) release];	// Might be nil
		mFullFileBuffer = nil;
	}
	[((NSFileHandle *)mFile) release];	// Might be nil
	mFile = nil;
}

//-----------------------------------------------------------------------------
// Rename the file to a self-generated name by appending a number. Increase
// the number until a non-existing file has been found.
bool DiskFile::Rename(void)
{
	// Determine the new name without any dependence on a max buffer length.
	u32 lIndex = 0;
	char lNumberAsString [20];		// Certainly long enough
	struct stat st;
	string newname;

	do
	{
		sprintf (lNumberAsString, "%u", ++lIndex);
		newname = string (filename.c_str()).append (".");
		newname.append (lNumberAsString);
	} while (stat(newname.c_str (), &st) == 0);
	return Rename(newname.c_str ());
}

//-----------------------------------------------------------------------------
bool DiskFile::Rename(string _filename)
{
	// Rename ourselves to _filename
	bool	rv = false;
	
	assert(((NSFileHandle *)mFile) == 0);		// File must not be open
	
	NSString *lOldUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());
	NSString *lNewUnicodeFilename = FileSystemFilename2Unicode (_filename.c_str ());
	
	if ([[NSFileManager defaultManager] moveItemAtPath : lOldUnicodeFilename 
												toPath : lNewUnicodeFilename error : nil])
	{
		filename = _filename;
		rv = true;
	}
	else
	{
		cerr << filename << " cannot be renamed to " << _filename << endl;
		rv = false;
	}
	
	return rv;
}

//-----------------------------------------------------------------------------
// Delete the file
bool DiskFile::Delete(void)
{
	bool	rv = false;
	assert (((NSFileHandle *)mFile) == 0);
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());

	if ([lUnicodeFilename length] > 0 && 
		[[NSFileManager defaultManager] removeItemAtPath:lUnicodeFilename error:nil])
	{
		rv = true;
	}
	else
	{
		cerr << "Cannot delete " << filename << endl;
		
		rv = false;
	}

	return rv;
}

//-----------------------------------------------------------------------------
string DiskFile::GetCanonicalPathname(string filename)
{
	// This function is independent of Unicode filenames; therefore it is a copy
	// of the same function in the original implementation in diskfile.cpp
	
	// Is the supplied path already an absolute one
	if (filename.size() == 0 || filename[0] == '/')
		return filename;
	
	// Get the current directory
	char curdir[1000];
	if (0 == getcwd(curdir, sizeof(curdir)))
	{
		return filename;
	}
	
	
	// Allocate a work buffer and copy the resulting full path into it.
	char *work = new char[strlen(curdir) + filename.size() + 2];
	strcpy(work, curdir);
	if (work[strlen(work)-1] != '/')
		strcat(work, "/");
	strcat(work, filename.c_str());
	
	char *in = work;
	char *out = work;
	
	while (*in)
	{
		if (*in == '/')
		{
			if (in[1] == '.' && in[2] == '/')
			{
				// skip the input past /./
				in += 2;
			}
			else if (in[1] == '.' && in[2] == '.' && in[3] == '/')
			{
				// backtrack the output if /../ was found on the input
				in += 3;
				if (out > work)
				{
					do
					{
						out--;
					} while (out > work && *out != '/');
				}
			}
			else
			{
				*out++ = *in++;
			}
		}
		else
		{
			*out++ = *in++;
		}
	}
	*out = 0;
	
	string result = work;
	delete [] work;
	
	return result;
}

//-----------------------------------------------------------------------------
void DiskFile::SplitFilename(string filename, string &path, string &name)
{
	// This function is independent of Unicode filenames; therefore it is a copy
	// of the same function in the original implementation in diskfile.cpp.
	// However, removed parsing on backslash (Windows style).

	string::size_type where;
	
	if (string::npos != (where = filename.find_last_of('/'))
		/* ||
		string::npos != (where = filename.find_last_of('\\')) */)
	{
		path = filename.substr(0, where+1);
		name = filename.substr(where+1);
	}
	else
	{
		path = "." PATHSEP;
		name = filename;
	}
}

//-----------------------------------------------------------------------------
std::string DiskFile::FS2UTF8(const char *aFilename)
{
	// Input is a file name in file system representation. Result is the same string,
	// converted to UTF8.
	// The result of this function is intended for the output to stdout / stderr
	NSString *lTemp = FileSystemFilename2Unicode(aFilename);
	return std::string([lTemp cStringUsingEncoding:NSUTF8StringEncoding]);
}

//-----------------------------------------------------------------------------
std::string DiskFile::FS2UTF8(const std::string &aFilename)
{
	return FS2UTF8(aFilename.c_str());
}

//-----------------------------------------------------------------------------
// Take a filename from a PAR2 file and replace any characters
// which would be illegal for a file on disk.
// And on OSX I do an essential extra feature: I try to establish the correct file system
// representation, taking the probable string encoding of filename into account. Do this
// after eliminating the invalid characters; they are always invalid, no matter what the 
// encoding is.
string DiskFile::TranslateFilename(string filename)
{
	string result;
	
	string::iterator p = filename.begin();
	while (p != filename.end())
	{
		unsigned char ch = *p;
		
		bool ok = true;
		if (ch < 32)			// Character must not be a control char
		{
			ok = false;
		}
		else
		{
			switch (ch)
			{
				case '/':
					ok = false; // Slash is not allowed
			}
		}
		
		if (ok)
		{
			result += ch;
		}
		else
		{
			// convert problem characters to hex
			result += ((ch >> 4) < 10) ? (ch >> 4) + '0' : (ch >> 4) + 'A'-10;
			result += ((ch & 0xf) < 10) ? (ch & 0xf) + '0' : (ch & 0xf) + 'A'-10;
		}
		
		++p;
	}
	
	// Now go to the FS representation, via Unicode
	NSString *lUnicodeName = Par2Filename2Unicode (result.c_str ());
	result = [[NSFileManager defaultManager] fileSystemRepresentationWithPath : lUnicodeName];

	return result;
}

//-----------------------------------------------------------------------------
bool DiskFile::FileExists(string filename)
{
	bool rv = false;
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());

	if (lUnicodeFilename)
	{
		// We don't want to traverse a symbolic link, if filename is one. Before Leopard, this
		// was done with fileAttributesAtPath:traverseLink. However, in Leopard it was
		// deprecated and replaced by a method without the traverseLink option. So now I first
		// make sure it isn't a symbolic link.
		NSString *lDest = [[NSFileManager defaultManager] destinationOfSymbolicLinkAtPath:lUnicodeFilename
																					error:nil];
		if (lDest)
		{
			rv = false;		// It is a symbolic link
		}
		else
		{
			NSDictionary *lFileAttr = [[NSFileManager defaultManager] attributesOfItemAtPath:lUnicodeFilename
																					   error:nil];
			if (lFileAttr)
			{
				rv = [[lFileAttr objectForKey : NSFileType] isEqualToString : NSFileTypeRegular];
			}
		}
	}

	return rv;
}


//-----------------------------------------------------------------------------
u64 DiskFile::GetFileSize(string filename)
{
	u64 rv = 0;

	NSString *lUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());

	// destinationOfSymbolicLinkAtPath traverses symbolic links if filename is one. To avoid that,
	// first test if it IS a link.
	NSString *lDest = [[NSFileManager defaultManager] destinationOfSymbolicLinkAtPath:lUnicodeFilename
																				error:nil];
	if (!lDest)
	{
		// It is not a symbolic link
		NSDictionary *lFileAttr = [[NSFileManager defaultManager] attributesOfItemAtPath:lUnicodeFilename 
																				   error:nil];
		if (lFileAttr)
		{
			if ([[lFileAttr objectForKey : NSFileType] isEqualToString : NSFileTypeRegular])
			{
				// It is a regular file
				rv = [[lFileAttr objectForKey : NSFileSize] longLongValue];
			}
		}
	}	
	return rv;
}

//-----------------------------------------------------------------------------
// Search the specified path for files which match the specified wildcard
// and return their names in a list.
list<string>* DiskFile::FindFiles(string path, string wildcard)
{
	// Apparently this function works without Unicode filename problems, so
	// this is a straight copy of the original implementation in diskfile.cpp
	list<string> *matches = new list<string>;
	
	string::size_type where;
	
	if ((where = wildcard.find_first_of('*')) != string::npos ||
		(where = wildcard.find_first_of('?')) != string::npos)
	{
		string front = wildcard.substr(0, where);
		bool multiple = wildcard[where] == '*';
		string back = wildcard.substr(where+1);
		
		DIR *dirp = opendir(path.c_str());
		if (dirp != 0)
		{
			struct dirent *d;
			while ((d = readdir(dirp)) != 0)
			{
				string name = d->d_name;
				
				if (name == "." || name == "..")
					continue;
				
				if (multiple)
				{
					if (name.size() >= wildcard.size() &&
						name.substr(0, where) == front &&
						name.substr(name.size()-back.size()) == back)
					{
						matches->push_back(path + name);
					}
				}
				else
				{
					if (name.size() == wildcard.size())
					{
						string::const_iterator pw = wildcard.begin();
						string::const_iterator pn = name.begin();
						while (pw != wildcard.end())
						{
							if (*pw != '?' && *pw != *pn)
								break;
							++pw;
							++pn;
						}
						
						if (pw == wildcard.end())
						{
							matches->push_back(path + name);
						}
					}
				}
				
			}
			closedir(dirp);
		}
	}
	else
	{
		struct stat st;
		string fn = path + wildcard;
		if (stat(fn.c_str(), &st) == 0)
		{
			matches->push_back(path + wildcard);
		}
	}
	
	return matches;
}

//-----------------------------------------------------------------------------
bool DiskFile::FileConsideredOK ()
{
	/* When the file name occurs in the environment variable PAR2_FILES_CONSIDERED_OK, the function 
	   returns true, else false.
	   The contents of the environment variable consists of file names, without the
	   directory path, separated by a tab (\t) character, expressed in UTF8 encoding. */
	bool rv = false;
	const char *lOKFilesEnv = getenv ("PAR2_FILES_CONSIDERED_OK");
	if (!lOKFilesEnv || !*lOKFilesEnv)
		return false;	// Data is absent or empty
	if (this->filename.empty())
		return false;

	// Determine our file name without preceding path
	std::string lPath;
	std::string lName;
	DiskFile::SplitFilename(this->filename, lPath, lName);
	if (lName.empty())
		return false;

	// Make copy of lOKFilesEnv; we are going to mess with its contents
	char *lOKFiles = (char *) malloc (strlen (lOKFilesEnv) + 1);	// One for the \0
	strcpy (lOKFiles, lOKFilesEnv);
	char *lOKFilesRemainder = lOKFiles;	// strsep will modify the pointer as well as the contents
	char *lCurrentOKFile;
	// Note we use strsep, not strtok. strsep is supposed to be thread-safe.
	while ((lCurrentOKFile = strsep (&lOKFilesRemainder, "\t")))
	{
		// Convert lCurrentOKFile into file system representation, via an NSString. 
		NSString *lCurrentOKFileString = [NSString stringWithCString:lCurrentOKFile 
															encoding:NSUTF8StringEncoding];
		// I discovered that fileSystemRepresentationWithPath chokes if it is given an empty
		// string, so avoid that.
		if ([lCurrentOKFileString length] > 0)
		{
			const char *lFilesystemRep = [[NSFileManager defaultManager] 
										  fileSystemRepresentationWithPath:lCurrentOKFileString];
			if (strcmp (lName.c_str (), lFilesystemRep) == 0)
			{
				rv = true;
				break;	// From the while
			}
		}
	}
	free (lOKFiles);
	return rv;
}

//-----------------------------------------------------------------------------
std::string DiskFile::Par2Representation (std::string aFilename)
{
	// We know aFilename is encoded in "Mac OSX file system representation". Translate it into
	// a form Quickpar can work with. Shame on the par2 standard for not using unicode and not
	// even enforcing a specific string encoding!
	std::string rv = aFilename;		// As a default

	NSString *lFilenameAsString = [[NSFileManager defaultManager]
								   stringWithFileSystemRepresentation:aFilename.c_str() 
															   length:aFilename.length ()];
	NSStringEncoding lEncoding = NSISOLatin1StringEncoding;
	if ([lFilenameAsString canBeConvertedToEncoding : lEncoding])
	{
		NSData *lData = [lFilenameAsString dataUsingEncoding : lEncoding];
		rv = std::string ((const char *) [lData bytes], [lData length]);
	}
	else
	{
		lEncoding = NSWindowsCP1252StringEncoding;
		if ([lFilenameAsString canBeConvertedToEncoding : lEncoding])
		{
			NSData *lData = [lFilenameAsString dataUsingEncoding : lEncoding];
			rv = std::string ((const char *) [lData bytes], [lData length]);
		}
	}
	
	return rv;
}

//-----------------------------------------------------------------------------
// Implementation of DiskFileMap is copy of implementation in diskfile.cpp
DiskFileMap::DiskFileMap(void)
{
	// The key is the Unicode file(NSMutableDictionary*)mDiskfilemap name; the data is a DiskFile pointer, wrapped in
	// an NSValue object.
	mDiskfilemap = [[NSMutableDictionary dictionaryWithCapacity : 100] retain];
}

DiskFileMap::~DiskFileMap(void)
{
	// Delete the wrapped pointers
	NSEnumerator *lEnum = [((NSMutableDictionary*)mDiskfilemap) objectEnumerator];
	NSValue *lValue;
	while ((lValue = [lEnum nextObject]))
		delete static_cast<DiskFile *> ([lValue pointerValue]);

	[((NSMutableDictionary*)mDiskfilemap) release];
}

//-----------------------------------------------------------------------------
bool DiskFileMap::Insert(DiskFile *diskfile)
{
	bool rv = false;
	string filename = diskfile->FileName();
	assert(filename.length() != 0);
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());
	NS_DURING
		[((NSMutableDictionary*)mDiskfilemap) setObject:[NSValue valueWithPointer : diskfile] forKey : lUnicodeFilename];
		rv = true;
	NS_HANDLER
		rv = false;
	NS_ENDHANDLER

	return rv;
}

//-----------------------------------------------------------------------------
void DiskFileMap::Remove(DiskFile *diskfile)
{
	string filename = diskfile->FileName();
	assert(filename.length() != 0);
	
	NSString *lUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());
	
	[((NSMutableDictionary*)mDiskfilemap) removeObjectForKey : lUnicodeFilename];
}

DiskFile* DiskFileMap::Find(string filename) const
{
	assert(filename.length() != 0);

	DiskFile* rv = NULL;

	NSString *lUnicodeFilename = FileSystemFilename2Unicode (filename.c_str ());
	NSValue *lValue = [((NSMutableDictionary*)mDiskfilemap) objectForKey : lUnicodeFilename];
	if (lValue)
		rv = static_cast<DiskFile*> ([lValue pointerValue]);

	return rv;
}
