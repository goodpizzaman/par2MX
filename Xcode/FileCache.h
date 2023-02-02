/*
 *  FileCache.h
 *  PBpar2
 *
 *  Created by Gerard Putter on 24-12-09.
 *  Copyright 2009 Gerard Putter. All rights reserved.
 *
 *  $Id: FileCache.h,v 1.1 2009/12/25 12:29:13 gputter Exp $
 *
 */

/*
 * Note: after some experiments, the class is currently not used. See comments in the commented-out
 * Open method in DiskFileX.mm. The singleton object is created, but the methods are never called.
 *
 * The FileCache is a singleton object, responsible for storing the contents of entire input files
 * that are used in the par2 process. This main benefit of the cache is to keep file data round 
 * between the verification step and an (optional) subsequent repair, so it does not have to be
 * re-read from disk. This saves I/O, and therefore speeds up the process.
 *
 * The FileCache object serves as a factory for NSData objects containing data for the entire
 * file. It is typically used by a DiskFile object, when it wants to open a file.
 * There are two ways to obtain such a "full file" buffer:
 * - GetFileDataConditional returns the data if it happens to be in the cache; otherwise it
 *   returns nil.
 * - GetFileData also returns the data if it is readily available, but tries to create a new
 *   object if the data is not available. It sees to it that there is sufficient free internal
 *   memory to hold the object. If there isn't, it will  try to free up memory by deleting existing
 *   cache entries. The rationale is, that the file at hand must be processed anyway, which occurs 
 *   faster if read entirey into memory. Files in the cache might or might not be needed in the
 *   near future.
 *   The method might still return nil if not sufficient memory is available. Note that we want
 *   internal memory, in order to avoid additional I/O caused by virtual memory swapping.
 *
 * If the FileCache returns a properly set up full file buffer, it adds one to its usecount. It is
 * the responsibility of the caller to tell the FileCache when it is done with the object by
 * calling DoneWithData.
 *
 * The external variable gFileCache points to the singleton object. There is no need to create another
 * instance.
 *
 * The implementation is thread-safe.
 *
 * Note that in this header file we avoid the use of Cocoa classes, so it can still be compiled with
 * C++ sources.
 */

#ifndef FILECACHE_H_INCLUDED_C207D7E3_3A51_478D_8324_206DA215993C
#define FILECACHE_H_INCLUDED_C207D7E3_3A51_478D_8324_206DA215993C

#include <dispatch/dispatch.h>

class FileCache
{
public:
	FileCache();
	~FileCache();
	
	// The file path is supposed to be encoded UTF8
	void *GetFileData(const char *aFilePath, bool &aCacheHit);		// Actually returns NSData*
	void *GetFileDataConditional(const char *aFilePath);	// Actually returns NSData*
	void DoneWithFileData(const char *aFilePath);
private:
	void *mDataMap;		// Actually NSMutableDictionary
	dispatch_semaphore_t mGenericSema;
	void *TryFileData(const char *aFilePath);
	bool TryToFreeMemory(uint64_t aNumRequestedBytes);
	uint64_t AverageDataSize();		// Of the files in the cache
};

extern FileCache *gFileCache;

#endif // FILECACHE_H_INCLUDED_C207D7E3_3A51_478D_8324_206DA215993C
