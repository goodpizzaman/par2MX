/*
 *  FileCache.mm
 *  PBpar2
 *
 *  Created by Gerard Putter on 24-12-09.
 *  Copyright 2009 Gerard Putter. All rights reserved.
 *
 *  $Id: FileCache.mm,v 1.2 2009/12/26 07:55:45 gputter Exp $
 *
 */

#include "FileCache.h"
#include "par2cmdline.h"
#include "OSXStuff.h"
#import <Foundation/Foundation.h>

@interface Par2FileCacheEntry : NSObject
{
	int		mUseCount;
	NSData	*mData;
}

@property (nonatomic, readonly) int useCount;
@property (nonatomic, readonly) NSData *data;

+(id) par2FileCacheEntryWithData:(NSData *)aData;	// Sets usecount to 0
-(id) initPar2FileCacheEntryWithData:(NSData *)aData;
-(void) incrementUseCount;
-(void) decrementUseCount;

@end // interface Par2FileCacheEntry

@implementation Par2FileCacheEntry

@synthesize useCount = mUseCount;
@synthesize data = mData;

//--------------------------------------------------------------------------------------------------
+(id) par2FileCacheEntryWithData:(NSData *)aData
{
	return [[[Par2FileCacheEntry alloc] initPar2FileCacheEntryWithData:aData] autorelease];
}

//--------------------------------------------------------------------------------------------------
-(id) initPar2FileCacheEntryWithData:(NSData *)aData
{
	self = [super init];
	if (self)
	{
		mUseCount = 0;
		mData = [aData retain];
	}
	return self;
}

//--------------------------------------------------------------------------------------------------
-(void) dealloc
{
	[mData release];
	[super dealloc];
}

//--------------------------------------------------------------------------------------------------
-(void) incrementUseCount
{
	mUseCount++;
}

//--------------------------------------------------------------------------------------------------
-(void) decrementUseCount
{
	mUseCount--;
}

@end // implementation Par2FileCacheEntry

static FileCache gSingletonFileCache;	// So that the destructor is executed at the end

FileCache *gFileCache = &gSingletonFileCache; 

//--------------------------------------------------------------------------------------------------
FileCache::FileCache()
{
	mDataMap = [[NSMutableDictionary dictionary] retain];	// Will contain Par2FileCacheEntry objects
	mGenericSema = dispatch_semaphore_create(1);			// Use as a mutex
}

//--------------------------------------------------------------------------------------------------
FileCache::~FileCache()
{
	[(NSMutableDictionary *)mDataMap release];
	dispatch_release(mGenericSema);
}

//--------------------------------------------------------------------------------------------------
void *FileCache::GetFileData(const char *aFilePath, bool &aCacheHit)
{
	// First see if we already have the data
	dispatch_semaphore_wait(mGenericSema, DISPATCH_TIME_FOREVER);
	void *rv = this->TryFileData(aFilePath);
	if (rv)
	{
		aCacheHit = true;
		dispatch_semaphore_signal(mGenericSema);
		return rv;
	}
	// OK, so we don't have the data. The semaphore is still locked.
	// See if there is sufficient memory to contain the file content.
	aCacheHit = false;
	u64 lRequiredSpace = DiskFile::GetFileSize(aFilePath);
	OSXStuff::MemoryStats lMemStats;
	OSXStuff::analyzeMemory(lMemStats);
	bool lCanLoad = false;
	// We calculate some extra space, namely the average size of all files we have. The rationale
	// is that I doubt that memory is reclaimed right away. We may assume that most of the files in
	// the set are roughly the same size.
	// Apart from that, we leave an arbitrary amount of memory free. Don't be too greedy.
	static const u64 cMinFree = 100 * 1024 * 1024;	// This many megabytes
	if (lRequiredSpace + this->AverageDataSize() + cMinFree < lMemStats.memFree)
	{
		lCanLoad = true;	// No problem; fits easily
	}
	else
	{
		lCanLoad = this->TryToFreeMemory(lRequiredSpace);
	}

	if (lCanLoad)
	{
		// Yes, data fits into memory. Make the data object and wrap it up in a cache entry
		NSString *lPath = [NSString stringWithCString:aFilePath encoding:NSUTF8StringEncoding];
		rv = [NSData dataWithContentsOfFile:lPath];
		if (rv)
		{
			Par2FileCacheEntry *lEntry = [Par2FileCacheEntry par2FileCacheEntryWithData:(NSData *)rv];
			[lEntry incrementUseCount];
			[(NSMutableDictionary *)mDataMap setObject:lEntry forKey:lPath];
		} // end could load file data
	}
	
	dispatch_semaphore_signal(mGenericSema);
	return rv;
}

//--------------------------------------------------------------------------------------------------
void *FileCache::GetFileDataConditional(const char *aFilePath)
{
	// See if we hapen to have the data
	dispatch_semaphore_wait(mGenericSema, DISPATCH_TIME_FOREVER);
	void *rv = this->TryFileData(aFilePath);
	dispatch_semaphore_signal(mGenericSema);
	return rv;
}

//--------------------------------------------------------------------------------------------------
void *FileCache::TryFileData(const char *aFilePath)
{
	// Caller must have locked semaphpore
	void *rv = nil;
	NSString *lPath = [NSString stringWithCString:aFilePath encoding:NSUTF8StringEncoding];
	Par2FileCacheEntry *lEntry = [(NSMutableDictionary *)mDataMap objectForKey:lPath];
	if (lEntry)
	{
		rv = lEntry.data;
		[lEntry incrementUseCount];	// So cannot be removed from the cache
	}
	return rv;
}

//--------------------------------------------------------------------------------------------------
bool FileCache::TryToFreeMemory(uint64_t aNumRequestedBytes)
{
	// This is a nasty one. For now, don't make it too complicated. Just look for file data that is
	// larger than aNumRequestedBytes, and of which the use count is 0. If there are multiple
	// candidates, pick the smallest.
	// This simplistic approach misses any combination of entries that might provide a (better) fit.
	// By the way: we know the generic semaphore is locked, so no worries there.
	bool rv = false;
	
	Par2FileCacheEntry *lBestCandidate = nil;
	NSString *lBestCandidateKey = nil;

	for (NSString *lKey in [(NSMutableDictionary *)mDataMap allKeys])
	{
		Par2FileCacheEntry *lEntry = [(NSMutableDictionary *)mDataMap objectForKey:lKey];
		
		if (lEntry.useCount == 0 && [lEntry.data length] > aNumRequestedBytes)
		{
			// This is a candidate. Is it better than the one we already have?
			if (lBestCandidate == nil || 
				[lEntry.data length] < [lBestCandidate.data length])
			{
				// Yes, it is a better candidate
				lBestCandidate = lEntry;
				lBestCandidateKey = lKey;
			}
		}
		else if (lEntry.useCount == 0 && [lEntry.data length] == aNumRequestedBytes)
		{
			// Wow, this is the perfect candidate; won't find any better than this!
			lBestCandidateKey = lKey;
			break; // From the for loop
		}
	}
	if (lBestCandidateKey)
	{
		// Remove this one from the dictionary. Memory is probably not reclaimed immediately, that
		// is why the caller must account for some slop.
		[(NSMutableDictionary *)mDataMap removeObjectForKey:lBestCandidateKey];
		rv = true;
	}
	return rv;
}

//--------------------------------------------------------------------------------------------------
void FileCache::DoneWithFileData(const char *aFilePath)
{
	NSString *lPath = [NSString stringWithCString:aFilePath encoding:NSUTF8StringEncoding];
	Par2FileCacheEntry *lEntry = [(NSMutableDictionary *)mDataMap objectForKey:lPath];
	if (lEntry)
	{
		[lEntry decrementUseCount];
	}
	else
	{
		NSLog (@"FileCache::DoneWithFileData called for non-existing key");
	}
}

//--------------------------------------------------------------------------------------------------
uint64_t FileCache::AverageDataSize()
{
	// Caller must have locked generic semaphore
	int lNumElements = [(NSMutableDictionary *)mDataMap count];
	if (lNumElements == 0)
		return 0;

	__block uint64_t lTotalSize = 0;
	[[(NSMutableDictionary *)mDataMap allValues] 
	 enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop)
	 {
		 lTotalSize += [((Par2FileCacheEntry *)obj).data length];
	 }];
	return lTotalSize / lNumElements;	// Precision is sifficient
}
