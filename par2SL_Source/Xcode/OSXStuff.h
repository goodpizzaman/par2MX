/*
 *  OSXStuff.h
 *  PBpar2
 *
 *  Created by Gerard Putter on 05-12-09.
 *  Copyright 2009 Gerard Putter. All rights reserved.
 *
 *  $Id: OSXStuff.h,v 1.2 2009/12/25 12:29:47 gputter Exp $
 *
 */

// Some functions required for proper operation in Mac OS X. They are called from C++, so this 
// is a way to shield the original code from Cocoa and Objectve C stuff.

#include <stdint.h>

namespace OSXStuff
{
	struct MemoryStats
	{
		uint64_t		memFree;			// All amounts in bytes
		uint64_t		memActive;
		uint64_t		memInactive;
		uint64_t		memWired;
	};
	void *SetupAutoreleasePool();		// Returns object pointer as void *
	void ReleaseAutoreleasePool(void *aPool);
	void analyzeMemory(MemoryStats &aMemStats);
}
