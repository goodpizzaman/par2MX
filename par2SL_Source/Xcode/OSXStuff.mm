/*
 *  OSXStuff.mm
 *  PBpar2
 *
 *  Created by Gerard Putter on 05-12-09.
 *  Copyright 2009 Gerard Putter. All rights reserved.
 *
 *  $Id: OSXStuff.mm,v 1.2 2009/12/25 12:29:47 gputter Exp $
 *
 */

#include "OSXStuff.h"
#import <AppKit/AppKit.h>
#import <mach/host_info.h>
#import <mach/mach_host.h>

//--------------------------------------------------------------------------------------------------
void *OSXStuff::SetupAutoreleasePool()
{
	return [NSAutoreleasePool new];
}

//--------------------------------------------------------------------------------------------------
void OSXStuff::ReleaseAutoreleasePool(void *aPool)
{
	[((NSAutoreleasePool *)aPool) release];
}

//--------------------------------------------------------------------------------------------------
void OSXStuff::analyzeMemory(MemoryStats &aMemStats)
{
	vm_statistics_data_t	lPageInfo;
	vm_size_t				lPageSize;
	mach_msg_type_number_t	lCount;
	kern_return_t			lKernRet;
	
	memset(&aMemStats, 0, sizeof aMemStats);

	lPageSize = 0;
	lKernRet = host_page_size (mach_host_self(), &lPageSize);
	
	lCount = HOST_VM_INFO_COUNT;
	lKernRet = host_statistics (mach_host_self(), HOST_VM_INFO, (host_info_t)&lPageInfo, &lCount);
	if (lKernRet == KERN_SUCCESS)
	{
		// Calc bytes. Take into account that (in Mac OS X Snow Leopard) the numbers in 
		// vm_statistics are 32 bits. We know that the typical page size is 4096 (12 bits), 
		// so the effective size we can express is limited to 44 bits. This is a lot (16 TB),
		// but not nearly what the full 64 bits would allow.
		
		aMemStats.memFree = lPageInfo.free_count;			aMemStats.memFree *= lPageSize;
		aMemStats.memActive = lPageInfo.active_count;		aMemStats.memActive *= lPageSize;
		aMemStats.memInactive = lPageInfo.inactive_count;	aMemStats.memInactive *= lPageSize;
		aMemStats.memWired = lPageInfo.wire_count;			aMemStats.memWired *= lPageSize;
	}
}
