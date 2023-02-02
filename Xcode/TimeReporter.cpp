/*
 *  TimeReporter.cpp
 *  PBpar2
 *
 *  Created by Gerard Putter on 04-12-09.
 *  Copyright 2009 Gerard Putter. All rights reserved.
 *
 *  $Id*
 */

#include "TimeReporter.h"
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>
#include <stdio.h>

static bool SetupThreadStuff();

bool lThreadStuffHasBeenSetup = SetupThreadStuff();	// Hope this is executed initially. Yes, it is.
pthread_key_t	gMarkedTimeKey;		// For thread-specific start times

//--------------------------------------------------------------------------------------------------
static void TLSDestructor(void *aData)
{
	// The data is a pointer to a double and we now it is not NULL
	free (aData);
}

//--------------------------------------------------------------------------------------------------
static bool SetupThreadStuff()
{
	int lResult = pthread_key_create(&gMarkedTimeKey, TLSDestructor);
	assert (lResult == 0);

	return lResult == 0;	
}

//--------------------------------------------------------------------------------------------------
static void StoreTime (double aTime)
{
	// We assume the thread local storage key has been set up.
	// If we already have TLS, use it. Otherwise, allocate it.
	double *lValuePtr = (double *)pthread_getspecific(gMarkedTimeKey);
	if (!lValuePtr)
	{
		lValuePtr = (double *) malloc(sizeof (double));
		pthread_setspecific(gMarkedTimeKey, lValuePtr);
	}
	*lValuePtr = aTime;
}

//--------------------------------------------------------------------------------------------------
static double GetStoredTime()
{
	// We assume the thread local storage key has been set up.
	// If we already have TLS, use it. Otherwise, return 0. That should not happen.
	double *lValuePtr = (double *)pthread_getspecific(gMarkedTimeKey);
	return lValuePtr ? *lValuePtr : 0.0;
}

//--------------------------------------------------------------------------------------------------
inline double NowAsDouble ()
{
    struct timeval  lNow;
    const double cMillion = 1000000.0;
    gettimeofday (&lNow, NULL);
	return (double) lNow.tv_sec + (double) lNow.tv_usec / cMillion;
}

//--------------------------------------------------------------------------------------------------
void TimeReporter::MarkTime(const char *aText)
{
	if (!lThreadStuffHasBeenSetup)
	{
		// Allocate stuff right here, in order not to include it in the measured time.
		SetupThreadStuff();
	}
	
	if (aText)
	{
		printf ("%s\n", aText);
	}
	StoreTime (NowAsDouble());
}

//--------------------------------------------------------------------------------------------------
void TimeReporter::MarkTime()
{
	MarkTime(NULL);
}

//--------------------------------------------------------------------------------------------------
void TimeReporter::PrintTime(const char *aText, bool aResetTheMark)
{
	if (!lThreadStuffHasBeenSetup)
	{
		// Actually, this should have been done already. If not, this overhead will be included in
		// the time measured, but the reported time will be meaningless anyway (time since start 0).
		SetupThreadStuff();
	}

	if (!aText)
	{
		aText = "Time elapsed since last Mark";
	}
	pthread_t lThisThread = pthread_self();	// pthread is a pointer
	
	printf("(%llx) %s: %f seconds\n", (unsigned long long) lThisThread, aText, NowAsDouble() - GetStoredTime());
	if (aResetTheMark)
	{
		StoreTime (NowAsDouble());
	}
}
