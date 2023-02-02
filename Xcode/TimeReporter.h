/*
 *  TimeReporter.h
 *  PBpar2
 *
 *  Created by Gerard Putter on 04-12-09.
 *  Copyright 2009 Gerard Putter. All rights reserved.
 *
 *  $Id*
 *
 */

// Contains functions to easily report elapsed time on a per-thread basis. The elapsed times are
// written to stdout, tigether wih a clarifying text, specified by the application.
//
// This is how to use it:
// Call MarkTime to mark the start of the time period to measure. Optionally specify a text to be
// printed on stdout. The start time is stored per thread.
// When it is time to print the elapsed time since the last MarkTime, call PrintTime. It has
// a boolean argument to also reset the time.

namespace TimeReporter
{
	void MarkTime(const char *aText);						// aText can be NULL
	void MarkTime();										// Short for MarkTime(NULL);
	void PrintTime(const char *aText, bool aResetTheMark);	// aText can be NULL, but shouldn't
}
