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
#include "OSXStuff.h"

dispatch_semaphore_t coutSema = NULL;

void banner(void)
{
  string version = PACKAGE " version " VERSION;

  cout << version << ", Copyright (C) 2003 Peter Brian Clements." << endl
       << "Adapted for use with MacPar deLuxe by Gerard Putter." << endl
       << endl
       << "This program is compatible with Mac OS X Snow Leopard or later." << endl
       << "It uses Grand Central Dispatch to optimize the speed and processor load." << endl
       << endl
       << "This is free software, and you are welcome to redistribute it and/or modify" << endl
       << "it under the terms of the GNU General Public License as published by the" << endl
       << "Free Software Foundation; either version 2 of the License, or (at your" << endl
       << "option) any later version. See COPYING for details." << endl
       << endl;
}

int main(int argc, char *argv[])
{
  void *lPool = OSXStuff::SetupAutoreleasePool();   // Avoid using Cocoa types here

#ifdef DEBUG
  // FOR DEBUG ONLY: ALLOW DEBUGGER TO BE ATTACHED
  int lInitialSleep = 0;
  while (lInitialSleep)
  {
    sleep(1);
  }
#endif
  
  coutSema = dispatch_semaphore_create(1);  // Effectively like a mutex
  // Parse the command line
  CommandLine *commandline = new CommandLine;

  Result result = eInvalidCommandLineArguments;
  
  if (!commandline->Parse(argc, argv))
  {
    banner();
    CommandLine::usage();
  }
  else
  {
    if (commandline->GetNoiseLevel() > CommandLine::nlSilent)
      banner();

    // Which operation was selected
    switch (commandline->GetOperation())
    {
    case CommandLine::opCreate:
      {
        // Create recovery data

        Par2Creator *creator = new Par2Creator;
        result = creator->Process(*commandline);
        delete creator;
      }
      break;
    case CommandLine::opVerify:
      {
        // Verify damaged files
        switch (commandline->GetVersion())
        {
        case CommandLine::verPar1:
          {
            Par1Repairer *repairer = new Par1Repairer;
            result = repairer->Process(*commandline, false);
            delete repairer;
          }
          break;
        case CommandLine::verPar2:
          {
            Par2Repairer *repairer = new Par2Repairer;
            result = repairer->Process(*commandline, false);
            delete repairer;
          }
          break;
        case CommandLine::opNone:
          break;
        }
      }
      break;
    case CommandLine::opRepair:
      {
        // Repair damaged files
        switch (commandline->GetVersion())
        {
        case CommandLine::verPar1:
          {
            Par1Repairer *repairer = new Par1Repairer;
            result = repairer->Process(*commandline, true);
            delete repairer;
          }
          break;
        case CommandLine::verPar2:
          {
            Par2Repairer *repairer = new Par2Repairer;
            result = repairer->Process(*commandline, true);
            delete repairer;
          }
          break;
        case CommandLine::opNone:
          break;
        }
      }
      break;
    case CommandLine::opNone:
      break;
    }
  }

  delete commandline;

  dispatch_release(coutSema);

  OSXStuff::ReleaseAutoreleasePool(lPool);   // Avoid using Cocoa types here
  
  return result;
}
