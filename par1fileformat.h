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

#ifndef __PAR1FILEFORMAT_H__
#define __PAR1FILEFORMAT_H__

#define PACKED __attribute__ ((packed))

struct PAR1MAGIC {u8 magic[8];}PACKED;

struct PAR1FILEHEADER
{
  PAR1MAGIC   magic;
  leu32       fileversion PACKED;
  leu32       programversion PACKED;
  MD5Hash     controlhash;
  MD5Hash     sethash;
  leu64       volumenumber PACKED;
  leu64       numberoffiles PACKED;
  leu64       filelistoffset PACKED;
  leu64       filelistsize PACKED;
  leu64       dataoffset PACKED;
  leu64       datasize PACKED;
};

struct PAR1FILEENTRY
{
  leu64       entrysize PACKED;
  leu64       status PACKED;
  leu64       filesize PACKED;
  MD5Hash     hashfull;
  MD5Hash     hash16k;
  leu16       name[] PACKED;
};

enum FILEENTRYSTATUS
{
  INPARITYVOLUME = 1,
  CHECKED = 2,
};

// Operators for comparing the MAGIC values

inline bool operator == (const PAR1MAGIC &left, const PAR1MAGIC &right)
{
  return (0==memcmp(&left, &right, sizeof(left)));
}

inline bool operator != (const PAR1MAGIC &left, const PAR1MAGIC &right)
{
  return !operator==(left, right);
}

extern PAR1MAGIC par1_magic;

#endif //__PAR1FILEFORMAT_H__
