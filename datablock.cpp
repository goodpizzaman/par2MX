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

// Open the file associated with the data block if is not already open.
// willUseEntireFile means we expect to use all data in the file
bool DataBlock::Open(bool tryToCacheData)
{
  if (diskfile == 0)
    return false;

  if (diskfile->IsOpen())
    return true;

  return diskfile->Open(tryToCacheData);
}

// Read some data at a specified position within a data block
// into a buffer in memory

bool DataBlock::ReadData(u64    position, // Position within the block
                         size_t size,     // Size of the memory buffer
                         void  *buffer)   // Pointer to memory buffer
{
  assert(diskfile != 0);

  // Check to see if the position from which data is to be read
  // is within the bounds of the data block
  if (length > position) 
  {
    // Compute the file offset and how much data to physically read from disk
    u64    fileoffset = offset + position;
    size_t want       = (size_t)min((u64)size, length - position);

    // Read the data from the file into the buffer
    if (!diskfile->Read(fileoffset, buffer, want))
      return false;

    // If the read extends beyond the end of the data block,
    // then the rest of the buffer is zeroed.
    if (want < size)
    {
      memset(&((u8*)buffer)[want], 0, size-want);
    }
  }
  else
  {
    // Zero the whole buffer
    memset(buffer, 0, size);
  }

  return true;
}

// Write some data at a specified position within a datablock
// from memory to disk

bool DataBlock::WriteData(u64         position, // Position within the block
                          size_t      size,     // Size of the memory buffer
                          const void *buffer,   // Pointer to memory buffer
                          size_t     &wrote)    // Amount actually written
{
  assert(diskfile != 0);

  wrote = 0;

  // Check to see if the position from which data is to be written
  // is within the bounds of the data block
  if (length > position)
  {
    // Compute the file offset and how much data to physically write to disk
    u64    fileoffset = offset + position;
    size_t have       = (size_t)min((u64)size, length - position);

    // Write the data from the buffer to disk
    if (!diskfile->Write(fileoffset, buffer, have))
      return false;

    wrote = have;
  }

  return true;
}
