/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "StringRef.h"

#include "Pool.hpp"
#include "ThreadLocalPool.h"
#include "FatalException.hpp"

using namespace voltdb;
using namespace std;

// Indicator that the string's size is not being tracked, so it must be part of a temp data pool.
// Any value that is not supported as a valid ThreadLocalPool allocation size would work for this.
static const size_t OUT_OF_RANGE_INDICATING_TEMP_POOL_ALLOCATION = 0x7fffffffffffffff;

// This needs to be >= the VoltType.MAX_VALUE_LENGTH defined in java, currently 1048576.
// The rationale for making it any larger would be to allow calculating wider "temp" values
// for use in situations where they are not being stored as column values.
static const size_t POOLED_MAX_VALUE_LENGTH = 1048576;


static size_t StringRefGetAllocationSizeForString(size_t length) {
    if (length > POOLED_MAX_VALUE_LENGTH - (POOLED_MAX_VALUE_LENGTH >> 2)) {
        if (length > POOLED_MAX_VALUE_LENGTH + sizeof(int32_t) + sizeof(void*)) {
            return 0;
        }
        return POOLED_MAX_VALUE_LENGTH + sizeof(int32_t) + sizeof(void*);
    }
    // Quickly calculate the lowest power of 2 >= length.
    // This public domain algorithm came from:
    // http://www-graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    size_t next_power_of_2 = length-1; // This back down only effects even powers of 2.
    // Copy highest bit downwards into all lower bits.
    next_power_of_2 |= (next_power_of_2 >> 1);
    next_power_of_2 |= (next_power_of_2 >> 2);
    next_power_of_2 |= (next_power_of_2 >> 4);
    next_power_of_2 |= (next_power_of_2 >> 8);
    next_power_of_2 |= (next_power_of_2 >> 16);
    // Carry through all the set bits to get the next power of 2.
    ++next_power_of_2;

    // To bridge the gaps between powers of 2,
    // first try to fit an allocation that is only 3/4 the next power of 2.
    // This gives a quick approximation of the nearest whole or half power (square root) of 2.
    size_t three_fourths_the_power = next_power_of_2 - (next_power_of_2 >> 2);
    if (length <= three_fourths_the_power) {
       return three_fourths_the_power;
    }
    return next_power_of_2;
}

size_t
StringRef::computeStringMemoryUsed(size_t length)
{
    // CompactingPool will allocate a chunk of this size for storage.
    // This size is the actual length plus the 4-byte length storage
    // plus the backpointer to the StringRef
    size_t alloc_size = StringRefGetAllocationSizeForString(sizeof(StringRef*) + length);
    //cout << "Object length: " << length << endl;
    //cout << "StringRef* size: " << sizeof(StringRef*) << endl;
    //cout << "Pool allocation size: " << alloc_size << endl;
    // One of these will be allocated in the thread local pool for the string
    alloc_size += sizeof(StringRef);
    //cout << "StringRef size: " << sizeof(StringRef) << endl;
    //cout << "Total allocation size: " << alloc_size << endl;
    return alloc_size;
}

StringRef*
StringRef::create(size_t size, Pool* dataPool)
{
    StringRef* retval;
    if (dataPool != NULL) {
        retval = new (dataPool->allocate(sizeof(StringRef)))
                StringRef(OUT_OF_RANGE_INDICATING_TEMP_POOL_ALLOCATION,
                          reinterpret_cast<char*>(dataPool->allocate(sizeof(StringRef*) + size)));
    }
    else {
        size_t allocatedSize = StringRefGetAllocationSizeForString(sizeof(StringRef*) + size);
        if (allocatedSize == 0) {
            throwFatalException("Attempted to allocate an object larger than the 1 meg limit. "
                    "Requested size was %ld", static_cast<int64_t>(size));
        }
        assert(StringRefGetAllocationSizeForString(allocatedSize) == allocatedSize);
#ifdef MEMCHECK
        // With MEMCHECK enabled, the rounded m_size is ONLY used to compatibly trigger the
        // exception above and to check for StringRef corruption during destroy.
        // The actual C++ allocation uses the originally requested size without rounding up.
        retval = new StringRef(allocatedSize, new char[size]);
#else
        retval = new (ThreadLocalPool::allocateObject(sizeof(StringRef)))
                StringRef(allocatedSize, ThreadLocalPool::allocateString(allocatedSize));
#endif
    }
    StringRef** backptr = reinterpret_cast<StringRef**>(retval->m_stringPtr);
    *backptr = retval;
    return retval;
}

void
StringRef::destroy(StringRef* sref)
{
    if (sref->m_size == OUT_OF_RANGE_INDICATING_TEMP_POOL_ALLOCATION) {
        return;
    }
    // Check for corruption of the StringRef's allocated size field.
    assert(sref->m_size != 0);
    assert(StringRefGetAllocationSizeForString(sref->m_size) == sref->m_size);
#ifdef MEMCHECK
    delete [] sref->m_stringPtr;
    delete sref;
#else
    if (ThreadLocalPool::freeString(sref->m_size, sref->m_stringPtr)) {
        // The location sref->m_stringPtr has been re-filled by compaction.
        // Use the back-pointer from the "string" to the StringRef object in the moved
        // data to update that StringRef with its new string location.
        StringRef* back_ptr = *reinterpret_cast<StringRef**>(sref->m_stringPtr);
        back_ptr->m_stringPtr = sref->m_stringPtr;
    }
    ThreadLocalPool::freeObject(sizeof(StringRef), sref);
#endif
}
