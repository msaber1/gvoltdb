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

#ifndef STRINGREF_H
#define STRINGREF_H

#include <cstddef>

namespace voltdb
{
    class Pool;

    /// An object to use in lieu of raw char* pointers for strings
    /// which are not inlined into tuple storage.  This provides a
    /// constant value to live in tuple storage while allowing the memory
    /// containing the actual string to be moved around as the result of
    /// compaction.
    class StringRef
    {
    public:
        /// Utility method to compute the amount of memory that will
        /// be used by non-inline storage of a string/varbinary of the
        /// given length.  Includes the size of pooled StringRef object,
        /// backpointer, and excess memory allocated in the compacting
        /// string pool.
        static std::size_t computeStringMemoryUsed(std::size_t length);

        /// Create and return a new StringRef object which points to an
        /// allocated memory block of (at least) the requested size.  The caller
        /// may provide an optional (temporary) Pool from which the memory (and
        /// the memory for the StringRef object itself) will be
        /// allocated, intended for temporary strings.
        /// If no Pool object is provided, the StringRef and the string memory will be
        /// allocated out of the (persistent) ThreadLocalPool
        /// (or via direct C++ allocation in MEMCHECK mode).
        static StringRef* create(std::size_t size, Pool* dataPool);

        /// Destroy the given StringRef object and free any memory
        /// allocated from non-temporary pools to store the object.
        /// sref must have been allocated and returned by a call to
        /// StringRef::create().
        /// It is an optional no-op when sref was created in a temporary Pool.
        static void destroy(StringRef* sref);

        char* get() { return m_stringPtr + sizeof(StringRef*); }
        const char* get() const { return m_stringPtr + sizeof(StringRef*); }

    private:
        StringRef(std::size_t size, char* stringPtr) : m_size(size), m_stringPtr(stringPtr) {}
        /// A no-op -- All cleanup is handled by destroy(StringRef*)
        ~StringRef() {}
        const std::size_t m_size;
        char* m_stringPtr;
    };
}

#endif // STRINGREF_H
