/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef BIGMEMORYALLOCATOR_H_
#define BIGMEMORYALLOCATOR_H_

#include "boost/shared_ptr.hpp"

#include <iostream>
#include <set>
#include <pthread.h>


namespace voltdb {

    class BigMemoryAllocator {
    public:
        BigMemoryAllocator();
        ~BigMemoryAllocator();

        static void *alloc(int64_t blocksize);
        static void free(void *ptr);

    private:
        static void init(int64_t blocksize);

        static int64_t m_blocksize;
        static void *m_base;
        static int64_t m_allocated;
        static std::set<int64_t> m_freeList;

        static pthread_mutex_t m_mutex;

        static BigMemoryAllocator m_raii;
    };

}

#endif /* defined(BIGMEMORYALLOCATOR_H_) */
