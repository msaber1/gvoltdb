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

#include "BigMemoryAllocator.h"

#include <cassert>

using namespace voltdb;

int64_t BigMemoryAllocator::m_blocksize = 0;
void *BigMemoryAllocator::m_base = NULL;
int64_t BigMemoryAllocator::m_allocated = 0;
std::set<int64_t> BigMemoryAllocator::m_freeList;

pthread_mutex_t BigMemoryAllocator::m_mutex = PTHREAD_MUTEX_INITIALIZER;

BigMemoryAllocator BigMemoryAllocator::m_raii;

const int64_t BIG_ALLOC_SIZE = (int64_t)4096 * (int64_t)1024 * (int64_t)1024;

BigMemoryAllocator::BigMemoryAllocator() {}

BigMemoryAllocator::~BigMemoryAllocator() {

    if (pthread_mutex_lock(&m_mutex)) {
        printf("Failed to lock mutex in BigMemoryAllocator::~BigMemoryAllocator()\n");
        exit(1);
    }

    if (m_base != NULL) {
        m_blocksize = 0;
        std::free(m_base);
        m_base = NULL;
        m_allocated = 0;
        m_freeList.clear();
    }

    if (pthread_mutex_unlock(&m_mutex)) {
        printf("Failed to unlock mutex in BigMemoryAllocator::~BigMemoryAllocator()\n");
        exit(1);
    }
}

void BigMemoryAllocator::init(int64_t blocksize) {
    m_blocksize = blocksize;
    // allocate a chunk
    m_base = std::malloc(BIG_ALLOC_SIZE);
    assert(m_base != NULL);
    assert(m_allocated == 0);
    assert(m_freeList.empty());
}

void *BigMemoryAllocator::alloc(int64_t blocksize) {
    void *next = NULL;

    if (pthread_mutex_lock(&m_mutex)) {
        printf("Failed to lock mutex in BigMemoryAllocator::alloc()\n");
        exit(1);
    }

    if (m_base == NULL) {
        init(blocksize);
    }
    assert(m_blocksize == blocksize);

    int64_t offset;

    if (m_freeList.empty()) {
        assert((m_allocated * m_blocksize) < BIG_ALLOC_SIZE);
        offset = m_allocated++;
    }
    else {
        std::set<int64_t>::iterator iter = m_freeList.begin();
        offset = *iter;
        m_freeList.erase(iter);
    }

    next = (char*) m_base + (offset * m_blocksize);

    if (pthread_mutex_unlock(&m_mutex)) {
        printf("Failed to unlock mutex in BigMemoryAllocator::alloc()\n");
        exit(1);
    }

    assert(next != NULL);
    return next;
}

void BigMemoryAllocator::free(void *ptr) {
    if (pthread_mutex_lock(&m_mutex)) {
        printf("Failed to lock mutex in BigMemoryAllocator::free()\n");
        exit(1);
    }

    // hack
    if (m_base == NULL) {
        return;
    }

    assert(m_base);
    assert(ptr);
    assert(m_allocated > 0);
    assert(m_blocksize > 0);

    int64_t offset = static_cast<int64_t>((char*)ptr - (char*)m_base);
    if ((offset % m_blocksize) != 0) {
        printf("Offset %lld, blocksize %lld\n", offset, m_blocksize);
        fflush(stdout);
        assert(false);
    }
    offset /= m_blocksize;

    m_freeList.insert(offset);

    /*printf("m_freeList.size() == %d\n", (int)m_freeList.size());

    std::set<int64_t>::const_iterator citer;
    for (citer = m_freeList.begin(); citer != m_freeList.end(); ++citer) {
        printf("%lld ", *citer);
    }
    printf(" BEFORE\n");*/


    while (true) {
        std::set<int64_t>::reverse_iterator iter = m_freeList.rbegin();
        if (iter == m_freeList.rend()) {
            break;
        }
        int64_t iterOffset = *iter;
        if (iterOffset != (m_allocated - 1)) {
            break;
        }
        m_freeList.erase(--iter.base());
        --m_allocated;

        if (m_allocated == 0) {
            assert(m_freeList.size() == 0);
        }
    }

    /*for (citer = m_freeList.begin(); citer != m_freeList.end(); ++citer) {
        printf("%lld ", *citer);
    }
    printf(" AFTER\n");*/

    if (pthread_mutex_unlock(&m_mutex)) {
        printf("Failed to unlock mutex in BigMemoryAllocator::free()\n");
        exit(1);
    }
}


