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
#include "common/ThreadLocalPool.h"
#include "common/FatalException.hpp"
#include "common/SQLException.h"
#include "structures/CompactingPool.h"
#include "boost/pool/pool.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/unordered_map.hpp"
#include <pthread.h>
#include <iostream>


namespace voltdb {

struct voltdb_pool_allocator_new_delete
{
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  static char * malloc(const size_type bytes);
  static void free(char * const block);
};


/**
 * Thread local key for storing thread specific memory pools
 */
static pthread_key_t m_key;
static pthread_key_t m_stringKey;
/**
 * Thread local keys for storing integer values for the ThreadLocalPool ref count and amount of memory allocated
 */
static pthread_key_t m_keyRefCount;
static pthread_key_t m_keyAllocated;
static pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

typedef boost::pool<voltdb_pool_allocator_new_delete> PoolType;
typedef boost::shared_ptr<PoolType> PoolTypePtr;
typedef boost::unordered_map< std::size_t, PoolTypePtr > MapType;

typedef boost::unordered_map<std::size_t, boost::shared_ptr<CompactingPool> > CompactingStringStorage;
typedef boost::shared_ptr<CompactingPool> StringPoolPtrType;

static void createThreadLocalKey() {
    (void)pthread_key_create( &m_key, NULL);
    (void)pthread_key_create( &m_stringKey, NULL);
    (void)pthread_key_create( &m_keyAllocated, NULL);
    (void)pthread_key_create( &m_keyRefCount, NULL);
}

ThreadLocalPool::ThreadLocalPool() {
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    std::size_t* pRefCount = static_cast<std::size_t*>(pthread_getspecific(m_keyRefCount));
    if (pRefCount != NULL) {
        ++(*pRefCount);
        return;
    }
    pthread_setspecific(m_keyRefCount, static_cast<const void *>(new std::size_t(1)));
    pthread_setspecific(m_keyAllocated, static_cast<const void *>(new std::size_t(0)));
    pthread_setspecific(m_key, static_cast<const void *>(new MapType()));
    pthread_setspecific(m_stringKey, static_cast<const void*>(new CompactingStringStorage()));
}

ThreadLocalPool::~ThreadLocalPool() {
    std::size_t* pRefCount = static_cast<std::size_t*>(pthread_getspecific(m_keyRefCount));
    assert(pRefCount != NULL);
    if (pRefCount == NULL) {
        return;
    }
    if (--(*pRefCount) > 0) {
        return;
    }
    delete static_cast<MapType*>(pthread_getspecific(m_key));
    pthread_setspecific(m_key, NULL);
    delete static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
    pthread_setspecific(m_stringKey, NULL);
    delete static_cast<std::size_t*>(pthread_getspecific(m_keyAllocated));
    pthread_setspecific(m_keyAllocated, NULL);
    delete pRefCount;
    pthread_setspecific(m_keyRefCount, NULL);
}

inline static CompactingStringStorage* ThreadLocalPoolGetStringPools()
{
    return static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
}

inline static MapType* ThreadLocalPoolGetObjectPools()
{
    return static_cast<MapType*>(pthread_getspecific(m_key));
}

inline static std::size_t* ThreadLocalPoolGetObjectBytesAllocated()
{
    return static_cast<std::size_t*>(pthread_getspecific(m_keyAllocated));
}

/**
 * Allocate from a pool that allocate chunks that are exactly the requested size. Only creates
 * pools up to 1 megabyte + 4 bytes.
 */
void * ThreadLocalPool::allocateObject(std::size_t size)
{
    MapType* pools = ThreadLocalPoolGetObjectPools();
    MapType::iterator iter = pools->find(size);
    PoolType * pool;
    if (iter == pools->end()) {
        pool = new PoolType(size);
        PoolTypePtr poolPtr(pool);
        pools->insert(std::pair<std::size_t, PoolTypePtr> (size, poolPtr));
        return pool->malloc();
    }
    pool = iter->second.get();

    /**
     * The goal of this code is to bypass the pool sizing algorithm used by boost
     * and replace it with something that bounds allocations to a series of 2 meg blocks
     * for small allocations. For large allocations fall back to a strategy of allocating two of
     * these huge things at a time. The goal of this bounding is make the amount of untouched but allocated
     * memory relatively small so that the counting done by the volt allocator accurately represents the effect
     * on RSS.
     */
    if (pool->get_next_size() * pool->get_requested_size() > (1024 * 1024 * 2)) {
        //If the size of objects served by this pool is less than 256 kilobytes
        //go ahead and allocate a 2 meg block of them
        if (pool->get_requested_size() < (1024 * 256)) {
            pool->set_next_size((1024 * 1024 * 2) /  pool->get_requested_size());
        } else {
            //For large objects allocate just two of them
            pool->set_next_size(2);
        }
    }
    return pool->malloc();
}

/**
 * Free from the pool that allocated chunks that are of exactly the given size.
 */
void ThreadLocalPool::freeObject(std::size_t size, const void* object)
{
    MapType* pools = ThreadLocalPoolGetObjectPools();
    MapType::iterator iter = pools->find(size);
    if (iter == pools->end()) {
        throwFatalException("Attempted to deallocate an object that was not pool allocated. "
                "Apparent size was %ld", static_cast<int64_t>(size));
    }
    iter->second->free(const_cast<void*>(object));
}

char * ThreadLocalPool::allocateString(std::size_t alloc_size)
{
    assert(alloc_size > 0);
    CompactingStringStorage* pools = ThreadLocalPoolGetStringPools();
    CompactingPool* pool = NULL;
    CompactingStringStorage::iterator iter = pools->find(alloc_size);
    if (iter != pools->end()) {
        pool = iter->second.get();
    } else {
        // compute num_elements to be closest multiple leading to a 2Meg buffer
        int32_t ssize = static_cast<int32_t>(alloc_size);
        int32_t num_elements = (2 * 1024 * 1024 / ssize) + 1;
        pool = new CompactingPool(ssize, num_elements);
        pools->insert(std::pair<std::size_t, StringPoolPtrType>(alloc_size, StringPoolPtrType(pool)));
    }
    return reinterpret_cast<char*>(pool->malloc());
}

bool ThreadLocalPool::freeString(std::size_t alloc_size, const char* string)
{
    assert(alloc_size > 0);
    CompactingStringStorage* pools = ThreadLocalPoolGetStringPools();
    CompactingStringStorage::iterator iter = pools->find(alloc_size);
    if (iter == pools->end()) {
        throwFatalException("Attempted to deallocate a string that was not pool allocated. "
            "Apparent size was %ld", static_cast<int64_t>(alloc_size));
    }
    return iter->second->free(const_cast<char*>(string));
}


std::size_t ThreadLocalPool::getTotalPoolBytesAllocated() {
    std::size_t bytes_allocated = *ThreadLocalPoolGetObjectBytesAllocated();
    CompactingStringStorage* blockMap = ThreadLocalPoolGetStringPools();
    for (CompactingStringStorage::iterator iter = blockMap->begin(); iter != blockMap->end(); ++iter) {
        bytes_allocated += iter->second->getBytesAllocated();
    }
    return bytes_allocated;
}

char * voltdb_pool_allocator_new_delete::malloc(const size_type bytes) {
    (*ThreadLocalPoolGetObjectBytesAllocated()) += bytes + sizeof(std::size_t);
    //std::cout << "Pooled memory is " << ((*ThreadLocalPoolGetObjectBytesAllocated()) / (1024 * 1024)) << " MB after requested allocation " << (bytes / (1024 * 1024)) <<  std::endl;
    char *retval = new (std::nothrow) char[sizeof(std::size_t) + bytes];
    *reinterpret_cast<std::size_t*>(retval) = sizeof(std::size_t) + bytes;
    return &retval[sizeof(std::size_t)];
}

void voltdb_pool_allocator_new_delete::free(char * const block) {
    (*ThreadLocalPoolGetObjectBytesAllocated()) -= *reinterpret_cast<std::size_t*>(block - sizeof(std::size_t));
    delete [](block - sizeof(std::size_t));
}
}
