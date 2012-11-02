/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "common/executorcontext.hpp"

#include "common/debuglog.h"
#include "common/Topend.h"

#include <pthread.h>

using namespace std;

namespace voltdb {

static pthread_key_t static_key;
static pthread_once_t static_keyOnce = PTHREAD_ONCE_INIT;

static void createThreadLocalKey()
{
    (void) pthread_key_create(&static_key, NULL);
}

void
ExecutorContext::installAsThreadLocalSingleton()
{
    (void)pthread_once(&static_keyOnce, createThreadLocalKey);
    bindToThread();
}

void ExecutorContext::bindToThread()
{
    // There can be only one (per thread).
    assert(pthread_getspecific(static_key) == NULL);
    pthread_setspecific(static_key, this);
    VOLT_DEBUG("Installing EC(%ld)", (long)this);
}

ExecutorContext::~ExecutorContext()
{
    // currently does not own any of its pointers

    // There can be only one (per thread).
    assert(pthread_getspecific(static_key) == this);
    // ... or none, now that the one is going away.
    VOLT_DEBUG("De-installing EC(%ld)", (long)this);
    pthread_setspecific(static_key, NULL);
}

ExecutorContext* ExecutorContext::getExecutorContext()
{
    (void) pthread_once(&static_keyOnce, createThreadLocalKey);
    return static_cast<ExecutorContext*>(pthread_getspecific(static_key));
}

void ExecutorContext::pushExportBuffer(int64_t exportGeneration, const std::string &signature, StreamBlock *block)
{
    ExecutorContext* singleton = getExecutorContext();
    singleton->m_topEnd->pushExportBuffer(exportGeneration, singleton->m_partitionId, signature, block, false, false);
}

void ExecutorContext::syncExportBuffer(int64_t exportGeneration, const std::string &signature)
{
    ExecutorContext* singleton = getExecutorContext();
    singleton->m_topEnd->pushExportBuffer(exportGeneration, singleton->m_partitionId, signature, NULL, true, false);
}

void ExecutorContext::endExportBuffer(int64_t exportGeneration, const std::string &signature)
{
    ExecutorContext* singleton = getExecutorContext();
    singleton->m_topEnd->pushExportBuffer(exportGeneration, singleton->m_partitionId, signature, NULL, false, true);
}

int64_t ExecutorContext::getQueuedExportBytes(const std::string &signature)
{
    ExecutorContext* singleton = getExecutorContext();
    return singleton->m_topEnd->getQueuedExportBytes( singleton->m_partitionId, signature);
}

void ExecutorContext::fallbackToEEAllocatedBuffer(char *buffer, size_t length)
{
    ExecutorContext* singleton = getExecutorContext();
    return singleton->m_topEnd->fallbackToEEAllocatedBuffer(buffer, length);
}

/**
 * Retrieve a logger by ID from the LogManager associated with this thread.
 * @parameter loggerId ID of the logger to retrieve
 */
const Logger* ExecutorContext::logger(LoggerId loggerId)
{
    ExecutorContext* singleton = getExecutorContext();
    return singleton->m_topEnd->getLogManager().getLogger(loggerId);
}


}

