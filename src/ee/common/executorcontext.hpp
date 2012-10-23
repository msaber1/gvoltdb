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

#ifndef _EXECUTORCONTEXT_HPP_
#define _EXECUTORCONTEXT_HPP_

#include "common/common.h"
#include "common/Topend.h"
#include "common/valuevector.h"

namespace voltdb {
class StreamBlock;
class Topend;
class UndoQuantum;

/*
 * EE site global data required by executors at runtime.
 *
 * This data is factored into common to avoid creating dependencies on
 * execution/VoltDBEngine throughout the storage and executor code.
 * This facilitates easier test case writing and breaks circular
 * dependencies between ee component directories.
 *
 * A better implementation that meets these goals is always welcome if
 * you see a preferable refactoring.
 */
class ExecutorContext {
  public:
    // It is the thread-hopping VoltDBEngine's responsibility to re-establish the EC for each new thread it runs on.
    void bindToThread();

    static CatalogId partitionId() { return getExecutorContext()->m_partitionId; }

    static int64_t siteId() { return getExecutorContext()->m_siteId; }

    // not always known at initial construction
    void setEpoch(int64_t epoch) { m_epoch = epoch; }

    // data available via tick()
    void setupForTick(int64_t lastCommittedTxnId) { m_lastCommittedTxnId = lastCommittedTxnId; }

    // data available via quiesce()
    void setupForQuiesce(int64_t lastCommittedTxnId) { m_lastCommittedTxnId = lastCommittedTxnId; }

    // helper to configure the context for a new jni call
    static void setupTxnIdsForPlanFragments(int64_t txnId, int64_t lastCommittedTxnId)
    {
        ExecutorContext* singleton = getExecutorContext();
        singleton->m_txnId = txnId;
        singleton->m_lastCommittedTxnId = lastCommittedTxnId;
    }

    void setupForPlanFragments(UndoQuantum *undoQuantum) { m_undoQuantum = undoQuantum; }

    static UndoQuantum *currentUndoQuantum() { return getExecutorContext()->m_undoQuantum; }

    /** Current or most recently executed transaction id. */
    static int64_t currentTxnId() { return getExecutorContext()->m_txnId; }

    /** Current or most recently executed transaction id. */
    static int64_t currentTxnTimestamp()
    {
        ExecutorContext* singleton = getExecutorContext();
        return (singleton->m_txnId >> 23) + singleton->m_epoch;
    }

    /** Last committed transaction known to this EE */
    static int64_t lastCommittedTxnId() { return getExecutorContext()->m_lastCommittedTxnId; }

    static ExecutorContext* getExecutorContext();

    static Pool* getTempStringPool()
    {
        ExecutorContext* singleton = getExecutorContext();
        assert(singleton != NULL);
        assert(singleton->m_tempStringPool != NULL);
        return singleton->m_tempStringPool;
    }

    static void setupTxnIdsForPlanFragments(int64_t txnId, int64_t lastCommittedTxnId,
                                            int paramcnt, const NValueArray &params)
    {
        ExecutorContext* singleton = getExecutorContext();
        singleton->m_txnId = txnId;
        singleton->m_lastCommittedTxnId = lastCommittedTxnId;
        singleton->m_params = &params;
        singleton->m_paramCnt = paramcnt;
    }

    static const NValueArray& getParams() { return *(getExecutorContext()->m_params); }

    static int getUsedParamcnt() { return getExecutorContext()->m_paramCnt; }

    void enableExportFeature() { m_exportFeatureEnabled = true; }

    static bool exportFeatureIsEnabled() { return getExecutorContext()->m_exportFeatureEnabled; }

    static void pushExportBuffer(int64_t exportGeneration, const std::string &signature, StreamBlock *block)
    {
        ExecutorContext* singleton = getExecutorContext();
        singleton->m_topEnd->pushExportBuffer(exportGeneration, singleton->m_partitionId, signature, block, false, false);
    }

    static void syncExportBuffer(int64_t exportGeneration, const std::string &signature)
    {
        ExecutorContext* singleton = getExecutorContext();
        singleton->m_topEnd->pushExportBuffer(exportGeneration, singleton->m_partitionId, signature, NULL, true, false);
    }

    static void endExportBuffer(int64_t exportGeneration, const std::string &signature)
    {
        ExecutorContext* singleton = getExecutorContext();
        singleton->m_topEnd->pushExportBuffer(exportGeneration, singleton->m_partitionId, signature, NULL, false, true);
    }

    static int64_t getQueuedExportBytes(const std::string &signature)
    {
        ExecutorContext* singleton = getExecutorContext();
        return singleton->m_topEnd->getQueuedExportBytes( singleton->m_partitionId, signature);
    }

    static void fallbackToEEAllocatedBuffer(char *buffer, size_t length)
    {
        ExecutorContext* singleton = getExecutorContext();
        return singleton->m_topEnd->fallbackToEEAllocatedBuffer(buffer, length);
    }

    /**
     * Retrieve a logger by ID from the LogManager associated with this thread.
     * @parameter loggerId ID of the logger to retrieve
     */
    inline static const Logger* logger(LoggerId loggerId) {
        ExecutorContext* singleton = getExecutorContext();
        return singleton->m_topEnd->getLogManager().getLogger(loggerId);
    }
    // SQL is historically the "go to" logger for the EE -- don't know why --paul
    inline static const Logger* sqlLogger() { return logger(LOGGERID_SQL); }


    ExecutorContext(int64_t siteId,
                    CatalogId partitionId,
                    UndoQuantum *undoQuantum,
                    Topend* topend,
                    Pool* tempStringPool,
                    bool exportEnabled,
                    std::string hostname,
                    CatalogId hostId) :
        m_topEnd(topend), m_tempStringPool(tempStringPool),
        m_undoQuantum(undoQuantum), m_txnId(0), m_lastCommittedTxnId(0),
        m_params(NULL),
        m_paramCnt(0),
        m_siteId(siteId), m_partitionId(partitionId),
        m_hostname(hostname), m_hostId(hostId),
        m_epoch(0), m_exportFeatureEnabled(exportEnabled) // reset later
    {
        ExecutorContext::installAsThreadLocalSingleton();
    }

    ~ExecutorContext();

  private:
    void installAsThreadLocalSingleton();

    Topend * const m_topEnd;
    Pool * const m_tempStringPool;
    UndoQuantum *m_undoQuantum;
    int64_t m_txnId;
    int64_t m_lastCommittedTxnId;
    const NValueArray* m_params;
    int m_paramCnt;

  public:
    int64_t const m_siteId;
    CatalogId const m_partitionId;
    const std::string m_hostname;
    CatalogId const m_hostId;

  private:
    /** local epoch for voltdb, sometime around 2008, pulled from catalog */
    int64_t m_epoch;
    bool m_exportFeatureEnabled;


};

}

#endif
