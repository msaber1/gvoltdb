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

#ifndef STORAGETESTSUPPORT_H_
#define STORAGETESTSUPPORT_H_

#include "common/Topend.h"

#include "common/executorcontext.hpp"
#include "common/StreamBlock.h"
#include "common/tabletuple.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include <queue>
#include <vector>

using namespace voltdb;

/// A Top End API implementation that exposes its state for easy verification.
/// Can also be used as a generic do-little Top End implementation for tests that don't care.
class AccessibleTopEnd : public voltdb::Topend {
public:
    AccessibleTopEnd() : receivedDRBuffer(false), receivedExportBuffer(false) { }

    int loadNextDependency(int32_t dependencyId, Pool *pool, Table* destination) { return 0; }

    int64_t fragmentProgressUpdate(int32_t batchIndex, std::string planNodeName,
            std::string targetTableName, int64_t targetTableSize, int64_t tuplesFound,
            int64_t currMemoryInBytes, int64_t peakMemoryInBytes)
    {
        return 1000000000; // larger means less likely/frequent callbacks to ignore
    }

    std::string planForFragmentId(int64_t fragmentId) { return ""; }

    void crashVoltDB(const voltdb::FatalException& e) { }

    int64_t getQueuedExportBytes(int32_t partitionId, std::string signature)
    {
        int64_t bytes = 0;
        for (int ii = 0; ii < blocks.size(); ii++) {
            bytes += blocks[ii]->rawLength();
        }
        return bytes;
    }

    void pushExportBuffer(int64_t generation, int32_t partitionId, std::string signature,
        StreamBlock *block, bool sync, bool endOfStream)
    {
        if (sync) {
            return;
        }
        partitionIds.push(partitionId);
        signatures.push(signature);
        blocks.push_back(boost::shared_ptr<StreamBlock>(new StreamBlock(block)));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
        receivedExportBuffer = true;
    }

    void pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block)
    {
        receivedDRBuffer = true;
        partitionIds.push(partitionId);
        blocks.push_back(boost::shared_ptr<StreamBlock>(new StreamBlock(block)));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
    }

    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) {}

    std::queue<int32_t> partitionIds;
    std::queue<std::string> signatures;
    std::deque<boost::shared_ptr<StreamBlock> > blocks;
    std::vector<boost::shared_array<char> > data;
    bool receivedDRBuffer;
    bool receivedExportBuffer;

};


// This can be overridden by #defining DEFAULT_COLUMN_COUNT to a different integer constant value.
#ifndef DEFAULT_COLUMN_COUNT
const int DEFAULT_COLUMN_COUNT = 5;
#endif

static const int32_t storageTestSizeOfInt = NValue::getTupleStorageSize(VALUE_TYPE_INTEGER);

struct StorageTestEnvironment  {
    StorageTestEnvironment(DRTupleStream* drStream = NULL,
            TupleSchema* defaultSchema = generateIntegerColumnSchema(DEFAULT_COLUMN_COUNT))
       : m_undoQuantum(new (m_pool) UndoQuantum(0, &m_pool))
       , m_defaultSchema(defaultSchema)
       , m_context(0, 0, m_undoQuantum, &m_topEnd, &m_pool, NULL, true, "", 0, drStream)
    {
        srand(0);
        if (m_defaultSchema) {
            m_defaultTupleStorage.init(m_defaultSchema, &m_pool);
            m_defaultTupleStorage.allocateActiveTuple();
        }
    }

    ~StorageTestEnvironment()
    {
        if (m_defaultSchema) {
            TupleSchema::freeTupleSchema(m_defaultSchema);
        }
        m_undoQuantum->release();
    }

    static TupleSchema* generateIntegerColumnSchema(const int columnCount)
    {
        std::vector<ValueType> columnTypes(columnCount, VALUE_TYPE_INTEGER);
        std::vector<int32_t> columnLengths(columnCount, storageTestSizeOfInt);
        std::vector<bool> columnAllowNull(columnCount, false);
        return TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);
    }

    void nextQuantum(int i, int64_t tokenOffset)
    {
        // Takes advantage of "grey box test" friend privileges on UndoQuantum.
        m_undoQuantum->release();
        m_undoQuantum = new (m_pool) UndoQuantum(i + tokenOffset, &m_pool);
        m_context.setupForPlanFragments(m_undoQuantum, i, i, i - 1, 0);
    }

    TableTuple& defaultTuple() { return (TableTuple&)m_defaultTupleStorage; }

    TableTuple& randomlyFillDefaultTuple()
    {
        TableTuple& tuple = defaultTuple();
        // fill a tuple
        for (int col = 0; col < DEFAULT_COLUMN_COUNT; col++) {
            int value = rand();
            tuple.setNValue(col, ValueFactory::getIntegerValue(value));
        }
        return tuple;
    }

    AccessibleTopEnd m_topEnd;
    Pool m_pool;
private:
    UndoQuantum *m_undoQuantum;
    TupleSchema* m_defaultSchema;
    PoolBackedTupleStorage m_defaultTupleStorage;
public:
    ExecutorContext m_context;
};

#endif /* STORAGETESTSUPPORT_H_ */
