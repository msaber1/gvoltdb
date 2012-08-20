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

#ifndef INDEXBASEDPERSISTENTTABLE_H
#define INDEXBASEDPERSISTENTTABLE_H

#include <string>
#include <vector>
#include <cassert>
#include "boost/shared_ptr.hpp"
#include "boost/scoped_ptr.hpp"
#include "common/ids.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/TupleStreamWrapper.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/RecoveryContext.h"
#include "common/UndoQuantumReleaseInterest.h"
#include "common/ThreadLocalPool.h"

namespace voltdb {

    class TableColumn;
    class TableIndex;
    class TableIterator;
    class TableFactory;
    class TupleSerializer;
    class SerializeInput;
    class Topend;
    class ReferenceSerializeOutput;
    class ExecutorContext;
    class MaterializedViewMetadata;
    class RecoveryProtoMsg;
    class PersistentTableUndoDeleteAction;

    class IndexBasedPersistentTable : public Table, public UndoQuantumReleaseInterest {
        friend class CopyOnWriteContext;
        friend class CopyOnWriteIterator;
        friend class TableFactory;
        friend class TableTuple;
        friend class TableIndex;
        friend class TableIterator;
        friend class PersistentTableStats;
        friend class PersistentTableUndoDeleteAction;
        friend class ::CopyOnWriteTest_CopyOnWriteIterator;
    private:
        // no default ctor, no copy, no assignment
        IndexBasedPersistentTable();
        IndexBasedPersistentTable(IndexBasedPersistentTable const&);
        IndexBasedPersistentTable operator=(IndexBasedPersistentTable const&);

        // default iterator
        TableIterator m_iter;

    public:
        virtual ~IndexBasedPersistentTable();

        void notifyQuantumRelease() {
            // no compaction
        }

        // Return a table iterator by reference
        TableIterator& iterator() {
            m_iter.reset(m_data.begin());
            return m_iter;
        }

        TableIterator* makeIterator() {
            return new TableIterator(this, m_data.begin());
        }

        // ------------------------------------------------------------------
        // OPERATIONS
        // ------------------------------------------------------------------
        void deleteAllTuples(bool freeAllocatedStrings);
        bool insertTuple(TableTuple &source);

        /*
         * Inserts a Tuple without performing an allocation for the
         * uninlined strings.
         */
        void insertTupleForUndo(char *tuple);

        /*
         * Note that inside update tuple the order of sourceTuple and
         * targetTuple is swapped when making calls on the indexes. This
         * is just an inconsistency in the argument ordering.
         */
        bool updateTuple(TableTuple &sourceTuple, TableTuple &targetTuple,
                         bool updatesIndexes);

        /*
         * Identical to regular updateTuple except no memory management
         * for unlined columns is performed because that will be handled
         * by the UndoAction.
         */
        void updateTupleForUndo(TableTuple &sourceTuple, TableTuple &targetTuple,
                                bool revertIndexes);

        /*
         * Delete a tuple by looking it up via table scan or a primary key
         * index lookup.
         */
        bool deleteTuple(TableTuple &tuple, bool freeAllocatedStrings);
        void deleteTupleForUndo(voltdb::TableTuple &tupleCopy);

        /*
         * Lookup the address of the tuple that is identical to the specified tuple.
         * Does a primary key lookup or table scan if necessary.
         */
        voltdb::TableTuple lookupTuple(TableTuple tuple);

        // ------------------------------------------------------------------
        // INDEXES
        // ------------------------------------------------------------------
        virtual int indexCount() const { return m_indexCount; }
        virtual int uniqueIndexCount() const { return m_uniqueIndexCount; }
        virtual std::vector<TableIndex*> allIndexes() const;
        virtual TableIndex *index(std::string name);
        virtual TableIndex *primaryKeyIndex() { return m_pkeyIndex; }
        virtual const TableIndex *primaryKeyIndex() const { return m_pkeyIndex; }

        // ------------------------------------------------------------------
        // UTILITY
        // ------------------------------------------------------------------
        std::string tableType() const;
        virtual std::string debug();

        int partitionColumn() { return m_partitionColumn; }
        /** inlined here because it can't be inlined in base Table, as it
         *  uses Tuple.copy.
         */
        TableTuple& getTempTupleInlined(TableTuple &source);

        /** Add a view to this table */
        void addMaterializedView(MaterializedViewMetadata *view);

        /**
         * Switch the table to copy on write mode. Returns true if the table was already in copy on write mode.
         */
        bool activateCopyOnWrite(TupleSerializer *serializer, int32_t partitionId);

        /**
         * Create a recovery stream for this table. Returns true if the table already has an active recovery stream
         */
        bool activateRecoveryStream(int32_t tableId);

        /**
         * Serialize the next message in the stream of recovery messages. Returns true if there are
         * more messages and false otherwise.
         */
        void nextRecoveryMessage(ReferenceSerializeOutput *out);

        /**
         * Process the updates from a recovery message
         */
        void processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool);

        /**
         * Attempt to serialize more tuples from the table to the provided
         * output stream.  Returns true if there are more tuples and false
         * if there are no more tuples waiting to be serialized.
         */
        bool serializeMore(ReferenceSerializeOutput *out);

        /**
         * Create a tree index on the primary key and then iterate it and hash
         * the tuple data.
         */
        size_t hashCode();

        size_t getBlocksNotPendingSnapshotCount() {
            return m_blocksNotPendingSnapshot.size();
        }

        void doIdleCompaction();
        void printBucketInfo();

        void increaseStringMemCount(size_t bytes)
        {
            m_nonInlinedMemorySize += bytes;
        }
        void decreaseStringMemCount(size_t bytes)
        {
            m_nonInlinedMemorySize -= bytes;
        }

    protected:

        // ------------------------------------------------------------------
        // FROM PIMPL
        // ------------------------------------------------------------------
        void insertIntoAllIndexes(TableTuple *tuple);
        void deleteFromAllIndexes(TableTuple *tuple);
        void updateFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);
        void updateWithSameKeyFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);

        bool tryInsertOnAllIndexes(TableTuple *tuple);
        bool checkUpdateOnUniqueIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);

        bool checkNulls(TableTuple &tuple) const;

        PersistentTable(ExecutorContext *ctx, bool exportEnabled);
        void onSetColumns();

        void notifyBlockWasCompactedAway(TBPtr block);
        void swapTuples(TableTuple sourceTuple, TableTuple destinationTuple);

        /**
         * Normally this will return the tuple storage to the free list.
         * In the memcheck build it will return the storage to the heap.
         */
        void deleteTupleStorage(TableTuple &tuple, TBPtr block = TBPtr(NULL));

        /*
         * Implemented by persistent table and called by Table::loadTuplesFrom
         * to do additional processing for views and Export
         */
        virtual void processLoadedTuple(TableTuple &tuple);

        // pointer to current transaction id and other "global" state.
        // abstract this out of VoltDBEngine to avoid creating dependendencies
        // between the engine and the storage layers - which complicate test.
        ExecutorContext *m_executorContext;

        // CONSTRAINTS
        TableIndex** m_uniqueIndexes;
        int m_uniqueIndexCount;
        bool* m_allowNulls;

        // INDEXES
        TableIndex** m_indexes;
        int m_indexCount;
        TableIndex *m_pkeyIndex;

        // partition key
        int m_partitionColumn;

        // list of materialized views that are sourced from this table
        std::vector<MaterializedViewMetadata *> m_views;

        // STATS
        voltdb::PersistentTableStats stats_;
        voltdb::TableStats* getTableStats();

        // is Export enabled
        bool m_exportEnabled;

        // Snapshot stuff
        boost::scoped_ptr<CopyOnWriteContext> m_COWContext;

        //Recovery stuff
        boost::scoped_ptr<RecoveryContext> m_recoveryContext;

    private:
        // pointers to chunks of data. Specific to table impl. Don't leak this type.
        TBMap m_data;
        int m_failedCompactionCount;
    };

    inline TableTuple& IndexBasedPersistentTable::getTempTupleInlined(TableTuple &source) {
        assert (m_tempTuple.m_data);
        m_tempTuple.copy(source);
        return m_tempTuple;
    }
}

#endif // INDEXBASEDPERSISTENTTABLE_H
