/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef HSTOREPERSISTENTTABLE_H
#define HSTOREPERSISTENTTABLE_H

#include "storage/table.h"
#include "common/UndoQuantumReleaseInterest.h"

#include <cassert>
#include "boost/scoped_ptr.hpp"
#include "common/ids.h"
#include "common/tabletuple.h"
#include "storage/PersistentTableStats.h"
#include "storage/tableiterator.h"
#include "common/ThreadLocalPool.h"

class CompactionTest_BasicCompaction;
class CompactionTest_CompactionWithCopyOnWrite;

namespace voltdb {

class CopyOnWriteContext;
class MaterializedViewMetadata;
class RecoveryContext;
class RecoveryProtoMsg;
class TupleSerializer;

/**
 * Represents a non-temporary table which permanently resides in
 * storage and also registered to Catalog (see other documents for
 * details of Catalog). PersistentTable has several additional
 * features to Table.  It has indexes, constraints to check NULL and
 * uniqueness as well as undo logs to revert changes.
 *
 * PersistentTable can have one or more Indexes, one of which must be
 * Primary Key Index. Primary Key Index is same as other Indexes except
 * that it's used for deletion and updates. Our Execution Engine collects
 * Primary Key values of deleted/updated tuples and uses it for specifying
 * tuples, assuming every PersistentTable has a Primary Key index.
 *
 * Currently, constraints are not-null constraint and unique
 * constraint.  Not-null constraint is
 * checked against insertion and update. Unique constraint is also
 * just a flag of TableIndex and checked against insertion and
 * update. There's no rule constraint or foreign key constraint so far
 * because our focus is performance and simplicity.
 *
 * To revert changes after execution, PersistentTable holds UndoLog.
 * PersistentTable does eager update which immediately changes the
 * value in data and adds an entry to UndoLog. We chose eager update
 * policy because we expect reverting rarely occurs.
 */

class PersistentTable : public Table, public UndoQuantumReleaseInterest {
    friend class CopyOnWriteContext;
    friend class CopyOnWriteIterator;
    friend class TableFactory;
    friend class PersistentTableUndoDeleteAction;
    friend class ::CompactionTest_BasicCompaction;
    friend class ::CompactionTest_CompactionWithCopyOnWrite;
  private:
    // no default ctor, no copy, no assignment
    PersistentTable();
    PersistentTable(PersistentTable const&);
    PersistentTable operator=(PersistentTable const&);

    // default iterator
    TableIterator m_iter;

  public:
    virtual ~PersistentTable();

    void notifyQuantumRelease() {
        if (compactionPredicate()) {
            doForcedCompaction();
        }
    }

    // Return a table iterator by const reference
    const TableIterator& iterator() {
        m_iter.reset(m_data.begin());
        return m_iter;
    }

    // ------------------------------------------------------------------
    // OPERATIONS
    // ------------------------------------------------------------------
    void deleteAllTuples(bool freeAllocatedStrings);
    virtual void insertTuple(TableTuple &source);

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
    virtual void updateTupleWithSpecificIndexes(TableTuple &targetTupleToUpdate,
                                                const TableTuple &sourceTupleWithNewValues,
                                                const std::vector<TableIndex*> &indexesToUpdate);

    /*
     * Identical to regular updateTuple except no memory management
     * for unlined columns is performed because that will be handled
     * by the UndoAction.
     */
    void updateTupleForUndo(TableTuple &targetTupleToUpdate,
                            char* sourceTupleDataWithNewValues,
                            bool revertIndexes);

    /*
     * Delete a tuple by looking it up via table scan or a primary key
     * index lookup.
     */
    virtual void deleteTuple(TableTuple &tuple, bool freeAllocatedStrings);
    void deleteTupleForUndo(const char* tupleData);

    /*
     * Lookup the address of the tuple that is identical to the specified tuple.
     * Does a primary key lookup or table scan if necessary.
     */
    TableTuple lookupTuple(const TableTuple &tuple)
    {
        const char* tupleData = tuple.address();
        return lookupTuple(tupleData);
    }
    TableTuple lookupTuple(const char* tupleData);

    // ------------------------------------------------------------------
    // UTILITY
    // ------------------------------------------------------------------
    std::string tableType() const;
    virtual std::string debug();

    int partitionColumn() { return m_partitionColumn; }

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

  private:

    size_t allocatedBlockCount() const {
        return m_data.size();
    }

    void snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock) {
        if (nextBlock != NULL) {
            assert(m_blocksPendingSnapshot.find(nextBlock) != m_blocksPendingSnapshot.end());
            m_blocksPendingSnapshot.erase(nextBlock);
            nextBlock->swapToBucket(TBBucketPtr());
        }
        if (finishedBlock != NULL && !finishedBlock->isEmpty()) {
            m_blocksNotPendingSnapshot.insert(finishedBlock);
            int bucketIndex = finishedBlock->calculateBucketIndex();
            if (bucketIndex != -1) {
                finishedBlock->swapToBucket(m_blocksNotPendingSnapshotLoad[bucketIndex]);
            }
        }
    }

    void nextFreeTuple(TableTuple *tuple);
    bool doCompactionWithinSubset(TBBucketMap *bucketMap);
    void doForcedCompaction();

    // ------------------------------------------------------------------
    // FROM PIMPL
    // ------------------------------------------------------------------
    void insertIntoAllIndexes(TableTuple *tuple);
    void deleteFromAllIndexes(TableTuple *tuple);
    bool tryInsertOnAllIndexes(TableTuple *tuple);
    bool checkUpdateOnUniqueIndexes(TableTuple &targetTupleToUpdate,
                                    const TableTuple &sourceTupleWithNewValues,
                                    const std::vector<TableIndex*> &indexesToUpdate);

    bool checkNulls(const TableTuple &tuple) const;

    PersistentTable(int partitionColumn);
    void onSetColumns();

    void notifyBlockWasCompactedAway(TBPtr block);
    void swapTuples(TableTuple &sourceTupleWithNewValues, TableTuple &destinationTuple);

    bool deletionMustDeferToCOWContext(const TableTuple& tuple) const;

    /**
     * Normally this will return the tuple storage to the free list.
     * In the memcheck build it will return the storage to the heap.
     */
    void deleteTupleStorage(TableTuple &tuple);
    void deleteTupleStorage(TableTuple &tuple, TBPtr block);

    // helper for deleteTupleStorage overload where the block is not specified.
    TBPtr findBlock(char *tuple);

    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(TableTuple &tuple);

    TBPtr allocateNextBlock();

    // CONSTRAINTS
    std::vector<bool> m_allowNulls;

    // partition key
    const int m_partitionColumn;

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



    // STORAGE TRACKING

    // Map from load to the blocks with level of load
    TBBucketMap m_blocksNotPendingSnapshotLoad;
    TBBucketMap m_blocksPendingSnapshotLoad;

    // Map containing blocks that aren't pending snapshot
    boost::unordered_set<TBPtr> m_blocksNotPendingSnapshot;

    // Map containing blocks that are pending snapshot
    boost::unordered_set<TBPtr> m_blocksPendingSnapshot;

    // Set of blocks with non-empty free lists or available tuples
    // that have never been allocated
    stx::btree_set<TBPtr > m_blocksWithSpace;

  private:
    // pointers to chunks of data. Specific to table impl. Don't leak this type.
    TBMap m_data;
    int m_failedCompactionCount;
};

inline void PersistentTable::deleteTupleStorage(TableTuple &tuple)
{
    TBPtr block = findBlock(tuple.address());
    deleteTupleStorage(tuple, block);
}

inline void PersistentTable::deleteTupleStorage(TableTuple &tuple, TBPtr block) {
    tuple.setActiveFalse(); // does NOT free strings

    // add to the free list
    m_tupleCount--;
    //m_tuplesPendingDelete--;

    bool transitioningToBlockWithSpace = !block->hasFreeTuples();

    int retval = block->freeTuple(tuple.address());
    if (retval != -1) {
        //Check if if the block is currently pending snapshot
        if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
            //std::cout << "Swapping block " << static_cast<void*>(block.get()) << " to bucket " << retval << std::endl;
            block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval]);
        //Check if the block goes into the pending snapshot set of buckets
        } else if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            block->swapToBucket(m_blocksPendingSnapshotLoad[retval]);
        } else {
            //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
            //do nothing, once the block is finished by the iterator, the iterator will return it
        }
    }

    if (block->isEmpty()) {
        m_data.erase(block->address());
        m_blocksWithSpace.erase(block);
        m_blocksNotPendingSnapshot.erase(block);
        assert(m_blocksPendingSnapshot.find(block) == m_blocksPendingSnapshot.end());
        //Eliminates circular reference
        block->swapToBucket(TBBucketPtr());
    } else if (transitioningToBlockWithSpace) {
        m_blocksWithSpace.insert(block);
    }
}

inline TBPtr PersistentTable::findBlock(char *tuple) {
    TBMapI i = m_data.lower_bound(tuple);
    if (i == m_data.end() && m_data.empty()) {
        throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
    }
    if (i == m_data.end()) {
        i--;
        if (i.key() + m_tableAllocationSize < tuple) {
            throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
        }
    } else {
        if (i.key() != tuple) {
            i--;
            if (i.key() + m_tableAllocationSize < tuple) {
                throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
            }
        }
    }
    return i.data();
}

inline TBPtr PersistentTable::allocateNextBlock() {
    TBPtr block(new (ThreadLocalPool::getExact(sizeof(TupleBlock))->malloc()) TupleBlock(this, m_blocksNotPendingSnapshotLoad[0]));
    m_data.insert( block->address(), block);
    m_blocksNotPendingSnapshot.insert(block);
    return block;
}


}



#endif
