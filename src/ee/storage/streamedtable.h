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

#ifndef STREAMEDTABLE_H
#define STREAMEDTABLE_H

#include "common/ids.h"
#include "table.h"
#include "storage/StreamedTableStats.h"
#include "storage/TableStats.h"
#include "storage/TupleBlock.h"

namespace voltdb {

class TupleStreamWrapper;

/**
 * A streamed table does not store data. It may not be read. It may
 * not be updated. Only new appended writes are permitted. All writes
 * are passed through a TupleStreamWrapper to Export. The table exists
 * only to support Export.
 */
class StreamedTable : public Table {
    friend class StreamedTableStats;
  public:
    StreamedTable();
    static StreamedTable* createForTest(size_t allocation);

    virtual ~StreamedTable();

    // undo interface particular to streamed table.
    void undo(size_t mark);

  private:
    /// Append an inserted or deleted tuple to the stream.
    void appendTuple(TableTuple &tuple, bool forDelete);

    // virtual Table functions
    // Return a table iterator by const reference suitable for initializing an independent iterator.
    virtual const TableIterator& iterator();

    virtual void deleteAllTuples(bool freeAllocatedStrings);
    virtual bool insertTuple(TableTuple &source);
    virtual bool updateTupleWithSpecificIndexes(TableTuple &targetTupleToUpdate,
                                                const TableTuple &sourceTupleWithNewValues,
                                                const std::vector<TableIndex*> &indexesToUpdate);
    virtual bool deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings);
    virtual void loadTuplesFrom(SerializeInput &serialize_in, Pool *stringPool = NULL);
    virtual void flushOldTuples(int64_t timeInMillis);
    virtual void setSignatureAndGeneration(std::string signature, int64_t generation);

    virtual std::string tableType() const {
        return "StreamedTable";
    }

    //Override and say how many bytes are in Java and C++
    int64_t allocatedTupleMemory() const;


    /**
     * Get the current offset in bytes of the export stream for this Table
     * since startup.
     */
    void getExportStreamPositions(int64_t &seqNo, size_t &streamBytesUsed);

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed);

    virtual bool isExport() {
        return true;
    }

    /*
     * For an export table return the sequence number
     */
    virtual int64_t activeTupleCount() const {
        return m_sequenceNo;
    }
  protected:
    // Stats
    voltdb::StreamedTableStats stats_;
    voltdb::TableStats *getTableStats();

    // Just say 0
    size_t allocatedBlockCount() const;

    TBPtr allocateNextBlock();
    void nextFreeTuple(TableTuple *tuple);

  private:
    TupleStreamWrapper *m_wrapper;
    int64_t m_sequenceNo;
};

}
#endif
