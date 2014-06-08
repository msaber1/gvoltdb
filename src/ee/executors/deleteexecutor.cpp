/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "deleteexecutor.h"

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "plannodes/deletenode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

#include <cassert>

namespace voltdb {

void DeleteExecutor::p_initMore()
{
    VOLT_TRACE("init Delete Executor");
    DeletePlanNode* node = dynamic_cast<DeletePlanNode*>(m_abstractNode);
    assert(node);
    if (node->getTruncate()) {
        assert(m_input_tables.size() == 0);
        return;
    }
    assert(m_input_tables.size() == 1);
}

bool DeleteExecutor::p_execute()
{
    // target table should be persistenttable
    // update target table reference from table delegate
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(getTargetTable());
    assert(targetTable);

    int64_t modified_tuples = 0;

    if (m_input_tables.size() == 0) {
        VOLT_TRACE("truncating table %s...", targetTable->name().c_str());
        // count the truncated tuples as deleted
        modified_tuples = targetTable->visibleTupleCount();

        VOLT_TRACE("Delete all rows from table : %s with %d active, %d visible, %d allocated",
                   targetTable->name().c_str(),
                   (int)targetTable->activeTupleCount(),
                   (int)targetTable->visibleTupleCount(),
                   (int)targetTable->allocatedTupleCount());

        // actually delete all the tuples: undo by table not by each tuple.
        targetTable->truncateTable(m_engine);
    }
    else {
        assert(m_input_tables.size() == 1);
        TempTable* inputTable = m_input_tables[0].getTempTable(); //input table should be temptable
        assert(inputTable);
        TableTuple inputTuple(inputTable->schema());
        assert(inputTable);

        TableTuple targetTuple(targetTable->schema());

        TableIterator inputIterator = inputTable->iterator();
        while (inputIterator.next(inputTuple)) {
            //
            // OPTIMIZATION: Single-Sited Query Plans
            // The first column in the input table will be the address of a
            // tuple on the target table that we will want to blow away.
            // This saves the trouble of having to do an index lookup.
            // The downside is a temptable with a small entry for each deleted row.
            // Maybe some day a delete executor might be able to operate as an inline
            // operation on a scan. Care would have to be taken about not invalidating
            // the scanning target table iterator.
            void *targetAddress = inputTuple.getNValue(0).castAsAddress();
            targetTuple.move(targetAddress);

            // Delete from target table
            if (!targetTable->deleteTuple(targetTuple, true)) {
                VOLT_ERROR("Failed to delete tuple from table '%s'",
                           targetTable->name().c_str());
                return false;
            }
        }
        modified_tuples = inputTable->tempTableTupleCount();
        VOLT_TRACE("Deleted %d rows from table : %s with %d active, %d visible, %d allocated",
                   (int)modified_tuples,
                   targetTable->name().c_str(),
                   (int)targetTable->activeTupleCount(),
                   (int)targetTable->visibleTupleCount(),
                   (int)targetTable->allocatedTupleCount());

    }

    setModifiedTuples(modified_tuples);
    return true;
}

}
