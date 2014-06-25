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

#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "plannodes/deletenode.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

namespace voltdb {

bool DeleteExecutor::p_init(AbstractPlanNode *abstract_node,
                            TempTableLimits* limits)
{
    VOLT_TRACE("init Delete Executor");

    DeletePlanNode* node = dynamic_cast<DeletePlanNode*>(m_abstractNode);
    assert(node);
    assert(dynamic_cast<PersistentTable*>(node->getTargetTable()));

    setDMLCountOutputTable(limits);

    m_truncate = node->getTruncate();
    if (m_truncate) {
        assert(node->getInputTables().size() == 0);
        return true;
    }

    assert(node->getInputTables().size() == 1);
    m_inputTable = node->getTempInputTable(); //input table should be temptable
    assert(m_inputTable);
    return true;
}

bool DeleteExecutor::p_execute(const NValueArray &params) {
    // target table should be persistenttable
    // update target table reference from table delegate
    DeletePlanNode* node = static_cast<DeletePlanNode*>(m_abstractNode);
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(node->getTargetTable());
    assert(targetTable);
    TableTuple targetTuple(targetTable->schema());

    int64_t modified_tuples = 0;

    if (m_truncate) {
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
    else
    {
        assert(m_inputTable);
        TableTuple inputTuple = (m_inputTable->schema());
        assert(targetTuple.sizeInValues() == targetTable->columnCount());
        TableIterator inputIterator = m_inputTable->iterator();
        while (inputIterator.next(inputTuple)) {
            //
            // OPTIMIZATION: Single-Sited Query Plans
            // If our beloved DeletePlanNode is apart of a single-site query plan,
            // then the first column in the input table will be the address of a
            // tuple on the target table that we will want to blow away. This saves
            // us the trouble of having to do an index lookup
            //
            void *targetAddress = inputTuple.getNValue(0).castAsAddress();
            targetTuple.move(targetAddress);

            // Delete from target table
            if (!targetTable->deleteTuple(targetTuple, true)) {
                VOLT_ERROR("Failed to delete tuple from table '%s'",
                           targetTable->name().c_str());
                return false;
            }
        }
        modified_tuples = m_inputTable->tempTableTupleCount();
        VOLT_TRACE("Deleted %d rows from table : %s with %d active, %d visible, %d allocated",
                   (int)modified_tuples,
                   targetTable->name().c_str(),
                   (int)targetTable->activeTupleCount(),
                   (int)targetTable->visibleTupleCount(),
                   (int)targetTable->allocatedTupleCount());

    }

    TableTuple& count_tuple = m_tmpOutputTable->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
    // try to put the tuple into the output table
    m_tmpOutputTable->insertTempTuple(count_tuple);
    m_engine->m_tuplesModified += modified_tuples;
    return true;
}

} // namespace voltdb
