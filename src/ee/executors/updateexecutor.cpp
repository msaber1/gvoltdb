/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#include "updateexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/types.h"
#include "common/tabletuple.h"
#include "plannodes/updatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/persistenttable.h"

#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

using namespace std;
using namespace voltdb;

bool UpdateExecutor::p_init()
{
    VOLT_TRACE("init Update Executor");

    UpdatePlanNode* node = dynamic_cast<UpdatePlanNode*>(m_abstractNode);
    assert(node);
    assert(hasExactlyOneInputTable());
    assert(m_inputTable);
    assert(m_targetTable);

    m_inputTargetSize = m_inputTable->columnCount();
    assert(m_inputTargetSize >= 2); // An address and at least 1 value.
    m_inputTuple = TableTuple(m_inputTable->schema());
    m_targetTuple = TableTuple(m_targetTable->schema());

    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(m_targetTable);
    assert(persistentTarget);
    if (persistentTarget) {
        m_partitionColumn = persistentTarget->partitionColumn();
    } else {
        // Probably trying to update a streamed table
        // -- not allowed unless/until logged stream updates are implemented.
        //TODO: really should throw something descriptive
        return false;
    }

    m_updatedColumns = node->getUpdatedColumns();

    // determine which indices are updated by this executor
    // iterate through all target table indices and see if they contain
    // columns mutated by this executor
    BOOST_FOREACH(TableIndex *index, m_targetTable->allIndexes()) {
        bool indexKeyUpdated = false;
        BOOST_FOREACH(int colIndex, index->getColumnIndices()) {
            BOOST_FOREACH(int updatedCol, m_updatedColumns) {
                if (updatedCol == colIndex) {
                    indexKeyUpdated = true;
                    break;
                }
            }
            if (indexKeyUpdated) break;
        }
        if (indexKeyUpdated) {
            m_indexesToUpdate.push_back(index);
        }
    }
    return true;
}

bool UpdateExecutor::p_execute() {
    assert(m_inputTable);
    assert(m_targetTable);
    assert(m_inputTargetSize > 0);

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", m_targetTable->debug().c_str());

    assert(m_inputTuple.sizeInValues() == m_inputTable->columnCount());
    assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount());
    TableIterator input_iterator = m_inputTable->iterator();
    while (input_iterator.next(m_inputTuple)) {
        //
        // OPTIMIZATION: Single-Sited Query Plans
        // If our beloved UpdatePlanNode is part of a single-site query plan,
        // then the first column in the input table will be the address of a
        // tuple on the target table that we will want to update. This saves us
        // the trouble of having to require a primary key to do an index lookup.
        // There are also multi-site plans where this would continue to work,
        // at least in theory. The worst case scenario is where a change to the
        // partition key would require a delete on one partition and an insert on another.
        // At the EE/plan fragment level, that hardly looks like an update at all, so
        // the current approach may have a long future.
        void *target_address = m_inputTuple.getSelfAddressColumn();
        m_targetTuple.move(target_address);

        // Loop through input columns and update the values that we need to.
        // The key thing to note here is that we grab a temp tuple
        // that is a copy of the target tuple (i.e., the tuple we want to update).
        TableTuple &tempTuple = static_cast<PersistentTable*>(m_targetTable)->getTempTupleInlined(m_targetTuple);

        // The first input column is the tuple address expression and it isn't represented
        // by an updated column, so skip it in the input.
        for (int ctr = 1; ctr < m_inputTargetSize; ctr++) {
            tempTuple.setNValue(m_updatedColumns[ctr-1], // don't skip an updated column
                                m_inputTuple.getNValue(ctr));
        }

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {
            // check for partition problems
            // get the value for the partition column
            NValue value = tempTuple.getNValue(m_partitionColumn);
            // if it doesn't map to this site
            if ( ! valueHashesToTheLocalPartition(value)) {
                VOLT_ERROR("Mispartitioned tuple in single-partition plan for"
                           " table '%s'", m_targetTable->name().c_str());
                return false;
            }
        }

        if (!m_targetTable->updateTupleWithSpecificIndexes(m_targetTuple, tempTuple,
                                                           m_indexesToUpdate)) {
            VOLT_INFO("Failed to update tuple from table '%s'",
                      m_targetTable->name().c_str());
            return false;
        }
    }

    return storeModifiedTupleCount(m_inputTable->activeTupleCount());
}
