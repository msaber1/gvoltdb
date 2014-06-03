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

#include "insertexecutor.h"
#include "common/debuglog.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "plannodes/insertnode.h"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"
#include "storage/ConstraintFailureException.h"

#include <vector>

using namespace std;
using namespace voltdb;

void InsertExecutor::p_initMore()
{
    VOLT_TRACE("init Insert Executor");

    InsertPlanNode* node = dynamic_cast<InsertPlanNode*>(m_abstractNode);
    assert(node);
    assert(getTargetTable());
    assert(m_input_tables.size() == 1);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(getTargetTable());
    m_partitionColumn = -1;
    m_partitionColumnIsString = false;
    m_isStreamed = (persistentTarget == NULL);
    m_multiPartition = node->isMultiPartition();
    if ( ! persistentTarget) {
        return;
    }

    m_partitionColumn = persistentTarget->partitionColumn();
    if (m_partitionColumn != -1 &&
        persistentTarget->schema()->columnType(m_partitionColumn) == VALUE_TYPE_VARCHAR) {
        m_partitionColumnIsString = true;
    }
}

bool InsertExecutor::p_execute()
{
    InsertPlanNode* node = dynamic_cast<InsertPlanNode*>(m_abstractNode);
    assert(node);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    // Update target table reference from table delegate
    Table* targetTable = getTargetTable();
    assert(targetTable);
    assert((targetTable == dynamic_cast<PersistentTable*>(targetTable)) ||
            (targetTable == dynamic_cast<StreamedTable*>(targetTable)));

    Table* input_table = m_input_tables[0].getTable();
    assert(input_table);
    TableTuple inputTuple(input_table->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", input_table->debug().c_str());

    // count the number of successful inserts
    int modifiedTuples = 0;

    // Loop through our inputTable and insert any tuple that we find into our targetTable.
    TableIterator iterator = input_table->iterator();
    while (iterator.next(inputTuple)) {
        VOLT_TRACE("Inserting tuple '%s' into target table '%s' with table schema: %s",
                   inputTuple.debug(input_table->name()).c_str(), targetTable->name().c_str(),
                   targetTable->schema()->debug().c_str());

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {

            // get the value for the partition column
            NValue value = inputTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                if (!m_multiPartition) {
                    throw ConstraintFailureException(
                            dynamic_cast<PersistentTable*>(targetTable),
                            inputTuple,
                            "Mispartitioned tuple in single-partition insert statement.");
                }

                // don't insert
                continue;
            }
        }

        // for multi partition export tables,
        //  only insert them into one place (the partition with hash(0))
        if (m_isStreamed && m_multiPartition) {
            bool isLocal = m_engine->isLocalSite(ValueFactory::getBigIntValue(0));
            if (!isLocal) continue;
        }

        // try to put the tuple into the target table
        if (!targetTable->insertTuple(inputTuple)) {
            VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                       " target table '%s'",
                       input_table->name().c_str(),
                       targetTable->name().c_str());
            return false;
        }

        // successfully inserted
        modifiedTuples++;
    }

    setModifiedTuples(modifiedTuples);
    VOLT_DEBUG("Finished inserting tuple");
    return true;
}
