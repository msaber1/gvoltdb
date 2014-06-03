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

#include <cassert>
#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

#include "updateexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/types.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/updatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/ConstraintFailureException.h"

using namespace std;
using namespace voltdb;

void UpdateExecutor::p_initMore()
{
    VOLT_TRACE("init Update Executor");

    UpdatePlanNode* node = dynamic_cast<UpdatePlanNode*>(m_abstractNode);
    assert(node);
    assert(m_input_tables.size() == 1);

    // target table should be persistenttable
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(getTargetTable());
    assert(targetTable);

    AbstractPlanNode *child = node->getChildren()[0];
    assert(child);
    ProjectionPlanNode *proj_node = dynamic_cast<ProjectionPlanNode*>(child);
    if (proj_node == NULL) {
        proj_node = dynamic_cast<ProjectionPlanNode*>(child->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
        assert(proj_node);
    }

    const vector<string> output_column_names = proj_node->getOutputColumnNames();
    const vector<string> &targettable_column_names = targetTable->getColumnNames();

    /*
     * The first output column is the tuple address expression and it isn't part of our output so we skip
     * it when generating the map from input columns to the target table columns.
     */
    for (int ii = 1; ii < output_column_names.size(); ii++) {
        for (int jj=0; jj < targettable_column_names.size(); ++jj) {
            if (targettable_column_names[jj].compare(output_column_names[ii]) == 0) {
                m_inputTargetMap.push_back(pair<int,int>(ii, jj));
                break;
            }
        }
    }

    assert(m_inputTargetMap.size() == (output_column_names.size() - 1));
    m_inputTargetMapSize = (int)m_inputTargetMap.size();

    // for target table related info.
    m_partitionColumn = targetTable->partitionColumn();
    m_partitionColumnIsString = false;
    if (m_partitionColumn != -1) {
        if (targetTable->schema()->columnType(m_partitionColumn) == VALUE_TYPE_VARCHAR) {
            m_partitionColumnIsString = true;
        }
    }
}

bool UpdateExecutor::p_execute()
{
    TempTable* input_table = getTempInputTable(); //input table should be temptable
    assert(input_table);

    // target table should be persistenttable
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(getTargetTable());
    assert(targetTable);
    TableTuple targetTuple(targetTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", input_table->debug().c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", targetTable->debug().c_str());

    // determine which indices are updated by this executor
    // iterate through all target table indices and see if they contain
    // columns mutated by this executor
    //TODO: optimize this out of the execute code path either by completing it in the planner
    // and serializing/deserializing a list of index names or at least doing this analysis
    // resolving to a member list of index name strings in p_initMore.
    std::vector<TableIndex*> indexesToUpdate;
    const std::vector<TableIndex*>& allIndexes = targetTable->allIndexes();
    BOOST_FOREACH(TableIndex *index, allIndexes) {
        bool indexKeyUpdated = false;
        BOOST_FOREACH(int colIndex, index->getColumnIndices()) {
            std::pair<int, int> updateColInfo; // needs to be here because of macro failure
            BOOST_FOREACH(updateColInfo, m_inputTargetMap) {
                if (updateColInfo.second == colIndex) {
                    indexKeyUpdated = true;
                    break;
                }
            }
            if (indexKeyUpdated) {
                break;
            }
        }
        if (indexKeyUpdated) {
            indexesToUpdate.push_back(index);
        }
    }

    TableTuple input_tuple(input_table->schema());
    TableIterator input_iterator = input_table->iterator();
    while (input_iterator.next(input_tuple)) {
        //
        // OPTIMIZATION: Single-Sited Query Plans
        // If our beloved UpdatePlanNode is apart of a single-site query plan,
        // then the first column in the input table will be the address of a
        // tuple on the target table that we will want to update. This saves us
        // the trouble of having to do an index lookup
        //
        void *target_address = input_tuple.getNValue(0).castAsAddress();
        targetTuple.move(target_address);

        // Loop through INPUT_COL_IDX->TARGET_COL_IDX mapping and only update
        // the values that we need to. The key thing to note here is that we
        // grab a temp tuple that is a copy of the target tuple (i.e., the tuple
        // we want to update). This insures that if the input tuple is somehow
        // bringing garbage with it, we're only going to copy what we really
        // need to into the target tuple.
        //
        TableTuple &tempTuple = targetTable->getTempTupleInlined(targetTuple);
        for (int map_ctr = 0; map_ctr < m_inputTargetMapSize; map_ctr++) {
            tempTuple.setNValue(m_inputTargetMap[map_ctr].second,
                                input_tuple.getNValue(m_inputTargetMap[map_ctr].first));
        }

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {
            // check for partition problems
            // get the value for the partition column
            NValue value = tempTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                throw ConstraintFailureException(
                         dynamic_cast<PersistentTable*>(targetTable),
                         tempTuple,
                         "An update to a partitioning column triggered a partitioning error. "
                         "Updating a partitioning column is not supported. Try delete followed by insert.");
            }
        }

        if (!targetTable->updateTupleWithSpecificIndexes(targetTuple, tempTuple,
                                                           indexesToUpdate)) {
            VOLT_INFO("Failed to update tuple from table '%s'",
                      targetTable->name().c_str());
            return false;
        }
    }

    VOLT_TRACE("TARGET TABLE - AFTER: %s\n", targetTable->debug().c_str());
    // TODO lets output result table here, not in result executor. same thing in
    // delete/insert

    // add to the planfragments count of modified tuples
    int64_t modifiedTuples = input_table->tempTableTupleCount();

    setModifiedTuples(modifiedTuples);
    return true;
}
