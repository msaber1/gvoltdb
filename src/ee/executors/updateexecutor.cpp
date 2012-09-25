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

#include <cassert>
#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

#include "common/debuglog.h"
#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/types.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/VoltDBEngine.h"
#include "plannodes/updatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/persistenttable.h"

using namespace std;
using namespace voltdb;

bool UpdateExecutor::p_init()
{
    VOLT_TRACE("init Update Executor");

    m_node = dynamic_cast<UpdatePlanNode*>(m_abstractNode);
    assert(m_node);
    assert(getInputTables().size() == 1);
    assert(m_inputTable);
    assert(m_targetTable);

    AbstractPlanNode *child = m_node->getChildren()[0];
    if (NULL == child) {
        VOLT_ERROR("Attempted to initialize update executor with NULL child");
        return false;
    }

    ProjectionPlanNode *proj_node = NULL;
    PlanNodeType pnt = child->getPlanNodeType();
    if (pnt == PLAN_NODE_TYPE_PROJECTION) {
        proj_node = dynamic_cast<ProjectionPlanNode*>(child);
        assert(NULL != proj_node);
    } else if (pnt == PLAN_NODE_TYPE_SEQSCAN || pnt == PLAN_NODE_TYPE_INDEXSCAN) {
        proj_node = dynamic_cast<ProjectionPlanNode*>(child->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
        assert(NULL != proj_node);
    }

    const vector<string> &targettable_column_names = m_targetTable->getColumnNames();

    /*
     * The first output column is the tuple address expression and it isn't part of our output so we skip
     * it when generating the map from input columns to the target table columns.
     */
    for (int ii = 1; ii < proj_node->getOutputSchema().size(); ii++) {
        string column_name = proj_node->getOutputSchema()[ii]->getColumnName();
        for (int jj=0; jj < targettable_column_names.size(); ++jj) {
            if (targettable_column_names[jj].compare(column_name) == 0) {
                m_inputTargetMap.push_back(pair<int,int>(ii, jj));
                break;
            }
        }
    }

    assert(m_inputTargetMap.size() == (output_column_names.size() - 1));
    m_inputTargetMapSize = (int)m_inputTargetMap.size();
    m_inputTuple = TableTuple(m_inputTable->schema());
    m_targetTuple = TableTuple(m_targetTable->schema());

    m_partitionColumn = m_targetTable->partitionColumn();

    // determine which indices are updated by this executor
    // iterate through all target table indices and see if they contain
    //  tables mutated by this executor
    BOOST_FOREACH(TableIndex *index, m_targetTable->allIndexes()) {
        bool indexKeyUpdated = false;
        BOOST_FOREACH(int colIndex, index->getColumnIndices()) {
            std::pair<int, int> updateColInfo; // needs to be here because of macro failure
            BOOST_FOREACH(updateColInfo, m_inputTargetMap) {
                if (updateColInfo.second == colIndex) {
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

bool UpdateExecutor::p_execute(const NValueArray &params) {
    assert(m_inputTable);
    assert(m_targetTable);

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", m_targetTable->debug().c_str());

    assert(m_inputTuple.sizeInValues() == m_inputTable->columnCount());
    assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount());
    TableIterator input_iterator = m_inputTable->iterator();
    while (input_iterator.next(m_inputTuple)) {
        //
        // OPTIMIZATION: Single-Sited Query Plans
        // If our beloved UpdatePlanNode is apart of a single-site query plan,
        // then the first column in the input table will be the address of a
        // tuple on the target table that we will want to update. This saves us
        // the trouble of having to do an index lookup
        //
        void *target_address = m_inputTuple.getNValue(0).castAsAddress();
        m_targetTuple.move(target_address);

        // Loop through INPUT_COL_IDX->TARGET_COL_IDX mapping and only update
        // the values that we need to. The key thing to note here is that we
        // grab a temp tuple that is a copy of the target tuple (i.e., the tuple
        // we want to update). This insures that if the input tuple is somehow
        // bringing garbage with it, we're only going to copy what we really
        // need to into the target tuple.
        //
        TableTuple &tempTuple = m_targetTable->getTempTupleInlined(m_targetTuple);
        for (int map_ctr = 0; map_ctr < m_inputTargetMapSize; map_ctr++) {
            tempTuple.setNValue(m_inputTargetMap[map_ctr].second,
                                m_inputTuple.getNValue(m_inputTargetMap[map_ctr].first));
        }

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {
            // check for partition problems
            // get the value for the partition column
            NValue value = tempTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
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

    TableTuple& count_tuple = m_outputTable->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(m_inputTable->activeTupleCount()));
    // try to put the tuple into the output table
    if (!m_outputTable->insertTuple(count_tuple)) {
        VOLT_ERROR("Failed to insert tuple count (%ld) into"
                   " output table '%s'",
                   static_cast<long int>(m_inputTable->activeTupleCount()),
                   m_outputTable->name().c_str());
        return false;
    }

    VOLT_TRACE("TARGET TABLE - AFTER: %s\n", m_targetTable->debug().c_str());
    // TODO lets output result table here, not in result executor. same thing in
    // delete/insert

    // add to the planfragments count of modified tuples
    m_engine->m_tuplesModified += m_inputTable->activeTupleCount();

    return true;
}
