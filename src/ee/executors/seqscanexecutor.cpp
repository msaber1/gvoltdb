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

#include "seqscanexecutor.h"

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/ProgressMonitorProxy.h"
#include "executors/projectionexecutor.h"
#include "expressions/abstractexpression.h"
#include "plannodes/seqscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"

#include <iostream>

using namespace voltdb;

bool SeqScanExecutor::p_initMore(TempTableLimits* limits)
{
    VOLT_TRACE("init SeqScan Executor");
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(m_abstractNode);
    assert(node);
    Table* targetTable = NULL;
    if (node->isSubQuery()) {
        assert(node->getChildren().size() == 1);
        targetTable = getOutputTableOf(node->getChildren()[0]->getExecutor());
    }
    else {
        targetTable = getTargetTable();
    }
    assert(targetTable);

    //
    // OPTIMIZATION: If there is no predicate for this SeqScan,
    // then we want to just set our OutputTable pointer to be the
    // pointer of our TargetTable. This prevents us from just
    // reading through the entire TargetTable and copying all of
    // the tuples. We are guarenteed that no Executor will ever
    // modify an input table, so this operation is safe
    //
    if (getPredicate() == NULL && node->getInlinePlanNodes().empty()) {
        m_output_is_input = true;
        setOutputTable(targetTable);
        return true;
    }

    m_output_is_input = false;
    // Otherwise create a new temp table that mirrors the output schema specified in the plan
    // (which should mirror the output schema for any inlined projection)
    const std::string& temp_name = targetTable->name();
    setTempOutputTable(limits, temp_name);
    return true;
}

bool SeqScanExecutor::p_execute()
{
    if (m_output_is_input) {
        return true;
    }
    TempTable* output_table = getTempOutputTable();
    assert(output_table);

    bool isSubquery = m_input_tables.size() > 0;
    Table* input_table = isSubquery ? m_input_tables[0].getTable() : getTargetTable();
    assert(input_table);

    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " input table " << (void*)input_table <<
    //* for debug */    " has " << input_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("Sequential Scanning table :\n %s",
               input_table->debug().c_str());
    VOLT_DEBUG("Sequential Scanning table : %s which has %d active, %d"
               " allocated",
               input_table->name().c_str(),
               (int)input_table->activeTupleCount(),
               (int)input_table->allocatedTupleCount());

    //
    // OPTIMIZATION: INLINE PROJECTION
    // Since we have the input params, we need to call substitute to
    // change any nodes in our expression tree to be ready for the
    // projection operations in execute
    //
    int num_of_columns = (int)output_table->columnCount();
    TableTuple &temp_tuple = output_table->tempTuple();

    TableTuple tuple(input_table->schema());
    TableIterator iterator = input_table->iteratorDeletingAsWeGo();

    AbstractExpression *predicate = getPredicate();
    if (predicate) {
        VOLT_TRACE("SCAN PREDICATE A:\n%s\n", predicate->debug(true).c_str());
    }

    int limit = -1;
    int offset = -1;
    getLimitAndOffsetByReference(limit, offset);
    int tuple_ctr = 0;
    int tuple_skipped = 0;

    const AbstractExpression* const* projection_expressions = NULL;
    const int* projection_columns = getProjectionColumns();
    if (projection_columns == NULL) {
        projection_expressions = getProjectionExpressions();
    }

    // Report progress on a persistent target table even if it is from a trivial "select *" subquery.
    ProgressMonitorProxy pmp(m_engine, this, dynamic_cast<PersistentTable*>(input_table));

    //
    // Just walk through the table using our iterator and apply
    // the predicate to each tuple. For each tuple that satisfies
    // our expression, we'll insert them into the output table.
    //
    while ((limit == -1 || tuple_ctr < limit) && iterator.next(tuple)) {
        VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                   tuple.debug(input_table->name()).c_str(), tuple_ctr,
                   (int)input_table->activeTupleCount());
        pmp.countdownProgress();

        if (predicate == NULL || predicate->eval(&tuple, NULL).isTrue()) {
            // Check if we have to skip this tuple because of offset
            if (tuple_skipped < offset) {
                tuple_skipped++;
                continue;
            }
            ++tuple_ctr;

            ProjectionExecutor::insertTempOutputTuple(output_table, tuple, temp_tuple, num_of_columns,
                                                      projection_columns, projection_expressions);
            pmp.countdownProgress();
        }
    }
    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " output table " << (void*)output_table <<
    //* for debug */    " put " << output_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}
