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

#include "nestloopexecutor.h"

#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "plannodes/nestloopnode.h"
#include "plannodes/limitnode.h"

#ifdef VOLT_DEBUG_ENABLED
#include <ctime>
#include <sys/times.h>
#include <unistd.h>
#endif

namespace voltdb {

bool NestLoopExecutor::p_init(TempTableLimits* limits)
{
    VOLT_TRACE("init NestLoop Executor");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(m_abstractNode);
    assert(node);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    //
    // Pre Join Expression
    //
    m_preJoinPredicate = node->getPreJoinPredicate();
    m_joinPredicate = node->getJoinPredicate();
    m_wherePredicate = node->getWherePredicate();

    m_join_type = node->getJoinType();
    assert(m_join_type == JOIN_TYPE_INNER || m_join_type == JOIN_TYPE_LEFT);

    // NULL tuple for outer join
    if (m_join_type == JOIN_TYPE_LEFT) {
        Table* inner_table = m_input_tables[1].getTable();
        assert(inner_table);
        m_null_tuple.init(inner_table->schema());
    }

    // pickup an inlined limit, if one exists
    LimitPlanNode* limit_node =
        static_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    if (limit_node) {
        m_inlineLimitOffset = limit_node->getState();
    }

    return true;
}


bool NestLoopExecutor::p_execute()
{
    VOLT_DEBUG("executing NestLoop...");

    assert(dynamic_cast<NestLoopPlanNode*>(m_abstractNode));
    assert(m_input_tables.size() == 2);

    TempTable* output_table = getTempOutputTable();
    assert(output_table);

    Table* outer_table = m_input_tables[0].getTable();
    assert(outer_table);

    Table* inner_table = m_input_tables[1].getTable();
    assert(inner_table);

    VOLT_TRACE ("input table left:\n %s", outer_table->debug().c_str());
    VOLT_TRACE ("input table right:\n %s", inner_table->debug().c_str());

    int limit = -1;
    int offset = -1;
    m_inlineLimitOffset.getLimitAndOffsetByReference(m_engine, limit, offset);
    int tuple_ctr = 0;
    int tuple_skipped = 0;

    int outer_cols = outer_table->columnCount();
    int inner_cols = inner_table->columnCount();
    TableTuple outer_tuple(outer_table->schema());
    TableTuple inner_tuple(inner_table->schema());
    TableTuple &joined = output_table->tempTuple();
    m_null_tuple.resetWithCompatibleSchema(inner_table->schema());
    TableTuple null_tuple = m_null_tuple;

    TableIterator iterator0 = outer_table->iteratorDeletingAsWeGo();
    ProgressMonitorProxy pmp(m_engine, this, inner_table);

    while ((limit == -1 || tuple_ctr < limit) && iterator0.next(outer_tuple)) {
        pmp.countdownProgress();

        // populate output table's temp tuple with outer table's values
        // probably have to do this at least once - avoid doing it many
        // times per outer tuple
        joined.setNValues(0, outer_tuple, 0, outer_cols);

        // did this loop body find at least one match for this tuple?
        bool match = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (m_preJoinPredicate == NULL || m_preJoinPredicate->eval(&outer_tuple, NULL).isTrue()) {

            // By default, the delete as we go flag is false.
            TableIterator iterator1 = inner_table->iterator();
            while ((limit == -1 || tuple_ctr < limit) && iterator1.next(inner_tuple)) {
                pmp.countdownProgress();
                // Apply join filter to produce matches for each outer that has them,
                // then pad unmatched outers, then filter them all
                if (m_joinPredicate == NULL || m_joinPredicate->eval(&outer_tuple, &inner_tuple).isTrue()) {
                    match = true;
                    // Filter the joined tuple
                    if (m_wherePredicate == NULL || m_wherePredicate->eval(&outer_tuple, &inner_tuple).isTrue()) {
                        // Check if we have to skip this tuple because of offset
                        if (tuple_skipped < offset) {
                            tuple_skipped++;
                            continue;
                        }
                        ++tuple_ctr;
                        // Matched! Complete the joined tuple with the inner column values.
                        joined.setNValues(outer_cols, inner_tuple, 0, inner_cols);
                        output_table->insertTempTuple(joined);
                        pmp.countdownProgress();
                    }
                }
            }
        }
        //
        // Left Outer Join
        //
        if ((limit == -1 || tuple_ctr < limit) && m_join_type == JOIN_TYPE_LEFT && !match) {
            // Still needs to pass the filter
            if (m_wherePredicate == NULL || m_wherePredicate->eval(&outer_tuple, &null_tuple).isTrue()) {
                // Check if we have to skip this tuple because of offset
                if (tuple_skipped < offset) {
                    tuple_skipped++;
                    continue;
                }
                ++tuple_ctr;
                joined.setNValues(outer_cols, null_tuple, 0, inner_cols);
                output_table->insertTempTuple(joined);
                pmp.countdownProgress();
            }
        }
    }

    return true;
}

}
