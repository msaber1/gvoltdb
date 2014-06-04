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

#include "materializeexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "expressions/abstractexpression.h"
#include "plannodes/materializenode.h"
#include "storage/table.h"
#include "storage/temptable.h"

namespace voltdb {

bool MaterializeExecutor::p_init(TempTableLimits* limits)
{
    VOLT_TRACE("init Materialize Executor");

    MaterializePlanNode* node = dynamic_cast<MaterializePlanNode*>(m_abstractNode);
    assert(node);

    m_batched = node->isBatched();

    if (m_batched) {
        // Create output table based on output schema from the plan
        setTempOutputTable(limits);
        return true;
    }

    ProjectionExecutor::p_init(limits);
    m_all_param_array = node->getOutputParameterIdArrayIfAllParameters();
    return true;
}

bool MaterializeExecutor::p_execute()
{
    TempTable* output_table = getTempOutputTable();
    TableTuple &temp_tuple = output_table->tempTuple();
    assert (output_table);
    int columnCount = output_table->columnCount();

    // batched insertion
    if (m_batched) {
        int paramcnt = m_engine->getUsedParamcnt();
        const NValueArray& params = m_engine->getParameterContainer();
        VOLT_TRACE("batched insertion with %d params. %d for each tuple.", paramcnt, columnCount);
        TableTuple &temp_tuple = output_table->tempTuple();
        for (int i = 0, tuples = paramcnt / columnCount; i < tuples; ++i) {
            for (int j = columnCount - 1; j >= 0; --j) {
                temp_tuple.setNValue(j, params[i * columnCount + j]);
            }
            output_table->insertTempTuple(temp_tuple);
        }
        VOLT_TRACE ("Materialized :\n %s", output_table->debug().c_str());
        return true;
    }


    // For now a MaterializePlanNode can make at most one new tuple We
    // should think about whether we would ever want to materialize
    // more than one tuple and whether such a thing is possible with
    // the AbstractExpression scheme
    if (m_all_param_array != NULL) {
        const NValueArray& params = m_engine->getParameterContainer();
        VOLT_TRACE("sweet, all params\n");
        for (int ctr = columnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, params[m_all_param_array[ctr]]);
        }
    }
    else {
        const AbstractExpression* const* expression_array = m_expression_array;
        TableTuple dummy;
        // add the generated value to the temp tuple. it must have the
        // same value type as the output column.
        for (int ctr = columnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, expression_array[ctr]->eval(&dummy, NULL));
        }
    }

    // Add tuple to the output
    output_table->insertTempTuple(temp_tuple);
    return true;
}

}
