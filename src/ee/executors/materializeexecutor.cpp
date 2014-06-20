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

#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "plannodes/materializenode.h"
#include "storage/temptable.h"

namespace voltdb {

bool MaterializeExecutor::p_init(AbstractPlanNode* abstractNode,
                                 TempTableLimits* limits)
{
    VOLT_TRACE("init Materialize Executor");

    MaterializePlanNode* node = dynamic_cast<MaterializePlanNode*>(m_abstractNode);
    assert(node);
    batched = node->isBatched();

    // Construct the output table
    int columnCount = static_cast<int>(node->getOutputSchema().size());
    assert(columnCount >= 0);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    std::vector<AbstractExpression*>& columnExpressions = node->getOutputColumnExpressions();

    // initialize local variables
    all_param_array_ptr = ExpressionUtil::convertIfAllParameterValues(columnExpressions);
    all_param_array = all_param_array_ptr.get();

    needs_substitute = new bool[columnCount];
    needs_substitute_ptr.reset(needs_substitute);

    expression_array = new AbstractExpression*[columnCount];
    expression_array_ptr.reset(expression_array);

    for (int ctr = 0; ctr < columnCount; ctr++) {
        assert (columnExpressions[ctr] != NULL);
        expression_array_ptr[ctr] = columnExpressions[ctr];
        needs_substitute_ptr[ctr] = columnExpressions[ctr]->hasParameter();
    }

    return true;
}

bool MaterializeExecutor::p_execute(const NValueArray &params) {
    assert(dynamic_cast<MaterializePlanNode*>(m_abstractNode));
    assert (m_tmpOutputTable);

    int columnCount = m_tmpOutputTable->columnCount();

    // batched insertion
    if (batched) {
        int paramcnt = m_engine->getUsedParamcnt();
        VOLT_TRACE("batched insertion with %d params. %d for each tuple.", paramcnt, columnCount);
        TableTuple &temp_tuple = m_tmpOutputTable->tempTuple();
        for (int i = 0, tuples = paramcnt / columnCount; i < tuples; ++i) {
            for (int j = columnCount - 1; j >= 0; --j) {
                temp_tuple.setNValue(j, params[i * columnCount + j]);
            }
            m_tmpOutputTable->insertTempTuple(temp_tuple);
        }
        VOLT_TRACE ("Materialized :\n %s", m_tmpOutputTable->debug().c_str());
        return true;
    }


    // substitute parameterized values in expression trees.
    if (all_param_array == NULL) {
        for (int ctr = columnCount - 1; ctr >= 0; --ctr) {
            assert(expression_array[ctr]);
            VOLT_TRACE("predicate[%d]: %s", ctr, expression_array[ctr]->debug(true).c_str());
        }
    }

    // For now a MaterializePlanNode can make at most one new tuple We
    // should think about whether we would ever want to materialize
    // more than one tuple and whether such a thing is possible with
    // the AbstractExpression scheme
    TableTuple &temp_tuple = m_tmpOutputTable->tempTuple();
    if (all_param_array != NULL) {
        VOLT_TRACE("sweet, all params\n");
        for (int ctr = columnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, params[all_param_array[ctr]]);
        }
    }
    else {
        TableTuple dummy;
        // add the generated value to the temp tuple. it must have the
        // same value type as the output column.
        for (int ctr = columnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, expression_array[ctr]->eval(&dummy, NULL));
        }
    }

    // Add tuple to the output
    m_tmpOutputTable->insertTempTuple(temp_tuple);

    return true;
}

MaterializeExecutor::~MaterializeExecutor() {
}

} // namespace voltdb
