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

#include "projectionexecutor.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "expressions/expressionutil.h"
#include "plannodes/projectionnode.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"

namespace voltdb {

bool ProjectionExecutor::p_init(TempTableLimits* limits)
{
    VOLT_TRACE("init Projection Executor");

    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(m_abstractNode);
    assert(node);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    // initialize local variables
    m_state.init(node);
    return true;
}

bool ProjectionExecutor::p_execute()
{
    TempTable* output_table = getTempOutputTable();
    assert (output_table);
    TableTuple &temp_tuple = output_table->tempTuple();

    Table* input_table = getInputTable();
    assert (input_table);
    TableTuple tuple(input_table->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", input_table->debug().c_str());

    AbstractExpression* const* projection_expressions = NULL;
    const int* projection_columns = m_state.getProjectionColumns();
    if (projection_columns == NULL) {
        projection_expressions = m_state.getProjectionExpressions();
    }

    // Now loop through all the tuples and push them through our output
    // expression This will generate new tuple values that we will insert into
    // our output table
    int num_of_columns = output_table->columnCount();
    TableIterator iterator = input_table->iteratorDeletingAsWeGo();
    while (iterator.next(tuple)) {
        insertTempOutputTuple(output_table, tuple, temp_tuple, num_of_columns,
                              projection_columns, projection_expressions);
        VOLT_TRACE("OUTPUT TABLE: %s\n", output_table->debug().c_str());
    }
    VOLT_TRACE("PROJECTED TABLE: %s\n", output_table->debug().c_str());
    return true;
}

void ProjectionPlanNode::InlineState::init(ProjectionPlanNode* projection_node)
{
    const std::vector<voltdb::AbstractExpression*>&
        projection_expr_vector = projection_node->getOutputColumnExpressions();

    m_all_column_array_ptr = ExpressionUtil::convertIfAllTupleValues(projection_expr_vector);

    if (m_all_column_array_ptr.get() == NULL) {
        int num_of_columns = (int)projection_expr_vector.size();
        AbstractExpression** projection_expressions = new AbstractExpression*[num_of_columns];
        for (int ctr = 0; ctr < num_of_columns; ctr++) {
            assert(projection_expr_vector[ctr]);
            projection_expressions[ctr] = projection_expr_vector[ctr];
        }
        m_expression_array_ptr.reset(projection_expressions);
    }
}

}
