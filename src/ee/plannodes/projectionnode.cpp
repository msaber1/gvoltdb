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

#include "projectionnode.h"

#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"

#include "SchemaColumn.h"

using namespace std;

namespace voltdb {

PlanNodeType ProjectionPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_PROJECTION; }

string ProjectionPlanNode::debugInfo(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "Projection Output[" << m_outputColumnNames.size() << "]:\n";
    const AbstractExpression* const* outputExpressions = getOutputExpressionArray();
    for (int ctr = 0, cnt = (int)m_outputColumnNames.size(); ctr < cnt; ctr++) {
        buffer << spacer << "  [" << ctr << "] ";
        buffer << "name=" << m_outputColumnNames[ctr] << " : ";
        if (outputExpressions[ctr] != NULL) {
            buffer << outputExpressions[ctr]->debug(spacer + "   ");
        }
        else {
            buffer << spacer << "  <NULL>\n";
        }
    }
    return buffer.str();
}

void ProjectionPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    for (int ii = 0; ii < getOutputSchema().size(); ii++) {
        SchemaColumn* outputColumn = getOutputSchema()[ii];
        m_outputColumnNames.push_back(outputColumn->getColumnName());
    }
}

const int* ProjectionPlanNode::getOutputColumnIdArrayIfAllColumns() const
{
    int columnCount = (int)getOutputSchema().size();
    int* result = ExpressionUtil::convertIfAllTupleValues(getOutputExpressionArray(), columnCount);
    const_cast<ProjectionPlanNode*>(this)->
        m_outputColumnIds.reset(result); // cache for memory management purposes.
    return result;
}

void ProjectionPlanNode::InlineState::initProjectionState(ProjectionPlanNode* projection_node)
{
    if (projection_node) {
        m_expression_array = projection_node->getOutputExpressionArray();
        m_all_column_array = projection_node->getOutputColumnIdArrayIfAllColumns();
    } else {
        m_expression_array = NULL;
        m_all_column_array = NULL;
    }
}

}
