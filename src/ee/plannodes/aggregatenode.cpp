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

#include "aggregatenode.h"
#include "common/debuglog.h"
#include "expressions/abstractexpression.h"

#include <boost/foreach.hpp>
#include <sstream>

using namespace json_spirit;
using namespace std;
using namespace voltdb;

AggregatePlanNode::~AggregatePlanNode()
{
    for (int i = 0; i < m_aggregateInputExpressions.size(); i++)
    {
        delete m_aggregateInputExpressions[i];
    }
    for (int i = 0; i < m_groupByExpressions.size(); i++)
    {
        delete m_groupByExpressions[i];
    }
}

string AggregatePlanNode::debugInfo(const string &spacer) const {
    ostringstream buffer;
    buffer << spacer << "\nAggregates["
           << (int) m_aggregates.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregates.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << "type="
               << expressionToString(m_aggregates[ctr]) << "\n";
        buffer << spacer << "distinct="
               << m_distinctAggregates[ctr] << "\n";
        buffer << spacer << "outcol="
               << m_aggregateOutputColumns[ctr] << "\n";
        buffer << spacer << "expr="
               << m_aggregateInputExpressions[ctr]->debug(spacer) << "\n";
    }
    buffer << spacer << "}";

    buffer << spacer << "\nGroupByExpressions[";
    string add = "";
    for (int ctr = 0, cnt = (int) m_groupByExpressions.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << m_groupByExpressions[ctr]->debug(spacer);
        add = ", ";
    }
    buffer << "]\n";

    return buffer.str();
}

void
AggregatePlanNode::loadFromJSONObject(Object &obj)
{
    Value aggregateColumnsValue = find_value(obj, "AGGREGATE_COLUMNS");
    vector<string> types = loadStringsFromJSONArray(obj, "AGGREGATE_TYPE");
    BOOST_FOREACH(const string& aggtype, types) {
        m_aggregates.push_back(stringToExpression(aggtype));
    }
    assert(types.size() == m_aggregates.size());

    vector<int> distincts = loadIntegersFromJSONArray(obj, "AGGREGATE_DISTINCT");
    BOOST_FOREACH(int distinct, distincts) {
        m_distinctAggregates.push_back(distinct);
    }
    assert(types.size() == m_distinctAggregates.size());

    m_aggregateOutputColumns = loadIntegersFromJSONArray(obj, "AGGREGATE_OUTPUT_COLUMN");
    assert(types.size() == m_aggregateOutputColumns.size());

    m_aggregateInputExpressions = loadExpressionsFromJSONArray(obj, "AGGREGATE_EXPRESSION");
    assert(types.size() == m_aggregateInputExpressions.size());

    m_groupByExpressions = loadExpressionsFromJSONArray(obj, "GROUPBY_EXPRESSIONS");
}


void AggregatePlanNode::collectOutputExpressions(std::vector<AbstractExpression*>& outputColumnExpressions) const
{
    const std::vector<SchemaColumn*>& outputSchema = getOutputSchema();
    size_t size = outputSchema.size();
    outputColumnExpressions.resize(size);
    for (int ii = 0; ii < size; ii++) {
        SchemaColumn* outputColumn = outputSchema[ii];
        outputColumnExpressions[ii] = outputColumn->getExpression();
    }
}

// definitions of public test methods

void
AggregatePlanNode::setAggregates(vector<ExpressionType> &aggregates)
{
    m_aggregates = aggregates;
}

void
AggregatePlanNode::setAggregateOutputColumns(vector<int> outputColumns)
{
    m_aggregateOutputColumns = outputColumns;
}
