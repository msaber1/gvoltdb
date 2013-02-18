/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "common/common.h"
#include "common/executorcontext.hpp"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "expressions/parametervalueexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"

#include <boost/foreach.hpp>

namespace voltdb {

bool ProjectionExecutor::p_init()
{
    VOLT_TRACE("init Projection Executor");
    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(m_abstractNode);
    assert(node);

    m_columnExpressions = outputExpressions(node);
    m_columnsOnly = indexesIfAllTupleValues(m_columnExpressions);
    if (m_columnsOnly.size() == 0) {
        m_paramsOnly = valuesIfAllParameterValues(m_columnExpressions);
    }

    if (!node->isInline()) {
        m_tuple = TableTuple(m_inputTable->schema());
    }
    return true;
}

void ProjectionExecutor::p_execute() {
    assert (dynamic_cast<ProjectionPlanNode*>(m_abstractNode));
    assert ( ! m_abstractNode->isInline()); // inline projection's execute() should not be called
    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
    int columnsOnlyCount = (int) m_columnsOnly.size();
    int paramsOnlyCount = 0;
    int exprsCount = 0;
    if (columnsOnlyCount == 0) {
        paramsOnlyCount = (int) m_paramsOnly.size();
        if (paramsOnlyCount == 0) {
            exprsCount = (int) m_columnExpressions.size();
        }
    }

    //
    // Now loop through all the tuples and push them through our output
    // expression This will generate new tuple values that we will insert into
    // our output table
    //
    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);
    assert(output_temp_table);
    TableTuple &temp_tuple = output_temp_table->tempTuple();
    TableIterator iterator = m_inputTable->iterator();
    TableTuple tuple(m_inputTable->schema());
    assert (tuple.sizeInValues() == m_inputTable->columnCount());
    //
    // Project values from the input tuple
    //
    if (columnsOnlyCount != 0) {
        VOLT_TRACE("sweet, all tuples");
        while (iterator.next(tuple)) {
            for (int ctr = columnsOnlyCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, tuple.getNValue(m_columnsOnly[ctr]));
            }
            output_temp_table->insertTempTuple(temp_tuple);
        }
    } else if (paramsOnlyCount != 0) {
        VOLT_TRACE("sweet, all params");
        while (iterator.next(tuple)) {
            for (int ctr = paramsOnlyCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, *(m_paramsOnly[ctr]));
            }
            output_temp_table->insertTempTuple(temp_tuple);
        }
    } else {
        while (iterator.next(tuple)) {
            for (int ctr = exprsCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, m_columnExpressions[ctr]->eval(&tuple));
            }
            output_temp_table->insertTempTuple(temp_tuple);
        }
    }
    VOLT_TRACE("PROJECTED TABLE: %s\n", output_temp_table->debug().c_str());
}

std::vector<AbstractExpression*> ProjectionExecutor::outputExpressions(ProjectionPlanNode* node)
{
    std::vector<AbstractExpression*> columnExpressions;
    BOOST_FOREACH(SchemaColumn* outputColumn, node->getOutputSchema()) {
        AbstractExpression* ae = outputColumn->getExpression();
        columnExpressions.push_back(ae);
    }
    return columnExpressions;
}

std::vector<int> ProjectionExecutor::indexesIfAllTupleValues(std::vector<AbstractExpression*>& columnExpressions)
{
    std::vector<int> cols;
    BOOST_FOREACH(AbstractExpression* ae, columnExpressions) {
        TupleValueExpression* tve = dynamic_cast<TupleValueExpression*>(ae);
        if (tve == NULL) {
            cols.resize(0);
            break;
        }
        cols.push_back(tve->getColumnId());
    }
    return cols;
}

std::vector<const NValue*> ProjectionExecutor::valuesIfAllParameterValues(std::vector<AbstractExpression*>& columnExpressions)
{
    std::vector<const NValue*> values;
    const NValueArray& params = ExecutorContext::getParams();
    BOOST_FOREACH(AbstractExpression* ae, columnExpressions) {
        ParameterValueExpression* pve = dynamic_cast<ParameterValueExpression*>(ae);
        if (pve == NULL) {
            values.resize(0);
            break;
        }
        values.push_back(&(params[pve->getParameterId()]));
    }
    return values;
}

ProjectionExecutor::~ProjectionExecutor() {
}

}
