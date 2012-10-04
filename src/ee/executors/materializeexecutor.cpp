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

#include "materializeexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/executorcontext.hpp"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "expressions/abstractexpression.h"
#include "plannodes/materializenode.h"
#include "storage/temptable.h"

namespace voltdb {

bool MaterializeExecutor::p_init()
{
    VOLT_TRACE("init Materialize Executor");
    assert(dynamic_cast<MaterializePlanNode*>(m_abstractNode));
    return (true);
}

bool MaterializeExecutor::p_execute() {
    MaterializePlanNode* node = dynamic_cast<MaterializePlanNode*>(m_abstractNode);
    assert(node);
    assert(!node->isInline()); // inline projection's execute() should not be called

    // Construct the output table
    const std::vector<const NValue*>& paramArray = node->getOutputIfAllParameterValues();
    int paramColumnCount = (int)paramArray.size();

    // output must be a temp table
    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);
    assert(output_temp_table);
    TableTuple &temp_tuple = output_temp_table->tempTuple();

    // Parameter-based materialization (the most common case) single or batched.
    // The batched case assumes that paramaters are referenced serially in index order.
    // The single-tuple case allows out-of-order indexing, but this is not usually needed.
    if (paramColumnCount > 0) {
        // batched insertion
        if (node->isBatched()) {
            const NValueArray& params = ExecutorContext::getParams();
            int paramcnt = m_engine->getUsedParamcnt();
            VOLT_TRACE("batched insertion with %d params. %d for each tuple.", paramcnt, paramColumnCount);
            for (int i = 0, tuples = paramcnt / paramColumnCount; i < tuples; ++i) {
                for (int j = paramColumnCount - 1; j >= 0; --j) {
                    temp_tuple.setNValue(j, params[i * paramColumnCount + j]);
                }
                output_temp_table->TempTable::insertTuple(temp_tuple);
            }
            VOLT_TRACE ("Materialized :\n %s", m_outputTable->debug().c_str());
            return true;
        }

        for (int ctr = paramColumnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, *(paramArray[ctr]));
        }
    } else {
        int exprsCount = (int) node->getOutputColumnExpressions().size();
        const std::vector<AbstractExpression*>& exprArray = node->getOutputColumnExpressions();
        // Add each calculated value to the temp tuple.
        // It must have the same value type as the output column.
        for (int ctr = exprsCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, exprArray[ctr]->eval());
        }
    }
    // Add tuple to the output
    output_temp_table->insertTempTuple(temp_tuple);
    return true;
}

MaterializeExecutor::~MaterializeExecutor() {
}

}
