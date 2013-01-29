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

#include "materializeexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "executors/projectionexecutor.h"
#include "expressions/abstractexpression.h"
#include "plannodes/materializenode.h"
#include "storage/temptable.h"

namespace voltdb {

bool MaterializeExecutor::p_init()
{
    VOLT_TRACE("init Materialize Executor");
    MaterializePlanNode* node = dynamic_cast<MaterializePlanNode*>(m_abstractNode);
    assert(node);
    m_columnExpressions = ProjectionExecutor::outputExpressions(node);
    m_paramsOnly = ProjectionExecutor::valuesIfAllParameterValues(m_columnExpressions);
    return (true);
}

bool MaterializeExecutor::p_execute()
{
    assert(dynamic_cast<MaterializePlanNode*>(m_abstractNode));
    int paramColumnCount = (int)m_paramsOnly.size();

    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);
    TableTuple &temp_tuple = output_temp_table->tempTuple();
    if (paramColumnCount > 0) {
        for (int ctr = paramColumnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, *(m_paramsOnly[ctr]));
        }
    } else {
        int exprsCount = (int) m_columnExpressions.size();
        // Add each calculated value to the temp tuple.
        // It must have the same value type as the output column.
        for (int ctr = exprsCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, m_columnExpressions[ctr]->eval());
        }
    }

    // Add tuple to the output
    output_temp_table->insertTempTuple(temp_tuple);
    VOLT_TRACE ("Materialized :\n %s", m_outputTable->debug().c_str());
    return true;
}

MaterializeExecutor::~MaterializeExecutor() {
}

}
