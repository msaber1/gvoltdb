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

#include "abstractoperationexecutor.h"

#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "plannodes/abstractoperationnode.h"
#include "storage/TableCatalogDelegate.hpp"
#include "storage/temptable.h"

namespace voltdb {

bool AbstractOperationExecutor::p_init(TempTableLimits* limits)
{
    VOLT_TRACE("init DML Executor");

    setDMLCountOutputTable(limits);

    AbstractOperationPlanNode* node = dynamic_cast<AbstractOperationPlanNode*>(m_abstractNode);
    assert(node);
    m_target_tcd = m_engine->getTableDelegate(node->getTargetTableName());
    assert(m_target_tcd);
    p_initMore();
    return true;
}

Table* AbstractOperationExecutor::getTargetTable()
{
    return m_target_tcd->getTable();
}

void AbstractOperationExecutor::setModifiedTuples(int64_t modified_tuples)
{
    TempTable* output_table = getTempOutputTable();
    TableTuple& count_tuple = output_table->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
    // try to put the tuple into the output table
    output_table->insertTempTuple(count_tuple);
    m_engine->m_tuplesModified += modified_tuples;
}

}
