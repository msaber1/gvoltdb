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

#include "abstractoperationexecutor.h"

#include "common/ValueFactory.hpp"
#include "execution/VoltDBEngine.h"
#include "plannodes/abstracttableionode.h"
#include "storage/table.h"
#include "storage/tablefactory.h"


using namespace std;
using namespace voltdb;

/**
 * Set up a single-column temp output table for DML executors that require one to return their counts.
 */
void AbstractOperationExecutor::p_setOutputTable(TempTableLimits* limits) {
    TupleSchema* schema = m_abstractNode->generateTupleSchema(false);
    // The column count (1) and column name for the DML counter column is hard-coded in the planner
    // and passed via the output schema -- kind of pointless since they could just as easily be hard-coded here,
    // possibly saving the trouble of serializing an outputSchema at all for DML nodes.
    assert(m_abstractNode->getOutputSchema().size() == 1);
    CatalogId databaseId = m_abstractNode->databaseId();
    const string name("temp");
    vector<string> columnNames(1, m_abstractNode->getOutputSchema()[0]->getColumnName());
    m_outputTable = TableFactory::getTempTable(databaseId,
                                               name,
                                               schema,
                                               columnNames,
                                               limits);
}

bool AbstractOperationExecutor::storeModifiedTupleCount(int64_t modifiedTuples) {
    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);
    TableTuple& count_tuple = output_temp_table->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modifiedTuples));
    // try to put the count tuple into the output table
    if (!output_temp_table->insertTempTuple(count_tuple)) {
        VOLT_ERROR("Failed to insert tuple count (%ld) into result table", modifiedTuples);
        return false;
    }

    // add to the planfragments count of modified tuples
    getEngine()->m_tuplesModified += modifiedTuples;
    VOLT_INFO("Finished modifying table");
    return true;
}
