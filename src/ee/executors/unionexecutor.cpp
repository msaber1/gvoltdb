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

#include "unionexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/unionnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"

namespace voltdb {

bool UnionExecutor::p_init()
{
    VOLT_TRACE("init Union Executor");

    assert(dynamic_cast<UnionPlanNode*>(m_abstractNode));

    //
    // First check to make sure they have the same number of columns
    //
    assert(getInputTables().size() > 1);
    for (int table_ctr = 1, table_cnt = (int)getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
        if (m_inputTable->columnCount() != getInputTables()[table_ctr]->columnCount()) {
            VOLT_ERROR("Table '%s' has %d columns, but table '%s' has %d"
                       " columns",
                       m_inputTable->name().c_str(),
                       m_inputTable->columnCount(),
                       getInputTables()[table_ctr]->name().c_str(),
                       getInputTables()[table_ctr]->columnCount());
            return false;
        }
    }

    //
    // Then check that they have the same types
    // The two loops here are broken out so that we don't have to keep grabbing the same column for input_table[0]
    //

    // get the first table
    const TupleSchema *table0Schema = m_inputTable->schema();
    // iterate over all columns in the first table
    for (int col_ctr = 0, col_cnt = table0Schema->columnCount(); col_ctr < col_cnt; col_ctr++) {
        // get the type for the current column
        ValueType type0 = table0Schema->columnType(col_ctr);

        // iterate through all the other tables, comparing one column at a time
        for (int table_ctr = 1, table_cnt = (int)getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
            // get another table
            const TupleSchema *table1Schema = getInputTables()[table_ctr]->schema();
            ValueType type1 = table1Schema->columnType(col_ctr);
            if (type0 != type1) {
                // TODO: DEBUG
                VOLT_ERROR("Table '%s' has value type '%s' for column '%d',"
                           " table '%s' has value type '%s' for column '%d'",
                           m_inputTable->name().c_str(),
                           getTypeName(type0).c_str(),
                           col_ctr,
                           getInputTables()[table_ctr]->name().c_str(),
                           getTypeName(type1).c_str(), col_ctr);
                return false;
            }
        }
    }
    return (true);
}

bool UnionExecutor::p_execute() {
    assert(dynamic_cast<UnionPlanNode*>(m_abstractNode));
    assert(m_outputTable);

    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);

    //
    // For each input table, grab their TableIterator and then append all of its tuples
    // to our ouput table
    //
    for (int ctr = 0, cnt = (int)getInputTables().size(); ctr < cnt; ctr++) {
        Table* input_table = getInputTables()[ctr];
        assert(input_table);
        TableIterator iterator = input_table->iterator();
        TableTuple tuple(input_table->schema());
        while (iterator.next(tuple)) {
            if ( ! output_temp_table->insertTempTuple(tuple)) {
                VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                           " output table '%s'",
                           input_table->name().c_str(),
                           m_outputTable->name().c_str());
                return false;
            }
        }
        // FIXME: node->tables[ctr]->onTableRead(undo);
    }

    return (true);
}

}
