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

#include "abstractexecutor.h"

#include "execution/VoltDBEngine.h"
#include "plannodes/abstractoperationnode.h"
#include "plannodes/abstractscannode.h"
#include "storage/tablefactory.h"
#include "storage/TableCatalogDelegate.hpp"

#include <vector>

using namespace std;

namespace voltdb {

bool AbstractExecutor::init(VoltDBEngine* engine,
                            TempTableLimits* limits)
{
    //TODO: stop passing engine to each executor constructor
    // -- that's covered in one line here.
    m_engine = engine;
    assert (m_abstractNode);

    //
    // Grab the input tables directly from this node's children
    //
    vector<Table*> input_tables;
    const std::vector<AbstractPlanNode*>& children = m_abstractNode->getChildren();
    for (int ctr = 0, cnt = static_cast<int>(children.size()); ctr < cnt; ctr++) {
        Table* table = children[ctr]->getOutputTable();
        if (table == NULL) {
            VOLT_ERROR("Output table from PlanNode '%s' is NULL", children[ctr]->debug().c_str());
            return false;
        }
        input_tables.push_back(table);
    }
    m_abstractNode->setInputTables(input_tables);

    // Call the p_init() method on our derived class
    //TODO: stop passing each executor its own data member: m_abstractNode
    return p_init(m_abstractNode, limits);
}

/**
 * Set up a multi-column temp output table for those executors that require one.
 * Called from p_init.
 */
void AbstractExecutor::setTempOutputTable(TempTableLimits* limits, const string tempTableName)
{
    assert(limits);
    TupleSchema* schema = m_abstractNode->generateTupleSchema(true);
    int column_count = (int)m_abstractNode->getOutputSchema().size();
    std::vector<std::string> column_names(column_count);
    assert(column_count >= 1);
    for (int ctr = 0; ctr < column_count; ctr++) {
        column_names[ctr] = m_abstractNode->getOutputSchema()[ctr]->getColumnName();
    }
    m_tmpOutputTable = TableFactory::getTempTable(m_abstractNode->databaseId(),
                                                  tempTableName,
                                                  schema,
                                                  column_names,
                                                  limits);
    m_abstractNode->setOutputTable(m_tmpOutputTable);
}

void AbstractExecutor::setTempOutputLikeInputTable(TempTableLimits* limits)
{
    assert(m_abstractNode->getInputTables().size() >= 1);
    Table* input_table = m_abstractNode->getInputTable();
    m_tmpOutputTable = TableFactory::getCopiedTempTable(m_abstractNode->databaseId(),
                                                        input_table->name(),
                                                        input_table,
                                                        limits);
    m_abstractNode->setOutputTable(m_tmpOutputTable);
}

/**
 * Set up a single-column temp output table for DML executors that require one to return their counts.
 * Called from p_init.
 */
void AbstractExecutor::setDMLCountOutputTable(TempTableLimits* limits)
{
    TupleSchema* schema = m_abstractNode->generateDMLCountTupleSchema();
    const std::vector<std::string> columnNames(1, "modified_tuples");
    m_tmpOutputTable = TableFactory::getTempTable(m_abstractNode->databaseId(),
                                                  "temp",
                                                  schema,
                                                  columnNames,
                                                  limits);
    m_abstractNode->setOutputTable(m_tmpOutputTable);
}

AbstractExecutor::~AbstractExecutor() { }

} // namespace voltdb
