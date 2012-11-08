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

#include "abstractexecutor.h"

#include "plannodes/abstractplannode.h"
#include "storage/table.h"
#include "storage/tablefactory.h"

#include "boost/foreach.hpp"

using namespace std;
using namespace voltdb;

bool AbstractExecutor::init(TempTableLimits* limits)
{
    assert (m_abstractNode);
    //
    // Grab the input tables directly from this node's children
    //
    vector<Table*> input_tables;
    std::vector<AbstractPlanNode*>& children = m_abstractNode->getChildren();
    BOOST_FOREACH(AbstractPlanNode* child, children) {
        Table* table = child->getOutputTable();
        if (table == NULL) {
            VOLT_ERROR("Output table from PlanNode '%s' is NULL",
                       child->debug().c_str());
            return false;
        }
        m_inputTables.push_back(table);
        // Optimize access to the first -- often the only -- input table.
        if (m_inputTable == NULL) {
            m_inputTable = table;
        }
    }

    // Initialize the output table just as needed by the derived class.
    // This step is separate from p_init since it has only a handful of possible
    // implementations that can often be inherited.
    p_setOutputTable(limits);

    // Call the highly specialized p_init() method of the derived class.
    if ( ! p_init()) {
        return false;
    }
    return true;
}

AbstractExecutor::~AbstractExecutor()
{
    // If it's not a TempTable, it's not owned by this executor, so delete nothing.
    delete dynamic_cast<TempTable*>(m_outputTable);
}

/**
 * Set up a multi-column temp output table for those executors that require one.
 * Called from p_setOutputTable.
 */
void AbstractExecutor::setTempOutputTable(TempTableLimits* limits, const string tempTableName) {
    assert(limits);
    CatalogId databaseId = m_abstractNode->databaseId();
    TupleSchema* schema = m_abstractNode->generateTupleSchema(true);
    int column_count = (int)m_abstractNode->getOutputSchema().size();
    vector<string> column_names(column_count);
    assert(column_count >= 1);
    for (int ctr = 0; ctr < column_count; ctr++)
    {
        column_names[ctr] = m_abstractNode->getOutputSchema()[ctr]->getColumnName();
    }
    m_outputTable = TableFactory::getTempTable(databaseId,
                                               tempTableName,
                                               schema,
                                               column_names,
                                               limits);
}

/**
 * Set up a temp table to pass through tuples from a child node.
 * Called from p_setOutputTable.
 */
void AbstractExecutor::setPassThroughTempOutputTable(TempTableLimits* limits) {
    assert(m_inputTable);
    assert(m_abstractNode->getChildren()[0] != NULL);
    CatalogId databaseId = m_abstractNode->databaseId();
    const string &name = m_inputTable->name();
    m_outputTable = TableFactory::getCopiedTempTable(databaseId,
                                                     name,
                                                     m_inputTable,
                                                     limits);
}
