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

#include "plannodes/abstracttableionode.h"
#include "storage/table.h"
#include "storage/tablefactory.h"

#include <vector>

using namespace std;
using namespace voltdb;

bool AbstractExecutor::init(TempTableLimits* limits)
{
    assert (m_abstractNode);
    //
    // Grab the input tables directly from this node's children
    //
    vector<Table*> input_tables;
    for (int ctr = 0,
             cnt = static_cast<int>(m_abstractNode->getChildren().size());
         ctr < cnt;
         ctr++)
    {
        Table* table = m_abstractNode->getChildren()[ctr]->getOutputTable();
        if (table == NULL) {
            VOLT_ERROR("Output table from PlanNode '%s' is NULL",
                       m_abstractNode->getChildren()[ctr]->debug().c_str());
            return false;
        }
        m_inputTables.push_back(table);
        if (cnt == 0) {
            m_inputTable = table;
        }
    }

    try {
        // Initialize the output table just as needed by the derived class.
        // This step is separate from p_init since it has only a handful of possible
        // implementations that can often be inherited.
        p_setOutputTable(limits);

        // Call the p_init() method on our derived class.
        if ( ! p_init()) {
            return false;
        }
    } catch (exception& err) {
        char message[128];
        snprintf(message, 128, "The Executor failed to initialize PlanNode '%s'",
                m_abstractNode->debug().c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      message);
    }
    return true;
}

/**
 * Set up a multi-column temp output table for those executors that require one.
 * Called from p_init.
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
 * Called from p_init.
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

AbstractExecutor::~AbstractExecutor()
{
    // If it's not a TempTable, it's not owned by this executor, so delete nothing.
    delete dynamic_cast<TempTable*>(m_outputTable);
}


// Hook up abstract scan and (write) operation executors with the engine
// and with the target table known to the engine.
// Other executors (Send, Recieve, and Materialize) also provide initEngine specializations
// to access the engine for various other purposes.
bool AbstractTableIOExecutor::initEngine(VoltDBEngine* engine)
{
    m_engine = engine;

    AbstractTableIOPlanNode* node = dynamic_cast<AbstractTableIOPlanNode*>(m_abstractNode);
    assert(node);
    if (!node) {
        return false;
    }
    m_targetTable = node->resolveTargetTable(engine);
    return (m_targetTable != NULL);
}

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
