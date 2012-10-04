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

#ifndef VOLTDBNODEABSTRACTEXECUTOR_H
#define VOLTDBNODEABSTRACTEXECUTOR_H

#include "plannodes/abstractplannode.h"
#include "storage/temptable.h"
#include <cassert>

namespace voltdb {

class TempTableLimits;
class VoltDBEngine;

/**
 * AbstractExecutor provides the API for initializing and invoking executors.
 */
class AbstractExecutor {
  public:
    virtual ~AbstractExecutor();

    /**
     * Set/get the plannode that generated this executor.
     */
    void initPlanNode(AbstractPlanNode* abstractNode) { m_abstractNode = abstractNode; }
    inline AbstractPlanNode* getPlanNode() { return m_abstractNode; }

    // By default, executors don't need to be engine-aware, but a few do.
    virtual bool initEngine(VoltDBEngine* engine) { return true; }

    /** Executors are initialized once when the catalog is loaded */
    bool init(TempTableLimits* limits);

    /** Invoke a plannode's associated executor */
    bool execute();

    /**
     * Returns true if the output table for the plannode must be cleaned up
     * after p_execute().  <b>Default is false</b>. This should be overriden in
     * the receive executor since this is the only place we need to clean up the
     * output table.
     */
    virtual bool needsPostExecuteClear() { return false; }

    //
    // Output Table
    // This is where we will write the results of the plan node's
    // execution out to.
    // This public accessor is provided to allow the output table of one executor
    // to be passed up the plannode chain as the input table of a parent node's executor.
    //
    Table *getOutputTable() const { return m_outputTable; }

protected:
    AbstractExecutor() :
        m_abstractNode(NULL),
        m_inputTable(NULL),
        m_outputTable(NULL),
        m_inputTables()
    {}

    // By default, executors get a node-schema-based temp table named "temp".
    // Exceptions include DML nodes, pass-through executors, and scans.
    virtual void p_setOutputTable(TempTableLimits* limits) { setTempOutputTable(limits); }

    /** Concrete executor classes implement initialization in p_init() */
    virtual bool p_init() = 0;

    /** Concrete executor classes impelmenet execution in p_execute() */
    virtual bool p_execute() = 0;

    /**
     * Set up a multi-column temp output table for those executors that require one.
     * Called from p_setOutputTable.
     */
    void setTempOutputTable(TempTableLimits* limits, const std::string tempTableName="temp");
    void setPassThroughTempOutputTable(TempTableLimits* limits);

    const std::vector<Table*> &getInputTables() const { return m_inputTables; }
    bool hasExactlyOneInputTable() const { return m_inputTables.size() == 1; }

    // execution engine owns the plannode allocation.
    AbstractPlanNode* m_abstractNode;

    // The input table may be NULL OR the one and only OR the first of many input tables
    // which are the cached output tables from m_abstractNode->m_child[i]->m_executor->m_outputTable
    // getInputTables() provides more generalized access to all of these input tables in a vector for
    // those executors that need more than one.
    Table* m_inputTable;
    Table* m_outputTable; // either a TempTable or, for some seqscans, m_abstractNode->m_targetTable

private:
    std::vector<Table*> m_inputTables;
};

inline bool AbstractExecutor::execute()
{
    assert(m_abstractNode);
    VOLT_TRACE("Starting execution of plannode(id=%d)...",
               m_abstractNode->getPlanNodeId());

    if (dynamic_cast<TempTable*>(m_outputTable))
    {
        VOLT_TRACE("Clearing output table...");
        dynamic_cast<TempTable*>(m_outputTable)->TempTable::deleteAllTuples(false);
    }

    // run the executor
    return p_execute();
}

}

#endif
