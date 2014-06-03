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

#ifndef VOLTDBNODEABSTRACTEXECUTOR_H
#define VOLTDBNODEABSTRACTEXECUTOR_H

#include "common/InterruptException.h"
#include "execution/VoltDBEngine.h"
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
    virtual ~AbstractExecutor()
    {
        delete getTempOutputTable();
    }

    /** Executors are initialized once when the catalog is loaded */
    bool init(VoltDBEngine*, TempTableLimits* limits);

    /** Invoke a plannode's associated executor */
    bool execute()
    {
        assert(m_abstractNode);
        VOLT_TRACE("Starting execution of plannode(id=%d)...",  m_abstractNode->getPlanNodeId());
        // run the executor
        return p_execute();
    }

    /**
     * Returns the plannode that generated this executor.
     */
    void setPlanNode(AbstractPlanNode* node) { assert( ! m_abstractNode); m_abstractNode = node; }
    AbstractPlanNode* getPlanNode() const { return m_abstractNode; }

    inline void cleanupTempOutputTable() const
    {
        TempTable* temp_table = getTempOutputTable();
        if (temp_table) {
            VOLT_TRACE("Clearing output table...");
            temp_table->deleteAllTuplesNonVirtual(false);
        }
    }

    class TableReference {
    public:
        TableReference()
        {
            m_temp_table = NULL;
            m_tcd = NULL;
        }
        void setTableDelegate(TableCatalogDelegate* tcd)
        {
            m_temp_table = NULL;
            m_tcd = tcd;
        }
        void setTempTable(TempTable* temp_table)
        {
            m_temp_table = temp_table;
            m_tcd = NULL;
        }
        Table* getTable() const;
        TempTable* getTempTable() const { return m_temp_table; }
    private:
        TempTable* m_temp_table;
        TableCatalogDelegate* m_tcd;
    };

  protected:
    AbstractExecutor() : m_abstractNode(NULL) { }

    /** Concrete executor classes implement initialization in p_init() */
    virtual bool p_init(TempTableLimits* limits) = 0;

    /** Concrete executor classes implement execution in p_execute() */
    virtual bool p_execute() = 0;

    /**
     * Set up a multi-column temp output table for those executors that require one.
     * Called from p_init.
     */
    void setTempOutputTable(TempTableLimits* limits, const std::string tempTableName="temp");
    void setTempOutputLikeInputTable(TempTableLimits* limits);

    /**
     * Set up a single-column temp output table for DML executors that require one to return their counts.
     * Called from p_init.
     */
    void setDMLCountOutputTable(TempTableLimits* limits);

    // execution engine owns the plannode allocation.
    AbstractPlanNode* m_abstractNode;

    void setOutputTable(Table* table)
    {
        TempTable* asTemp = dynamic_cast<TempTable*>(table);
        if (asTemp) {
            m_output_table.setTempTable(asTemp);
        } else {
            TableCatalogDelegate* tcd = m_engine->getTableDelegate(table->name());
            m_output_table.setTableDelegate(tcd);
        }
    }

    void abandonOutputTable()
    {
        // This overrides both the TempTable and tcd pointers to prevent cleanup in ~AbstractExecutor.
        m_output_table.setTempTable(NULL);
    }

    Table* getOutputTable() const { return m_output_table.getTable(); }
    // Wrapping a protected member function in a protected static function allows it to be
    // accessed more flexibly in derived classes that need to apply it to more than "this".
    static Table* getOutputTableOf(const AbstractExecutor* not_this)
    { return not_this->getOutputTable(); }

    TempTable* getTempOutputTable() const { return m_output_table.getTempTable(); }

    // Input Tables
    // These tables are derived from the output of this node's children
    Table* getInputTable() const
    {
        assert(m_input_tables.size() == 1);
        return m_input_tables[0].getTable();
    }
    TempTable* getTempInputTable() const
    {
        assert(m_input_tables.size() == 1);
        return m_input_tables[0].getTempTable();
    }

    std::vector<TableReference> m_input_tables;

    /** reference to the engine to call up to the top end */
    VoltDBEngine* m_engine;
private:
    TableReference m_output_table;
};

}

#endif
