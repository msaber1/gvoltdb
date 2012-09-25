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

#include "projectionexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"

namespace voltdb {

bool ProjectionExecutor::p_init()
{
    VOLT_TRACE("init Projection Executor");
    assert(limits);

    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(m_abstractNode);
    assert(node);

    m_columnCount = static_cast<int>(node->getOutputSchema().size());

    // initialize local variables
    all_tuple_array_ptr = node->convertOutputIfAllTupleValues();
    all_tuple_array = all_tuple_array_ptr.get();
    all_param_array_ptr = node->convertOutputIfAllParameterValues();
    all_param_array = all_param_array_ptr.get();

    typedef AbstractExpression* ExpRawPtr;
    expression_array_ptr = boost::shared_array<ExpRawPtr>(new ExpRawPtr[m_columnCount]);
    expression_array = expression_array_ptr.get();
    for (int ctr = 0; ctr < m_columnCount; ctr++) {
        assert (node->getOutputColumnExpressions()[ctr] != NULL);
        expression_array_ptr[ctr] = node->getOutputColumnExpressions()[ctr];
    }

    if (!node->isInline()) {
        tuple = TableTuple(m_inputTable->schema());
    }
    return true;
}

bool ProjectionExecutor::p_execute(const NValueArray &params) {
#ifndef NDEBUG
    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(m_abstractNode);
#endif
    assert (node);
    assert (!node->isInline()); // inline projection's execute() should not be
                                // called

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());

    //
    // Since we have the input params, we need to call substitute to change any
    // nodes in our expression tree to be ready for the projection operations in
    // execute
    //
    assert (m_columnCount == (int)node->getOutputSchema().size());
    if (all_tuple_array == NULL && all_param_array == NULL) {
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            assert(expression_array[ctr]);
            expression_array[ctr]->substitute(params);
            VOLT_TRACE("predicate[%d]: %s", ctr,
                       expression_array[ctr]->debug(true).c_str());
        }
    }

    //
    // Now loop through all the tuples and push them through our output
    // expression This will generate new tuple values that we will insert into
    // our output table
    //
    TempTable* output_table = dynamic_cast<TempTable*>(m_outputTable);
    assert(output_table);
    TableIterator iterator = m_inputTable->iterator();
    assert (tuple.sizeInValues() == m_inputTable->columnCount());
    while (iterator.next(tuple)) {
        //
        // Project (or replace) values from input tuple
        //
        TableTuple &temp_tuple = output_table->tempTuple();
        if (all_tuple_array != NULL) {
            VOLT_TRACE("sweet, all tuples");
            for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, tuple.getNValue(all_tuple_array[ctr]));
            }
        } else if (all_param_array != NULL) {
            VOLT_TRACE("sweet, all params");
            for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, params[all_param_array[ctr]]);
            }
        } else {
            for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, expression_array[ctr]->eval(&tuple, NULL));
            }
        }
        output_table->TempTable::insertTuple(temp_tuple);
        /*if (!output_table->TempTable::insertTuple(temp_tuple)) {
            // TODO: DEBUG
            VOLT_ERROR("Failed to insert projection tuple from input table '%s' into output table '%s'", m_inputTable->name().c_str(), m_tmpOutputTable->name().c_str());
            return (false);
        }*/
    }

    //VOLT_TRACE("PROJECTED TABLE: %s\n", m_outputTable->debug().c_str());

    return (true);
}

ProjectionExecutor::~ProjectionExecutor() {
}

}
