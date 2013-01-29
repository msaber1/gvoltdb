/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include "tablecountexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "plannodes/tablecountnode.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool TableCountExecutor::p_init()
{
    VOLT_TRACE("init Table Count Executor");

    assert(dynamic_cast<TableCountPlanNode*>(m_abstractNode));
    assert(m_targetTable);

    assert(m_abstractNode->getOutputSchema().size() == 1);

    return true;
}

bool TableCountExecutor::p_execute()
{
#ifndef NDEBUG
    TableCountPlanNode* node = dynamic_cast<TableCountPlanNode*>(m_abstractNode);
#endif
    assert(node);
    assert(m_outputTable);
    assert ((int)m_outputTable->columnCount() == 1);

    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);

    assert(m_targetTable);
    VOLT_TRACE("Table Count table :\n %s",
               m_targetTable->debug().c_str());
    VOLT_DEBUG("Table Count table : %s which has %d active, %d"
               " allocated, %d used tuples",
               m_targetTable->name().c_str(),
               (int)m_targetTable->activeTupleCount(),
               (int)m_targetTable->allocatedTupleCount(),
               (int)m_targetTable->usedTupleCount());

    assert (node->getPredicate() == NULL);

    TableTuple& tmptup = output_temp_table->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( m_targetTable->activeTupleCount() ));
    output_temp_table->insertTempTuple(tmptup);

    //printf("Table count answer: %d", iterator.getSize());
    //printf("\n%s\n", m_outputTable->debug().c_str());
    VOLT_TRACE("\n%s\n", m_outputTable->debug().c_str());
    VOLT_DEBUG("Finished Table Counting");

    return true;
}

TableCountExecutor::~TableCountExecutor() {
}

