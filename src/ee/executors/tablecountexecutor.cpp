/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "plannodes/tablecountnode.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"

#include <iostream>

using namespace voltdb;

bool TableCountExecutor::p_initMore(TempTableLimits* limits)
{
    VOLT_TRACE("init Table Count Executor");

    assert(dynamic_cast<TableCountPlanNode*>(m_abstractNode));
    assert(dynamic_cast<TableCountPlanNode*>(m_abstractNode)->isSubQuery() || getTargetTable());
    assert(dynamic_cast<TableCountPlanNode*>(m_abstractNode)->getPredicate() == NULL);
    assert(m_abstractNode->getOutputSchema().size() == 1);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    return true;
}

bool TableCountExecutor::p_execute()
{
    assert(dynamic_cast<TableCountPlanNode*>(m_abstractNode));

    TempTable* output_table = getTempOutputTable();
    assert(output_table);
    assert ((int)output_table->columnCount() == 1);

    int64_t rowCounts = 0;
    if (m_input_tables.size() > 0) {
        Table* input_table = m_input_tables[0].getTable();
        rowCounts = input_table->activeTupleCount();
    } else {
        PersistentTable* target_table = dynamic_cast<PersistentTable*>(getTargetTable());
        if ( ! target_table) {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "May not iterate a streamed table.");
        }
        VOLT_DEBUG("Table Count table : %s which has %d active, %d visible, %d allocated",
                   target_table->name().c_str(),
                   (int)target_table->activeTupleCount(),
                   (int)target_table->visibleTupleCount(),
                   (int)target_table->allocatedTupleCount());
        rowCounts = target_table->visibleTupleCount();
    }

    TableTuple& tmptup = output_table->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue(rowCounts));
    output_table->insertTempTuple(tmptup);

    VOLT_DEBUG("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Table Counting");
    return true;
}
