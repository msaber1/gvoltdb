/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "windowtable.h"
#include "storage/ConstraintFailureException.h"

namespace voltdb {
WindowTable::WindowTable(int partitionColumn, const char *signature,
        bool isTupleBased, int rowLimit, int timeLimit, int slideSize) :
        PersistentTable(partitionColumn, signature, false, 0, INT32_MAX, false) {
    m_isTupleBased = isTupleBased;
    m_rowLimit = rowLimit;
    m_timeLimit = timeLimit;
    m_slideSize = slideSize;
}

WindowTable::~WindowTable() {
}

// ------------------------------------------------------------------
// OPERATIONS
// ------------------------------------------------------------------
/*
 Insert a tuple into the window table.
 */

bool WindowTable::insertWindowTuple(TableTuple &source) {
    if (visibleTupleCount() >= m_tupleLimit) {
        char buffer[256];
        snprintf(buffer, 256, "Table %s exceeds table maximum row count %d",
                m_name.c_str(), m_tupleLimit);
        throw ConstraintFailureException(this, source, buffer);
    }
    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    TableTuple target(m_schema);
    PersistentTable::nextFreeTuple(&target);

    //
    // Then copy the source into the target
    //
    target.copyForPersistentInsert(source); // tuple in freelist must be already cleared

    try {
        insertTupleCommon(source, target, false);
    } catch (ConstraintFailureException &e) {
        deleteTupleStorage(target); // also frees object columns
        throw;
    }

    m_windowTupleQueue.push(target);

    return true;
}

bool WindowTable::isWindowTableFull() {
    if (m_isTupleBased) {
        return (m_tupleCount > m_rowLimit);
    } else {
        return false; // TODO time based window
    }
}

TableTuple WindowTable::popWindowTuple() {
    TableTuple& staleTuple = m_windowTupleQueue.front();
    //pop out of queue
    m_windowTupleQueue.pop();

    // copy the tuple to standalone storage
    if (!m_setupTempSchema) {
        m_templateTuple.init(m_schema);
        m_setupTempSchema = true;
    }
    TableTuple tempTuple = const_cast<TableTuple&>(m_templateTuple.tuple());
    tempTuple.copy(staleTuple);

    //remove from persistent table
    deleteWindowTuple(staleTuple);

    return tempTuple;
}

bool WindowTable::deleteWindowTuple(TableTuple &target) {
    PersistentTable::deleteTuple(target, false);
    return true;
}

std::string WindowTable::tableType() const {
    return "WindowTable";
}

std::string WindowTable::debugWindowTupleQueue() {
    std::ostringstream buffer;
    buffer << this << " m_windowTupleQueue is tupleBased " << m_isTupleBased << endl;

    buffer << "There are currently " << m_windowTupleQueue.size() << endl;
    buffer << "There should be " << m_tupleCount<< " Visible "<< visibleTupleCount() << endl;
    // buffer << " Front is: " << m_windowTupleQueue.front().toJsonArray() << endl;

    // buffer  << " Back is: " << m_windowTupleQueue.back().toJsonArray() << endl;

    return buffer.str();
}

}
