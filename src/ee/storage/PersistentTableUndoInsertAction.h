/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef PERSISTENTTABLEUNDOINSERTACTION_H_
#define PERSISTENTTABLEUNDOINSERTACTION_H_

#include "common/UndoAction.h"
#include "common/types.h"
#include "storage/persistenttable.h"

// Enable to debug.
//#define INSERT_ACTION_DEBUG_DUMP(stage) debugDump(stage)
#ifndef INSERT_ACTION_DEBUG_DUMP
#define INSERT_ACTION_DEBUG_DUMP(stage)
#endif

namespace voltdb {

class PersistentTableUndoInsertAction: public voltdb::UndoAction {
public:
    // Initialize with a table surgeon to get its private access to
    // the PersistentTable without an added friend declaration.
    PersistentTableUndoInsertAction(char* insertedTuple,
                                    PersistentTableSurgeon *surgeon)
        : m_tuple(insertedTuple)
        , m_tableSurgeon(surgeon)
    {
        INSERT_ACTION_DEBUG_DUMP("ctor ");
    }

    virtual ~PersistentTableUndoInsertAction() { }

    /*
     * Undo whatever this undo action was created to undo
     */
    virtual void undo() {
        INSERT_ACTION_DEBUG_DUMP("undo ");
        m_tableSurgeon->deleteTupleForUndo(m_tuple);
    }

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future.
     */
    virtual void release() {
        INSERT_ACTION_DEBUG_DUMP("rels ");
    }
private:
    void debugDump(const char* stage) const {
        std::cout << "DEBUG:InsAct " << stage << (void*)&(m_tableSurgeon->getTable())
                  << " currenttuples: " << m_tableSurgeon->getTable().activeTupleCount()
                  << ' ' << m_tableSurgeon->getTable().name()
                  << std::endl;
    }

    char* const m_tuple;
    PersistentTableSurgeon * const m_tableSurgeon;
};

}

#endif /* PERSISTENTTABLEUNDOINSERTACTION_H_ */
