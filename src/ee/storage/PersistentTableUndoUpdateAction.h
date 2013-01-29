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

#ifndef PERSISTENTTABLEUNDOUPDATEACTION_H_
#define PERSISTENTTABLEUNDOUPDATEACTION_H_

#include "common/UndoAction.h"

#include "common/tabletuple.h"
#include "storage/persistenttable.h"


namespace voltdb {

class PersistentTableUndoUpdateAction: public UndoAction {
public:

    PersistentTableUndoUpdateAction(char* oldTupleData, PersistentTable *table)
        : m_oldTupleData(oldTupleData), m_table(table), m_revertIndexes(false)
    { }

    void setNewTuple(char* newTupleData) {
        m_newTupleData = newTupleData;

        const TupleSchema *schema = m_table->schema();
        const uint16_t uninlineableObjectColumnCount = schema->getUninlinedObjectColumnCount();

        /*
         * Record which unlineableObjectColumns were updated so the
         * strings can be freed when this UndoAction is released or
         * undone.
         */
        if (uninlineableObjectColumnCount > 0) {
            TableTuple oldTuple(m_oldTupleData, schema);
            TableTuple newTuple(m_newTupleData, schema);
            for (uint16_t ii = 0; ii < uninlineableObjectColumnCount; ii++) {
                const uint16_t uninlineableObjectColumn = schema->getUninlinedObjectColumnInfoIndex(ii);
                const char * const *oPtr = reinterpret_cast<char* const*>
                  (oldTuple.getDataPtr(uninlineableObjectColumn));
                const char * const *nPtr = reinterpret_cast<char* const*>
                  (newTuple.getDataPtr(uninlineableObjectColumn));
                /*
                 * Only need to record the ones that are different and
                 * thus separate allocations.
                 */
                if (*oPtr != *nPtr) {
                    oldUninlineableColumns.push_back(*oPtr);
                    newUninlineableColumns.push_back(*nPtr);
                }
            }
        }
    }

    /*
     * Undo whatever this undo action was created to undo. In this
     * case the string allocations of the new tuple must be freed and
     * the tuple must be overwritten with the old one.
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future. In this case the string allocations
     * of the old tuple must be released.
     */
    void release();

    /**
     * After it has been decided to update the indexes the undo
     * quantum needs to be notified
     */
    inline void needToRevertIndexes() {
        m_revertIndexes = true;
    }

private:
    char* m_oldTupleData;
    char* m_newTupleData;
    PersistentTable *m_table;
    std::vector<const char*> oldUninlineableColumns;
    std::vector<const char*> newUninlineableColumns;
    bool m_revertIndexes;
};

}

#endif /* PERSISTENTTABLEUNDOUPDATEACTION_H_ */
