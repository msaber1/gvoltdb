/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#ifndef INDEXBASEDPERSISTENTTABLE_H
#define INDEXBASEDPERSISTENTTABLE_H

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"

namespace voltdb {

    class IndexBasedPersistentTable;

    class IndexTableUndoInsertAction: public voltdb::UndoAction {
    public:
        inline IndexTableUndoInsertAction(TableTuple insertedTuple,
                                          IndexBasedPersistentTable *table,
                                          Pool *pool)
        : m_tuple(insertedTuple), m_table(table)
        {
            void *tupleData = pool->allocate(m_tuple.tupleLength());
            m_tuple.move(tupleData);
            ::memcpy(tupleData, insertedTuple.address(), m_tuple.tupleLength());
        }

        virtual ~IndexTableUndoInsertAction();

        /*
         * Undo whatever this undo action was created to undo
         */
        void undo();

        /*
         * Release any resources held by the undo action. It will not need
         * to be undone in the future.
         */
        void release();
    private:
        voltdb::TableTuple m_tuple;
        IndexBasedPersistentTable *m_table;
    };

}

#endif // INDEXBASEDPERSISTENTTABLE_H
