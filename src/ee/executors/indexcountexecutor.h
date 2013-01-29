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


#ifndef HSTOREINDEXCOUNTEXECUTOR_H
#define HSTOREINDEXSCANEXECUTOR_H

#include "executors/abstracttableioexecutor.h"

#include "common/tabletuple.h"

namespace voltdb {

class IndexCountExecutor : public AbstractTableIOExecutor
{
public:
    IndexCountExecutor() { /* Do nothing. */ }
    ~IndexCountExecutor();

protected:
    bool p_init();
    bool p_execute();

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    int m_numOfColumns;
    int m_numOfStartKeys;
    int m_numOfEndKeys;

    // Search key
    StorageBackedTempTuple m_startKey;
    StorageBackedTempTuple m_endKey;

    IndexLookupType m_startType;
    IndexLookupType m_endType;

    TableIndex *m_index;
};

}

#endif
