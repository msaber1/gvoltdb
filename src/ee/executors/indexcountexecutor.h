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


#ifndef HSTOREINDEXCOUNTEXECUTOR_H
#define HSTOREINDEXSCANEXECUTOR_H

#include "executors/abstracttableioexecutor.h"

#include "common/tabletuple.h"

namespace voltdb {
class AbstractExpression;
class IndexCountPlanNode;

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
    int m_numOfSearchkeys;
    int m_numOfEndkeys;

    // Search key
    StorageBackedTempTuple m_searchKey;
    StorageBackedTempTuple m_endKey;

    IndexLookupType m_lookupType;
    IndexLookupType m_endType;

    TableIndex *m_index;
};

}

#endif
