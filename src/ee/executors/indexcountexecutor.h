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

#include "executors/abstractexecutor.h"

#include "common/tabletuple.h"
#include "boost/shared_array.hpp"

namespace voltdb {
class AbstractExpression;
class IndexCountPlanNode;

class IndexCountExecutor : public AbstractTableIOExecutor
{
public:
    IndexCountExecutor()
        : m_searchKeyBackingStore(NULL), m_endKeyBackingStore(NULL)
    {}
    ~IndexCountExecutor();

protected:
    bool p_init();
    bool p_execute(const NValueArray &params);

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    IndexCountPlanNode *m_node;
    int m_numOfColumns;
    int m_numOfSearchkeys;
    int m_numOfEndkeys;

    // Search key
    TableTuple m_searchKey;
    TableTuple m_endKey;
    // search_key_beforesubstitute_array_ptr[]
    AbstractExpression** m_searchKeyBeforeSubstituteArray;
    AbstractExpression** m_endKeyBeforeSubstituteArray;

    IndexLookupType m_lookupType;
    IndexLookupType m_endType;

    // IndexCount Information
    TempTable* m_outputTable;
    PersistentTable* m_targetTable;
    TableIndex *m_index;

    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::shared_array<AbstractExpression*>
        m_searchKeyBeforeSubstituteArrayPtr;
    boost::shared_array<AbstractExpression*>
            m_endKeyBeforeSubstituteArrayPtr;

    // So Valgrind doesn't complain:
    char* m_searchKeyBackingStore;
    char* m_endKeyBackingStore;
};

}

#endif
