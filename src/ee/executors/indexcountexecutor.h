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


#ifndef HSTOREINDEXCOUNTEXECUTOR_H
#define HSTOREINDEXCOUNTEXECUTOR_H

#include "executors/abstractscanexecutor.h"

#include "boost/scoped_array.hpp"
#include <string>

namespace voltdb {

class AbstractExpression;

class IndexCountExecutor : public AbstractScanExecutor
{
public:
    IndexCountExecutor() { }
    ~IndexCountExecutor() { }

private:
    bool p_initMore(TempTableLimits* limits);
    bool p_execute();

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    std::string m_index_name;
    int m_num_of_search_keys;
    int m_num_of_end_keys;

    IndexLookupType m_lookupType;
    IndexLookupType m_endType;

    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::scoped_array<AbstractExpression*> m_search_key_array_ptr;
    boost::scoped_array<AbstractExpression*> m_end_key_array_ptr;

    AbstractExpression* m_countNULLExpr;

    StandAloneTupleStorage m_search_key;
    StandAloneTupleStorage m_end_key;

};

}

#endif // HSTOREINDEXCOUNTEXECUTOR_H
