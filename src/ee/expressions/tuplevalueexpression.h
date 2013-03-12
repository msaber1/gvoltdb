/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef HSTORETUPLEVALUEEXPRESSION_H
#define HSTORETUPLEVALUEEXPRESSION_H

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

#include <string>
#include <sstream>

namespace voltdb {

class SerializeInput;
class SerializeOutput;

class TupleValueExpression : public AbstractExpression {
public:
    TupleValueExpression(int value_idx, std::string table_name, std::string col_name, bool is_inner = false)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE)
        , m_valueIdx(value_idx)
        , m_isInner(is_inner)
        , m_tableName(table_name)
        , m_columnName(col_name)
    {
        VOLT_TRACE("TupleValueExpression %d %d %s", m_type, m_valueIdx, (m_isInner ? "inner" : "outer") );
    }

    virtual voltdb::NValue eval(const TableTuple *outer_tuple, const TableTuple *inner_tuple) const
    {
        const TableTuple *the_tuple = outer_tuple;
        if (m_isInner) {
            assert(inner_tuple);
            if ( ! inner_tuple ) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL, "TupleValueExpression::eval:"
                                              " Couldn't find inner tuple (possible index scan planning error)");
            }
            the_tuple = inner_tuple;
        }
        else {
            assert(outer_tuple);
            if ( ! outer_tuple ) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL, "TupleValueExpression::eval:"
                                              " Couldn't find tuple (possible index scan planning error)");
            }
        }
        return the_tuple->getNValue(m_valueIdx);
    }

    std::string debugInfo(const std::string &spacer) const
    {
        std::ostringstream buffer;
        buffer << spacer << "Column Reference[" << m_valueIdx
               << (m_isInner ? " of inner" : " of outer" ) << " tuple] "
               << m_tableName << "." << m_columnName << std::endl;
        return (buffer.str());
    }

    int getColumnId() const {return m_valueIdx;}

private:
    const int m_valueIdx;           // which (offset) column of the tuple
    const int m_isInner;            // which tuple. defaults to the outer (or only) one
    const std::string m_tableName;
    const std::string m_columnName;
};

}
#endif
