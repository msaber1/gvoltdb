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

#include "SchemaColumn.h"
#include "expressions/abstractexpression.h"

using namespace json_spirit;
using namespace std;
using namespace voltdb;

SchemaColumn::SchemaColumn(Object& colObject)
{
    bool contains_table_name = false;
    bool contains_column_name = false;
    bool contains_type = false;
    bool contains_size = false;
    for (int attr = 0; attr < colObject.size(); attr++)
    {
        if (colObject[attr].name_ == "TABLE_NAME")
        {
            contains_table_name = true;
            m_tableName = colObject[attr].value_.get_str();
        }
        else if (colObject[attr].name_ == "COLUMN_NAME")
        {
            contains_column_name = true;
            m_columnName = colObject[attr].value_.get_str();
        }
        else if (colObject[attr].name_ == "TYPE")
        {
            contains_type = true;
            string colObjectTypeString = colObject[attr].value_.get_str();
            m_type = stringToValue(colObjectTypeString);
        }
        else if (colObject[attr].name_ == "SIZE")
        {
            contains_size = true;
            m_size = colObject[attr].value_.get_int();
        }
    }

    m_expression = NULL;
    // lazy vector search

    Value columnExpressionValue = find_value(colObject, "EXPRESSION");
    if (columnExpressionValue == Value::null)
    {
        throw runtime_error("SchemaColumn::constructor: "
                            "Can't find EXPRESSION value");
    }

    Object columnExpressionObject = columnExpressionValue.get_obj();
    m_expression = AbstractExpression::buildExpressionTree(columnExpressionObject);

    if(!(contains_table_name && contains_column_name && contains_type && contains_size)) {
        throw runtime_error("SchemaColumn::constructor missing configuration data.");
    }
}

SchemaColumn::~SchemaColumn()
{
    delete m_expression;
}

string
SchemaColumn::getTableName() const
{
    return m_tableName;
}

string
SchemaColumn::getColumnName() const
{
    return m_columnName;
}

ValueType
SchemaColumn::getType() const
{
    return m_type;
}

int32_t
SchemaColumn::getSize() const
{
    return m_size;
}

AbstractExpression*
SchemaColumn::getExpression()
{
    return m_expression;
}
