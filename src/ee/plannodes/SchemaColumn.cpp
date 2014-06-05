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

#include "SchemaColumn.h"

#include "expressions/abstractexpression.h"

using namespace std;

namespace voltdb {

SchemaColumn::SchemaColumn(PlannerDomValue colObject, int idx)
{
    if (colObject.hasKey("COLUMN_NAME")) {
        m_columnName = colObject.valueForKey("COLUMN_NAME").asStr();
    }
    else {
//        throw runtime_error("SchemaColumn::constructor missing column name.");
        char tmpName[6]; // 1024
        std::snprintf(tmpName, sizeof(tmpName), "C%d", idx);
        m_columnName = std::string(tmpName);
    }

    m_expression = NULL;
    // lazy vector search
    if (colObject.hasKey("EXPRESSION")) {
        PlannerDomValue columnExpressionValue = colObject.valueForKey("EXPRESSION");

        m_expression = AbstractExpression::buildExpressionTree(columnExpressionValue);
        assert(m_expression);
    }
}

SchemaColumn::~SchemaColumn()
{
    delete m_expression;
}

}
