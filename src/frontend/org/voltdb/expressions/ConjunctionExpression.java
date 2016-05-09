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

package org.voltdb.expressions;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

/**
 * ConjunctionExpression includes ExpressionTypes for
 * logical AND and OR operators.
 */
public class ConjunctionExpression extends AbstractExpression {
    public ConjunctionExpression(ExpressionType type) {
        super(type);
        setValueType(VoltType.BOOLEAN);
    }

    public ConjunctionExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
        setValueType(VoltType.BOOLEAN);
    }

    /**
     * This constructor is needed for the first step of deserialization.
     *  Members are deserialized and set post-construction.
     */
    public ConjunctionExpression() {
        super();
    }

    /// Both AND and OR are binary operators.
    @Override
    public boolean needsRightExpression() {
        return true;
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        m_valueType = VoltType.BOOLEAN;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        return "(" + m_left.explain(impliedTableName) +
            " " + type.symbol() + " " +
            m_right.explain(impliedTableName) + ")";
    }
}
