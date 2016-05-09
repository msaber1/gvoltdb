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

import java.util.HashMap;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QuantifierType;

/**
 * ComparisonExpression covers ExpressionTypes COMPARE_EQUAL, COMPARE_NOTEQUAL,
 * COMPARE_LESSTHAN, etc. as well as COMPARE_LIKE, COMPARE_IN, and
 * COMPARE_NOTDISTINCT. These are all binary operators with boolean results.
 * ComparisonExpression can also account for quantified comparisons such as
 * those modified by ANY and ALL keywords in SQL.
 */
public class ComparisonExpression extends AbstractExpression {
    private static final String QUANTIFIER_MEMBER_NAME = "QUANTIFIER";
    private QuantifierType m_quantifier = QuantifierType.NONE;

    public ComparisonExpression(ExpressionType type) {
        super(type);
        setValueType(VoltType.BOOLEAN);
    }

    public ComparisonExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
        setValueType(VoltType.BOOLEAN);
    }

    /*
     * This is needed for serialization
     */
    public ComparisonExpression() {
        super();
    }

    public void setQuantifier(QuantifierType quantifier) {
        m_quantifier = quantifier;
    }

    public QuantifierType getQuantifier() {
        return m_quantifier;
    }

    /**
     * All comparisons are binary operators.
     * @return true
     */
    @Override
    public boolean needsRightExpression() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) && obj instanceof ComparisonExpression) {
            return m_quantifier.equals(((ComparisonExpression)obj).m_quantifier);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + m_quantifier.hashCode();
    }

    @Override
    public Object clone() {
        ComparisonExpression clone = (ComparisonExpression) super.clone();
        clone.m_quantifier = m_quantifier;
        return clone;
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        super.loadFromJSONObject(obj);
        if (obj.has(QUANTIFIER_MEMBER_NAME)) {
            m_quantifier = QuantifierType.get(obj.getInt(QUANTIFIER_MEMBER_NAME));
        }
        else {
            m_quantifier = QuantifierType.NONE;
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_quantifier != QuantifierType.NONE) {
            stringer.key(QUANTIFIER_MEMBER_NAME).value(m_quantifier.getValue());
        }
    }

    /**
     * The map of reverse operators for comparison operators that HAVE a reversed form,
     * like "=" for "=", "<" for ">" etc.
     * Operators with no reverse syntax like "LIKE" have no entry in this map.
     * Note that this is simple syntactic reversal of arguments NOT logical negation,
     * so "=" does NOT map to "!=" and "<" does NOT map to ">=".
     */
    public static final Map<ExpressionType,ExpressionType> reverses = new HashMap<ExpressionType, ExpressionType>();
    static {
        reverses.put(ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_EQUAL);
        reverses.put(ExpressionType.COMPARE_NOTDISTINCT, ExpressionType.COMPARE_NOTDISTINCT);
        reverses.put(ExpressionType.COMPARE_NOTEQUAL, ExpressionType.COMPARE_NOTEQUAL);
        reverses.put(ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_GREATERTHAN);
        reverses.put(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_LESSTHAN);
        reverses.put(ExpressionType.COMPARE_LESSTHANOREQUALTO, ExpressionType.COMPARE_GREATERTHANOREQUALTO);
        reverses.put(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_LESSTHANOREQUALTO);
    }

    /**
     * Construct an equivalent comparison with its arguments reversed such as
     * "b < a" for "a > b" or "b = 1" for "1 = b".
     * @return the equivalent expression
     */
    public ComparisonExpression reverseOperator() {
        ExpressionType reverseType = reverses.get(this.m_type);
        // The caller is expected to have checked the map to ensure success.
        assert reverseType != null;
        // Left and right exprs are reversed on purpose
        ComparisonExpression reversed = new ComparisonExpression(reverseType, m_right, m_left);
        return reversed;
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        m_valueType = VoltType.BOOLEAN;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    /**
     * Construct the upper or lower bound expression that is implied by a prefix LIKE operator, given its required elements.
     * @param leftExpr - the LIKE operator's (and the result's) lhs expression
     * @param rangeComparator - a GTE or LT operator to indicate lower or upper bound, respectively,
     * @param comparand - a string operand value derived from the LIKE operator's rhs pattern
     * A helper for getGteFilterFromPrefixLike/getLtFilterFromPrefixLike
     **/
    static private ComparisonExpression rangeFilterFromPrefixLike(AbstractExpression leftExpr, ExpressionType rangeComparator, String comparand) {
        ConstantValueExpression cve = new ConstantValueExpression();
        cve.setValueType(VoltType.STRING);
        cve.setValue(comparand);
        cve.setValueSize(comparand.length());
        ComparisonExpression rangeFilter = new ComparisonExpression(rangeComparator, leftExpr, cve);
        return rangeFilter;
    }

    /**
     * Extract a prefix string from a prefix LIKE comparison's rhs pattern,
     * suitable for use as a lower bound constant.
     * Currently assumes the simple case of one final '%' wildcard character.
     * A helper for getGteFilterFromPrefixLike/getLtFilterFromPrefixLike
     **/
    private String extractLikePatternPrefix() {
        assert(getExpressionType() == ExpressionType.COMPARE_LIKE);
        ConstantValueExpression cve;
        if (m_right instanceof ParameterValueExpression) {
            ParameterValueExpression pve = (ParameterValueExpression)m_right;
            cve = pve.getOriginalValue();
        }
        else {
            assert(m_right instanceof ConstantValueExpression);
            cve = (ConstantValueExpression)m_right;
        }
        String pattern = cve.getValue();
        return pattern.substring(0, pattern.length()-1);
    }

    /**
     * Extract a modified prefix string from a prefix LIKE comparison's rhs pattern,
     * suitable for use as an upper bound constant.
     * Currently assumes the simple case of one final '%' wildcard character.
     * A helper for getLtFilterFromPrefixLike
     **/
    private String extractAndIncrementLikePatternPrefix() {
        String starter = extractLikePatternPrefix();
        // Right or wrong, this mimics what HSQL does for the case of " column LIKE prefix-pattern ".
        // It assumes that this last-sorting JAVA UTF-16 character maps to a suitably last-sorting UTF-8 string.
        String ender = starter + "\uffff";
        return ender;
    }

    /**
     * Construct the lower bound comparison filter implied by a prefix LIKE comparison.
     */
    public ComparisonExpression getGteFilterFromPrefixLike() {
        ExpressionType rangeComparator = ExpressionType.COMPARE_GREATERTHANOREQUALTO;
        String comparand = extractLikePatternPrefix();
        return rangeFilterFromPrefixLike(m_left, rangeComparator, comparand);
    }

    /**
     * Construct the upper bound comparison filter implied by a prefix LIKE comparison.
     */
    public ComparisonExpression getLtFilterFromPrefixLike() {
        ExpressionType rangeComparator = ExpressionType.COMPARE_LESSTHAN;
        String comparand = extractAndIncrementLikePatternPrefix();
        return rangeFilterFromPrefixLike(m_left, rangeComparator, comparand);
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        return "(" + m_left.explain(impliedTableName) +
            " " + type.symbol() + " " +
            (m_quantifier == QuantifierType.NONE ? "" :
                (m_quantifier.name() + " ")) +
            m_right.explain(impliedTableName) + ")";
    }

}
