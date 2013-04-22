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

package org.voltdb.plannodes;

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.JoinType;

public abstract class AbstractJoinPlanNode extends AbstractPlanNode {

    private enum Members {
        JOIN_TYPE,
    }

    private JoinType m_joinType = JoinType.INNER;

    protected AbstractJoinPlanNode() {
        super();
    }

    /**
     * @return the join_type
     */
    public JoinType getJoinType() {
        return m_joinType;
    }

    /**
     * @param join_type the join_type to set
     */
    public void setJoinType(JoinType join_type) {
        m_joinType = join_type;
    }

    /**
     * @param outer_schema
     * @param inner_schema
     * @param predicate_tves
     */
    protected final void generateJoinSchema(NodeSchema outer_schema,
                                            NodeSchema inner_schema,
                                            List<TupleValueExpression> predicate_tves) {
        for (TupleValueExpression tve : predicate_tves) {
            int index = outer_schema.getIndexOfTve(tve);
            if (index == -1) {
                index = inner_schema.getIndexOfTve(tve);
                if (index == -1) {
                    throw new RuntimeException("Unable to find index for join TVE: " + tve);
                }
                //TODO: tve.setIsInner();
            }
            tve.setColumnIndex(index);
        }
        // Join the schema together to form the output schema.
        // The child Table order in the EE is in the plan child order (inline/inner child last).
        // We make the output schema ordered: [outer table columns][inner table columns]
        // I dislike this magically implied ordering, should be fixable --izzy
        m_outputSchema = outer_schema.joinViaTVEs(inner_schema);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.JOIN_TYPE.name()).value(m_joinType.toString());
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_joinType = JoinType.get( jobj.getString( Members.JOIN_TYPE.name() ) );
    }

}
