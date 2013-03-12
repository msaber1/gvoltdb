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
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class NestLoopPlanNode extends AbstractJoinPlanNode {

    private enum Members {
        PREDICATE,
    }

    public NestLoopPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOP;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "NEST LOOP JOIN";
    }

    protected AbstractExpression m_predicate;

    /**
     * @return the predicate
     */
    public AbstractExpression getPredicate() {
        return m_predicate;
    }

    /**
     * @param predicate the predicate to set
     */
    public void setPredicate(List<AbstractExpression> joinClauses)
    {
        AbstractExpression predicate = ExpressionUtil.combine(joinClauses);
        if (predicate != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_predicate = (AbstractExpression) predicate.clone();
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        if (m_predicate != null) {
            m_predicate.validate();
        }
    }

    @Override
    public NodeSchema generateOutputSchema(Database db)
    {
        // First, assert that our topology is sane and then
        // recursively resolve all child/inline column indexes
        // Index join will have to override this method.
        assert(m_children.size()== 2);
        assert(null == getInlinePlanNode(PlanNodeType.INDEXSCAN));

        // FUTURE: At some point it would be awesome to further
        // cull the columns out of the join to remove columns that were only
        // used by scans/joins.  I think we can coerce HSQL into provide this
        // info relatively easily. --izzy

        NodeSchema outer_schema = m_children.get(0).generateOutputSchema(db);
        NodeSchema inner_schema = m_children.get(1).generateOutputSchema(db);

        // Finally, resolve m_predicate's column references.
        List<TupleValueExpression> predicate_tves = ExpressionUtil.getTupleValueExpressions(m_predicate);
        generateJoinSchema(outer_schema, inner_schema, predicate_tves);
        return m_outputSchema;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.PREDICATE.name()).value(m_predicate);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        if ( ! jobj.isNull(Members.PREDICATE.name())) {
            m_predicate = AbstractExpression.fromJSONObject(jobj.getJSONObject(Members.PREDICATE.name()), db);
        }
    }
}
