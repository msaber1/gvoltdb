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

import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class NestLoopIndexPlanNode extends AbstractJoinPlanNode {

    public NestLoopIndexPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOPINDEX;
    }

    @Override
    public NodeSchema generateOutputSchema(Database db)
    {
        // Important safety tip regarding this inlined
        // index scan and ITS inlined projection:
        // That projection is currently only used/usable as
        // a means to narrow the set of columns from the
        // indexscan's target table that make it into the
        // rest of the plan.  the expressions that are
        // given to the projection are currently not ever used
        assert(m_children.size() == 1);
        NodeSchema outer_schema = m_children.get(0).generateOutputSchema(db);
        IndexScanPlanNode inlineScan = (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert(inlineScan != null);
        NodeSchema inner_schema = inlineScan.generateOutputSchema(db);
        // The child subplan's output is the outer table
        // The inlined node's output is the inner table

        // Resolve all columns in m_predicate, the end expression and the search key expressions.
        // ALL of these are defined on the inner child node in spite of the fact that they may
        // reference both sides of the join.
        List<TupleValueExpression> predicate_tves = ExpressionUtil.getTupleValueExpressions(inlineScan.getPredicate());
        predicate_tves.addAll(ExpressionUtil.getTupleValueExpressions(inlineScan.getEndExpression()));
        for (AbstractExpression search_exp : inlineScan.getSearchKeyExpressions()) {
            predicate_tves.addAll(ExpressionUtil.getTupleValueExpressions(search_exp));
        }

        generateJoinSchema(outer_schema, inner_schema, predicate_tves);

        return m_outputSchema;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Check that we have an inline IndexScanPlanNode
        if (m_inlineNodes.isEmpty()) {
            throw new Exception("ERROR: No inline PlanNodes are set for " + this);
        } else if (!m_inlineNodes.containsKey(PlanNodeType.INDEXSCAN)) {
            throw new Exception("ERROR: No inline PlanNode with type '" + PlanNodeType.INDEXSCAN + "' was set for " + this);
        }
    }

    /**
     * Does the (sub)plan guarantee an identical result/effect when "replayed"
     * against the same database state, such as during replication or CL recovery.
     * @return
     */
    @Override
    public boolean isOrderDeterministic() {
        if ( ! super.isOrderDeterministic()) {
            return false;
        }
        IndexScanPlanNode index_scan =
            (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(index_scan != null);
        if ( ! index_scan.isOrderDeterministic()) {
            m_nondeterminismDetail = index_scan.m_nondeterminismDetail;
            return false;
        }
        return true;
    }


    @Override
    protected String explainPlanForNode(String indent) {
        return "NESTLOOP INDEX JOIN";
    }
}
