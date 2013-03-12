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

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.types.PlanNodeType;

public class TableCountPlanNode extends AbstractScanPlanNode {

    public TableCountPlanNode() {
        super();
    }

    public TableCountPlanNode(AbstractScanPlanNode child, AggregatePlanNode apn) {
        super();
        // The main point of the output schema is its type (BIGINT) and alias.
        // The apn's originally assigned output schema is fine for that.
        // It doesn't matter whether the internals of apn/child have been normalized via generateOutputSchema().
        // TODO: In this context, the output schema may drag along a count expression which will get needlessly serialized to the EE.
        // The solution would involve the EE deserializer accepting ColumnSchema with missing expressions.
        // Then cases like this in which the expression does not matter it could be set null.
        m_outputSchema = apn.getOutputSchema();
        m_estimatedOutputTupleCount = 1;
        m_targetTableAlias = child.getTargetTableAlias();
        m_targetTableName = child.getTargetTableName();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.TABLECOUNT;
    }

    @Override
    public NodeSchema generateOutputSchema(Database db) { return m_outputSchema; }

    @Override
    public void computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        m_estimatedOutputTupleCount = 1;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "TABLE COUNT of \"" + m_targetTableName + "\"";
    }

}
