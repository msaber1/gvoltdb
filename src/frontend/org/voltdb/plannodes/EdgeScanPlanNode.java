package org.voltdb.plannodes;

import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class EdgeScanPlanNode extends SeqScanPlanNode {

    public EdgeScanPlanNode() {
        super();
    }
    
    public EdgeScanPlanNode(StmtTableScan tableScan) {
        super(tableScan);
    }

    public EdgeScanPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
    }
    
    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.EDGESCAN;
    }
    
    @Override
    protected String explainPlanForNode(String indent) {
        String tableName = m_targetTableName == null? m_targetTableAlias: m_targetTableName;
        if (m_targetTableAlias != null && !m_targetTableAlias.equals(tableName)) {
            tableName += " (" + m_targetTableAlias +")";
        }
        return "SEQUENTIAL EDGESCAN of \"" + tableName + "\"" + explainPredicate("\n" + indent + " filter by ");
    }
    
}
