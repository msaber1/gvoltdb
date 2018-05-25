package org.voltdb.plannodes;

import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class VertexScanPlanNode extends SeqScanPlanNode {

    public VertexScanPlanNode() {
        super();
    }
    
    public VertexScanPlanNode(StmtTableScan tableScan) {
        super(tableScan);
    }

    public VertexScanPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
    }
    
    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.VERTEXSCAN;
    }
    
    @Override
    protected String explainPlanForNode(String indent) {
        String tableName = m_targetTableName == null? m_targetTableAlias: m_targetTableName;
        if (m_targetTableAlias != null && !m_targetTableAlias.equals(tableName)) {
            tableName += " (" + m_targetTableAlias +")";
        }
        return "VERTEXSCAN of \"" + tableName + "\"" + explainPredicate("\n" + indent + " filter by ");
    }
    
}
