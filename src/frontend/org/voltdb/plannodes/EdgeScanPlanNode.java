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
    
}
