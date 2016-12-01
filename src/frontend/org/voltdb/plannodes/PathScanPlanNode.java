package org.voltdb.plannodes;

import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class PathScanPlanNode extends SeqScanPlanNode {

    public PathScanPlanNode() {
        super();
    }
    
    public PathScanPlanNode(StmtTableScan tableScan) {
        super(tableScan);
    }

    public PathScanPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
    }
    
    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PATHSCAN;
    }
    
}
