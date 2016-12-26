package org.voltdb.plannodes;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetGraphScan;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.AbstractPlanNode.Members;
import org.voltdb.types.PlanNodeType;

public class PathScanPlanNode extends SeqScanPlanNode {

    public enum Members {
    	HINT,
    	STARTVERTEX,
    	ENDVERTEX;
    }
    
    String hint;
    int startvertexid;
    int endverexid;
	
    public PathScanPlanNode() {
        super();
    }
    
    public PathScanPlanNode(StmtTableScan tableScan) {
        super(tableScan);
        
        StmtTargetGraphScan graphScan = (StmtTargetGraphScan)tableScan;
        hint = graphScan.getHint();
        startvertexid = graphScan.getStartvertexid();
        endverexid = graphScan.getEndvertexid();
    }

    public PathScanPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
    }
    
    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PATHSCAN;
    }
    
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        
        stringer.key(Members.HINT.name()).value(hint);
        stringer.key(Members.STARTVERTEX.name()).value(startvertexid);
        stringer.key(Members.ENDVERTEX.name()).value(endverexid);
    }
}
