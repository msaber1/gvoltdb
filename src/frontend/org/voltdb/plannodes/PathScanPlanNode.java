package org.voltdb.plannodes;

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
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
    
    /*
     * GVoltDB
     * Similar to one in AbstractScanPlanNode, but we don't want to change column indices. 
     * @see org.voltdb.plannodes.SeqScanPlanNode#resolveColumnIndexes()
     */
    @Override
    public void resolveColumnIndexes()
    {
        // predicate expression
        /* part from AbstractScanPlanNode
    	List<TupleValueExpression> predicate_tves =
            ExpressionUtil.getTupleValueExpressions(m_predicate);
        for (TupleValueExpression tve : predicate_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(m_tableSchema);
            
            System.out.println("PathScanPlanNode 64: "+m_tableSchema);
            
            tve.setColumnIndex(index);
        }
        */

        // inline projection
        ProjectionPlanNode proj =
            (ProjectionPlanNode)getInlinePlanNode(PlanNodeType.PROJECTION);
        if (proj != null)
        {
            proj.resolveColumnIndexesUsingSchema(m_tableSchema);
            m_outputSchema = proj.getOutputSchema().clone();
            m_hasSignificantOutputSchema = false; // It's just a cheap knock-off of the projection's
        }
        else
        {
            // output columns
            // if there was an inline projection we will have copied these already
            // otherwise we need to iterate through the output schema TVEs
            // and sort them by table schema index order.
            for (SchemaColumn col : m_outputSchema.getColumns())
            {
                // At this point, they'd better all be TVEs.
                assert(col.getExpression() instanceof TupleValueExpression);
                TupleValueExpression tve = (TupleValueExpression)col.getExpression();
                int index = tve.resolveColumnIndexesUsingSchema(m_tableSchema);
                
                System.out.println("PathScanPlanNode 91: "+m_tableSchema);
                
                tve.setColumnIndex(index);
            }
            m_outputSchema.sortByTveIndex();
        }

        // The outputschema of an inline limit node is completely irrelevant to the EE except that
        // serialization will complain if it contains expressions of unresolved columns.
        // Logically, the limited scan output has the same schema as the pre-limit scan.
        // It's at least as easy to just re-use the known-good output schema of the scan
        // than it would be to carefully resolve the limit node's current output schema.
        // And this simply works regardless of whether the limit was originally applied or inlined
        // before or after the (possibly inline) projection.
        // There's no need to be concerned about re-adjusting the irrelevant outputschema
        // based on the different schema of the original raw scan and the projection.
        LimitPlanNode limit = (LimitPlanNode)getInlinePlanNode(PlanNodeType.LIMIT);
        if (limit != null) {
            limit.m_outputSchema = m_outputSchema.clone();
            limit.m_hasSignificantOutputSchema = false; // It's just another cheap knock-off
        }

        // Resolve subquery expression indexes
        resolveSubqueryColumnIndexes();

        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(this);

        if (aggNode != null) {
            aggNode.resolveColumnIndexesUsingSchema(m_outputSchema);
            m_outputSchema = aggNode.getOutputSchema().clone();
            // Aggregate plan node change its output schema, and
            // EE does not have special code to get output schema from inlined aggregate node.
            m_hasSignificantOutputSchema = true;
        }
        
        //System.out.println("PathScanPlanNode 128: "+m_outputSchema);
    }
    
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        
        stringer.key(Members.HINT.name()).value(hint);
        stringer.key(Members.STARTVERTEX.name()).value(startvertexid);
        stringer.key(Members.ENDVERTEX.name()).value(endverexid);
    }
}
