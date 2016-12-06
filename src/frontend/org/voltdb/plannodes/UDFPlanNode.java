package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class UDFPlanNode extends AbstractPlanNode {

    public UDFPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.UDFPLANNODE;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
    }

    @Override
    public void resolveColumnIndexes()
    {

//    	TODO
    }

    @Override
    public void generateOutputSchema(Database db)
    {
    
//    	TODO
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "UDF";
    }

    @Override
    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }
}
