/*
 * EdgeScan.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "EdgeScanNode.h"
//#include "storage/table.h"
#include "graph/GraphView.h"
#include "execution/VoltDBEngine.h"
#include "graph/GraphViewCatalogDelegate.h"

using namespace std;

namespace voltdb
{

EdgeScanPlanNode::EdgeScanPlanNode() {
	// TODO Auto-generated constructor stub

}

EdgeScanPlanNode::~EdgeScanPlanNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType EdgeScanPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_EDGESCAN; }

std::string EdgeScanPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << "EdgeScan PlanNode";
    return buffer.str();
}

GraphView* EdgeScanPlanNode::getTargetGraphView() const
{
	if (m_gcd == NULL)
	{
		return NULL;
	}
	return m_gcd->getGraphView();
}

void EdgeScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
	m_target_graph_name = obj.valueForKey("TARGET_GRAPH_NAME").asStr();

	m_isEmptyScan = obj.hasNonNullKey("PREDICATE_FALSE");

	// Set the predicate (if any) only if it's not a trivial FALSE expression
	if (!m_isEmptyScan)
	{
		m_predicate.reset(loadExpressionFromJSONObject("PREDICATE", obj));
	}

	m_isSubQuery = obj.hasNonNullKey("SUBQUERY_INDICATOR");

	if (m_isSubQuery) {
		m_gcd = NULL;
	} else
	{
		VoltDBEngine* engine = ExecutorContext::getEngine();
	    m_gcd = engine->getGraphViewDelegate(m_target_graph_name);
	    if ( ! m_gcd) {
	    	VOLT_ERROR("Failed to retrieve target graph view from execution engine for PlanNode '%s'",
	        debug().c_str());
	            //TODO: throw something
	    }
	    else
	    {
	    	LogManager::GLog("EdgeScanPlanNode", "loadFromJSONObject", 71, "Target graph view name = " + m_gcd->getGraphView()->name());
	    }
	}
}


}

