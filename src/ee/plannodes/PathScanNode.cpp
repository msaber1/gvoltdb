/*
 * PathScan.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "PathScanNode.h"
#include "graph/GraphView.h"
#include "execution/VoltDBEngine.h"
#include "graph/GraphViewCatalogDelegate.h"

using namespace std;

namespace voltdb
{

PathScanPlanNode::PathScanPlanNode() {

}

PathScanPlanNode::~PathScanPlanNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType PathScanPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_PATHSCAN; }

std::string PathScanPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << "PathScan Plan Node";
    return buffer.str();
}

GraphView* PathScanPlanNode::getTargetGraphView() const
{
	if (m_gcd == NULL)
	{
		return NULL;
	}
	return m_gcd->getGraphView();
}

int PathScanPlanNode::getSPColumnIdInEdgesTable() const
{
	int id = -1;

	if (!this->getSPColumnName().empty())
	{
		GraphView* gv = this->getTargetGraphView();
		if (gv != NULL)
		{
			id = gv->getEdgeTable()->columnIndex(this->getSPColumnName());
		}
	}

	return id;
}

void PathScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
	m_target_graph_name = obj.valueForKey("TARGET_GRAPH_NAME").asStr();
	m_t_startVertexId = obj.valueForKey("STARTVERTEX").asInt();
	m_t_endVertexId = obj.valueForKey("ENDVERTEX").asInt();
	m_t_queryType = obj.valueForKey("PROP1").asInt();
	m_t_pathLength = obj.valueForKey("LENGTH").asInt(); //used to be PROP2 before
	m_t_topK = obj.valueForKey("PROP3").asInt();
	m_t_vSelectivity = obj.valueForKey("PROP4").asInt();
	m_t_eSelectivity = obj.valueForKey("PROP5").asInt();
	m_sp_column_name = "";

	if (obj.hasNonNullKey("HINT"))
	{
		m_sp_column_name = obj.valueForKey("HINT").asStr();
		if (!m_sp_column_name.empty())
		{
			std::size_t openParenthesesPosition  = m_sp_column_name.find("(");
			if (openParenthesesPosition !=  string::npos)
			{
				m_sp_column_name = m_sp_column_name.substr(openParenthesesPosition+1, m_sp_column_name.length()-openParenthesesPosition-2);
			}
		}
	}

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
			std::stringstream paramsToPrint;
			paramsToPrint << "Target graph view name = " << m_gcd->getGraphView()->name()
					<< ", StartVertexId = " << m_t_startVertexId << ", EndVertexId = " << m_t_endVertexId
					<< ", QType = " << m_t_queryType
					<< ", PLength = " << m_t_pathLength
					<< ", K = " << m_t_topK
					<< ", vSelectivity = " << m_t_vSelectivity
					<< ", eSelectivity = " << m_t_eSelectivity;

			LogManager::GLog("PathScanPlanNode", "loadFromJSONObject", 72, paramsToPrint.str());
		}
	}

}

}
