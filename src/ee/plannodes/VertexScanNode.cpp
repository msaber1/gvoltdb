/*
 * VertexScan.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "storage/table.h"
#include "VertexScanNode.h"

using namespace std;

namespace voltdb
{

VertexScanPlanNode::VertexScanPlanNode() {
	// TODO Auto-generated constructor stub

}

VertexScanPlanNode::~VertexScanPlanNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType VertexScanPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_VERTEXSCAN; }

std::string VertexScanPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << "Graph Support";
    return buffer.str();
}

void VertexScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{

}

}
