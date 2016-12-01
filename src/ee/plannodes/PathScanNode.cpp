/*
 * PathScan.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "PathScanNode.h"
#include "storage/table.h"

using namespace std;

namespace voltdb
{

PathScanPlanNode::PathScanPlanNode() {
	// TODO Auto-generated constructor stub

}

PathScanPlanNode::~PathScanPlanNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType PathScanPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_PATHSCAN; }

std::string PathScanPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << "Graph Support";
    return buffer.str();
}

void PathScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{

}

}
