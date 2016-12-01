/*
 * EdgeScan.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "EdgeScanNode.h"
#include "storage/table.h"

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
    buffer << "Graph Support";
    return buffer.str();
}

void EdgeScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{

}


}

