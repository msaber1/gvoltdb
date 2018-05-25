/*
 * NestedLoopPathPlanNode.cpp
 *
 *  Created on: Apr 30, 2018
 *      Author: mohamed
 */

#include "NestedLoopPathNode.h"

namespace voltdb {


NestedLoopPathNode::~NestedLoopPathNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType NestedLoopPathNode::getPlanNodeType() const { return PLAN_NODE_TYPE_NESTEDLOOPPATH; }

void NestedLoopPathNode::loadFromJSONObject(PlannerDomValue obj)
{
	AbstractJoinPlanNode::loadFromJSONObject(obj);
	//m_target_graph_name = obj.valueForKey("TARGET_GRAPH_NAME").asStr();
}

}

