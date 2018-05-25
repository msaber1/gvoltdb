/*
 * NestedLoopPathPlanNode.h
 *
 *  Created on: Apr 30, 2018
 *      Author: mohamed
 */

#ifndef SRC_EE_PLANNODES_NESTEDLOOPPATHNODE_H_
#define SRC_EE_PLANNODES_NESTEDLOOPPATHNODE_H_

#include "abstractjoinnode.h"

namespace voltdb
{

class NestedLoopPathNode : public AbstractJoinPlanNode {
public:
	NestedLoopPathNode();
	~NestedLoopPathNode();
	PlanNodeType getPlanNodeType() const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);
};

}

#endif /* SRC_EE_PLANNODES_NESTEDLOOPPATHNODE_H_ */
