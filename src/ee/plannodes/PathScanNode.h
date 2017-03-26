/*
 * PathScan.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_PLANNODES_PATHSCANNODE_H_
#define SRC_EE_PLANNODES_PATHSCANNODE_H_

#include "plannodes/abstractplannode.h"

#include "expressions/abstractexpression.h"

namespace voltdb
{

class PathScanPlanNode : public AbstractPlanNode {
public:
	PathScanPlanNode();
	virtual ~PathScanPlanNode();

	PlanNodeType getPlanNodeType() const;
	std::string debugInfo(const std::string& spacer) const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);
};

}

#endif /* SRC_EE_PLANNODES_PATHSCANNODE_H_ */