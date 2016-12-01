/*
 * EdgeScan.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_PLANNODES_EDGESCANNODE_H_
#define SRC_EE_PLANNODES_EDGESCANNODE_H_

#include "plannodes/abstractplannode.h"

#include "expressions/abstractexpression.h"

namespace voltdb
{

class EdgeScanPlanNode : public AbstractPlanNode {
public:
	EdgeScanPlanNode();
	virtual ~EdgeScanPlanNode();

	PlanNodeType getPlanNodeType() const;
	std::string debugInfo(const std::string& spacer) const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);
};

}

#endif /* SRC_EE_PLANNODES_EDGESCANNODE_H_ */
