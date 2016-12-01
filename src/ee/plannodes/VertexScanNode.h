/*
 * VertexScan.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_PLANNODES_VERTEXSCANNODE_H_
#define SRC_EE_PLANNODES_VERTEXSCANNODE_H_

#include "plannodes/abstractplannode.h"

#include "expressions/abstractexpression.h"

namespace voltdb
{

class GraphViewCatalogDelegate;

class VertexScanPlanNode : public AbstractPlanNode {
public:
	VertexScanPlanNode();
	virtual ~VertexScanPlanNode();

	PlanNodeType getPlanNodeType() const;
	std::string debugInfo(const std::string& spacer) const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);

    std::string m_target_graph_name;
	GraphViewCatalogDelegate* m_gcd;
	//
	// This is the predicate used to filter out tuples during the scan
	//
	boost::scoped_ptr<AbstractExpression> m_predicate;
	// True if this scan represents a sub query
	bool m_isSubQuery;
	// True if this scan has a predicate that always evaluates to FALSE
	bool m_isEmptyScan;
};

}

#endif /* SRC_EE_PLANNODES_VERTEXSCANNODE_H_ */
