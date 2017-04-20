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

class GraphViewCatalogDelegate;
class GraphView;

class PathScanPlanNode : public AbstractPlanNode {
public:
	PathScanPlanNode();
	virtual ~PathScanPlanNode();

	PlanNodeType getPlanNodeType() const;
	std::string debugInfo(const std::string& spacer) const;

	GraphView* getTargetGraphView() const;
	void setTargetGraphViewDelegate(GraphViewCatalogDelegate* gcd) { m_gcd = gcd; } // DEPRECATED?

	std::string getTargetGraphViewName() const { return m_target_graph_name; } // DEPRECATED?
	AbstractExpression* getPredicate() const { return m_predicate.get(); }
	int getStartVertexId() const { return m_t_startVertexId; }
	int getEndVertexId() const { return m_t_endVertexId; }
	int getQType() const { return m_t_queryType; }
	int getPathLength() const { return m_t_pathLength; }
	int getTopK() const { return m_t_topK; }
	int getVertexSelectivity() const { return m_t_vSelectivity; }
	int getEdgeSelectivity() const { return m_t_eSelectivity; }

	bool isSubQuery() const { return m_isSubQuery; }

	bool isEmptyScan() const { return m_isEmptyScan; }

protected:
    void loadFromJSONObject(PlannerDomValue obj);

    std::string m_target_graph_name;
	GraphViewCatalogDelegate* m_gcd;
	int m_t_startVertexId = -1;
	int m_t_endVertexId = -1;
	int m_t_queryType = -1; //prop1
	int m_t_pathLength= -1; //prop2
	int m_t_topK= -1; //prop3
	int m_t_vSelectivity= -1; //prop4
	int m_t_eSelectivity= -1; //prop5
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

#endif /* SRC_EE_PLANNODES_PATHSCANNODE_H_ */
