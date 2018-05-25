/*
 * NestedLoopPathExecutor.h
 *
 *  Created on: May 1, 2018
 *      Author: mohamed
 */

#ifndef SRC_EE_EXECUTORS_NESTEDLOOPPATHEXECUTOR_H_
#define SRC_EE_EXECUTORS_NESTEDLOOPPATHEXECUTOR_H_

#include "common/common.h"
#include "common/valuevector.h"
#include "executors/abstractjoinexecutor.h"
#include "expressions/comparisonexpression.h"

namespace voltdb {

class GraphView;
class PathScanPlanNode;

class NestedLoopPathExecutor : public AbstractJoinExecutor {
public:
	NestedLoopPathExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) :
            AbstractJoinExecutor(engine, abstract_node) {
        	LogManager::GLog("NestedLoopPathExecutor", "Constructor", 22, abstract_node->debug());
        }
    private:

        bool p_init(AbstractPlanNode*, TempTableLimits* limits);
        bool p_execute(const NValueArray &params);
        void setStartAndEndVertexes(const AbstractExpression* joinExpression, const Table* inner, const Table* outer);
        int getQueryType();

        GraphView* graphView;
        PathScanPlanNode* pathScanNode;
        const int UNDEFINED = -1;
        int startVertexColumnId = UNDEFINED, endVertexColumnId = UNDEFINED;
        int startVertexId, endVertexId;
        const string StartVertexLiteral = "STARTVERTEXID";
        const string EndVertexLiteral = "ENDVERTEXID";
};

}

#endif /* SRC_EE_EXECUTORS_NESTEDLOOPPATHEXECUTOR_H_ */
