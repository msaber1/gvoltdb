/*
 * VertexScanExecutor.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_EXECUTORS_VERTEXSCANEXECUTOR_H_
#define SRC_EE_EXECUTORS_VERTEXSCANEXECUTOR_H_

#include <vector>
#include "boost/shared_array.hpp"
#include "common/common.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"
#include "execution/VoltDBEngine.h"

namespace voltdb {

class AbstractExpression;
class TempTable;
class Table;
class AggregateExecutorBase;
struct CountingPostfilter;

class VertexScanExecutor : public AbstractExecutor {

public:
	VertexScanExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
		: AbstractExecutor(engine, abstract_node)
		  , m_aggExec(NULL)
	{
         //output_table = NULL;
         LogManager::GLog("VertexScanExecutor", "Constructor", 32, abstract_node->debug());
    }
        ~VertexScanExecutor();
    protected:
        bool p_init(AbstractPlanNode*,
                    TempTableLimits* limits);
        bool p_execute(const NValueArray &params);

    private:
				// void aa(long destinationID, vector<int> vertexIDs, vector<string> attrNames, GraphView* graphView);
				// void qq(Table* table, long destinationID, vector<int> vertexIDs, vector<string> attrNames, GraphView* graphView);
				// void zz(int a);
        void outputTuple(CountingPostfilter& postfilter, TableTuple& tuple);
        AggregateExecutorBase* m_aggExec;


};

}
#endif /* SRC_EE_EXECUTORS_VERTEXSCANEXECUTOR_H_ */
