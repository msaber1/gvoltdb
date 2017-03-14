/*
 * EdgeScanExecutor.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_EXECUTORS_EDGESCANEXECUTOR_H_
#define SRC_EE_EXECUTORS_EDGESCANEXECUTOR_H_

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
class TempTableLimits;
class AggregateExecutorBase;
class GraphView;
struct CountingPostfilter;

class EdgeScanExecutor : public AbstractExecutor {
public:
	EdgeScanExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) : AbstractExecutor(engine, abstract_node) {
            //output_table = NULL;
            LogManager::GLog("EdgeScanExecutor", "Constructor", 28, abstract_node->debug());
        }
        ~EdgeScanExecutor();
    protected:
        bool p_init(AbstractPlanNode*,
                    TempTableLimits* limits);
        bool p_execute(const NValueArray &params);

    private:
        void outputTuple(CountingPostfilter& postfilter, TableTuple& tuple);
        AggregateExecutorBase* m_aggExec;
        GraphView* graphView;
};

}
#endif /* SRC_EE_EXECUTORS_EDGESCANEXECUTOR_H_ */
