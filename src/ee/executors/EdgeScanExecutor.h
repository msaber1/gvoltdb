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

namespace voltdb {

class AbstractExpression;
class TempTable;
class Table;

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
};

}
#endif /* SRC_EE_EXECUTORS_EDGESCANEXECUTOR_H_ */