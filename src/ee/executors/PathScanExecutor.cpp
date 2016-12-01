/*
 * PathScanExecutor.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "PathScanExecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "logging/LogManager.h"

namespace voltdb {

bool PathScanExecutor::p_init(AbstractPlanNode *abstractNode,
                                TempTableLimits* limits)
{
    return true;
}

bool PathScanExecutor::p_execute(const NValueArray &params)
{
    return true;
}

PathScanExecutor::~PathScanExecutor() {
	// TODO Auto-generated destructor stub
}

}

