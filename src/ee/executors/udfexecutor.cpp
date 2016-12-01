#include "udfexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/tuplescannode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "plannodes/udfplannode.h"

using namespace voltdb;

bool UDFExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init UDF Executor");

    return true;
}

int UDFExecutor::getUDFId(){
//	Get the id of which instance of the the udf is to be invoked
//	Hardcoding fr now
	return 21024;
}

bool UDFExecutor::p_execute(const NValueArray &params) {

//	TODO : fetch UDF id and invoke the method on that id
    return true;
}
