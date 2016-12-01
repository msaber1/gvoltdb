#include "udfplannode.h"

#include "storage/table.h"

using namespace std;

namespace voltdb {

UDFPlanNode::~UDFPlanNode() { }

PlanNodeType UDFPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_UDF; }

std::string UDFPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << spacer << "UDF Output]:\n";
    return buffer.str();
}

void UDFPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
//    TODO
}

} // namespace voltdb
