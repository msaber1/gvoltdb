#include "abstractplannode.h"

namespace voltdb {

/**
 *
 */
class UDFPlanNode : public AbstractPlanNode {
public:
	UDFPlanNode(){}
    ~UDFPlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;


protected:
    void loadFromJSONObject(PlannerDomValue obj);

};

} // namespace voltdb
