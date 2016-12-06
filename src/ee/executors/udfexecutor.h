#include "common/common.h"
#include "common/valuevector.h"
#include "executors/abstractexecutor.h"
#include "execution/VoltDBEngine.h"

namespace voltdb
{

    class UDFExecutor : public AbstractExecutor {
    public:
    	UDFExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
            : AbstractExecutor(engine, abstract_node)
        {}
    protected:
        bool p_init(AbstractPlanNode* abstract_node,
                    TempTableLimits* limits);
        bool p_execute(const NValueArray& params);
    private:
        int getUDFId();

    };
}
