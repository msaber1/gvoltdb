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
#include "graph/GraphView.h"
#include "graph/Vertex.h"
#include "plannodes/PathScanNode.h"
#include "common/FatalException.hpp"
#include "executors/aggregateexecutor.h"
#include "executors/executorutil.h"
#include "execution/ProgressMonitorProxy.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "graph/PathIterator.h"

//#include "common/NValue.hpp"
//#include "common/ValuePeeker.hpp"
//#include "common/ValueFactory.hpp"

namespace voltdb {

bool PathScanExecutor::p_init(AbstractPlanNode *abstractNode,
                                TempTableLimits* limits)
{

	VOLT_TRACE("init PathScan Executor");

	PathScanPlanNode* node = dynamic_cast<PathScanPlanNode*>(abstractNode);
	assert(node);
	bool isSubquery = node->isSubQuery();
	assert(isSubquery || node->getTargetGraphView());
	assert((! isSubquery) || (node->getChildren().size() == 1));
	graphView = node->getTargetGraphView();
	graphView->fromVertexId = node->getStartVertexId();
	graphView->toVertexId = node->getEndVertexId();
	graphView->queryType = node->getQType();
	graphView->pathLength = node->getPathLength();
	graphView->topK = node->getTopK();
	graphView->vSelectivity = node->getVertexSelectivity();
	graphView->eSelectivity = node->getEdgeSelectivity();

	//
	// OPTIMIZATION: If there is no predicate for this SeqScan,
	// then we want to just set our OutputTable pointer to be the
	// pointer of our TargetTable. This prevents us from just
	// reading through the entire TargetTable and copying all of
	// the tuples. We are guarenteed that no Executor will ever
	// modify an input table, so this operation is safe
	//

	if (node->getPredicate() != NULL || node->getInlinePlanNodes().size() > 0) {
		// Create output table based on output schema from the plan
		const std::string& temp_name = (node->isSubQuery()) ?
				node->getChildren()[0]->getOutputTable()->name():
				graphView->getPathsTableName();
		setTempOutputTable(limits, temp_name);
		LogManager::GLog("PathScanExecutor", "p_init", 70, "after calling setTempOutputTable with temp table = " + temp_name);
	}
	//
	// Otherwise create a new temp table that mirrors the
	// output schema specified in the plan (which should mirror
	// the output schema for any inlined projection)
	//
	else {
		Table* temp_t = isSubquery ?
				 node->getChildren()[0]->getOutputTable() :
				 graphView->getPathTable();
		//Table* temp_t = setTempOutputTable(limits, PathScanPlanNode::pathsTableName);
		node->setOutputTable(temp_t);
		LogManager::GLog("PathScanExecutor", "p_init", 83, "after calling setOutputTable with temp table name = " + temp_t->name());
	}

	//node->setOutputTable(node->getTargetGraphView()->getVertexTable());
	// Inline aggregation can be serial, partial or hash
	m_aggExec = voltdb::getInlineAggregateExecutor(node);

	return true;
}

bool PathScanExecutor::p_execute(const NValueArray &params)
{
	PathScanPlanNode* node = dynamic_cast<PathScanPlanNode*>(m_abstractNode);
	assert(node);

	/*
	// Short-circuit an empty scan
	if (node->isEmptyScan()) {
		VOLT_DEBUG ("Empty Path Scan :\n %s", output_table->debug().c_str());
		return true;
	}
	*/

	GraphView* graphView = node->getTargetGraphView();
	Table* input_table = (node->isSubQuery()) ?
			node->getChildren()[0]->getOutputTable():
			graphView->getPathTable();


	//Table* output_table = dynamic_cast<TempTable*>(node->getOutputTable());
	//assert(output_table);
	//float cost = -1;

	//
	// OPTIMIZATION: NESTED PROJECTION
	//
	// Since we have the input params, we need to call substitute to
	// change any nodes in our expression tree to be ready for the
	// projection operations in execute
	//

	int num_of_columns = -1;

	ProjectionPlanNode* projection_node = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
	if (projection_node != NULL) {
		num_of_columns = static_cast<int> (projection_node->getOutputColumnExpressions().size());
	}

	//
	// OPTIMIZATION: NESTED LIMIT
	// How nice! We can also cut off our scanning with a nested limit!
	//

	LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));

	//
	// OPTIMIZATION:
	//
	// If there is no predicate and no Projection for this SeqScan,
	// then we have already set the node's OutputTable to just point
	// at the TargetTable. Therefore, there is nothing we more we need
	// to do here
	//

	if (node->getPredicate() != NULL || projection_node != NULL ||
		limit_node != NULL || m_aggExec != NULL)
	{

		//
		// Just walk through the table using our iterator and apply
		// the predicate to each tuple. For each tuple that satisfies
		// our expression, we'll insert them into the output table.
		//
		TableTuple tuple(input_table->schema());
		//TableIterator iterator =  input_table->iteratorDeletingAsWeGo();
		PathIterator iterator =  graphView->iteratorDeletingAsWeGo(GraphOperationType::ShortestPath);
		AbstractExpression *predicate = node->getPredicate();

		if (predicate)
		{
			VOLT_TRACE("SCAN PREDICATE :\n%s\n", predicate->debug(true).c_str());
		}

		int limit = CountingPostfilter::NO_LIMIT;
		int offset = CountingPostfilter::NO_OFFSET;
		if (limit_node) {
			limit_node->getLimitAndOffsetByReference(params, limit, offset);
		}
		// Initialize the postfilter
		CountingPostfilter postfilter(m_tmpOutputTable, predicate, limit, offset);

		ProgressMonitorProxy pmp(m_engine, this);
		TableTuple temp_tuple;
		assert(m_tmpOutputTable);
		if (m_aggExec != NULL) {
			const TupleSchema * inputSchema = input_table->schema();
			if (projection_node != NULL) {
				inputSchema = projection_node->getOutputTable()->schema();
			}
			temp_tuple = m_aggExec->p_execute_init(params, &pmp,
					inputSchema, m_tmpOutputTable, &postfilter);
		} else {
			temp_tuple = m_tmpOutputTable->tempTuple();
		}

		while (postfilter.isUnderLimit() && iterator.next(tuple))
		{
			VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
					   tuple.debug(input_table->name()).c_str(), tuple_ctr,
					   (int)input_table->activeTupleCount());
			pmp.countdownProgress();

			//
			// For each tuple we need to evaluate it against our predicate and limit/offset
			//
			//TODO: uncomment after fixing the path filter FE and EE code
			//if (postfilter.eval(&tuple, NULL))
			{
				//
				// Nested Projection
				// Project (or replace) values from input tuple
				//
				if (projection_node != NULL)
				{
					VOLT_TRACE("inline projection...");

					for (int ctr = 0; ctr < num_of_columns; ctr++) {
						//msaber: todo, need to check the projection operator construction

						NValue value = projection_node->getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
						temp_tuple.setNValue(ctr, value);
					}

					outputTuple(postfilter, temp_tuple);
				}
				else
				{
					outputTuple(postfilter, tuple);
				}
				pmp.countdownProgress();
			}
		}

		if (m_aggExec != NULL) {
			m_aggExec->p_execute_finish();
		}
	}
	//* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
	//* for debug */    " output table " << (void*)output_table <<
	//* for debug */    " put " << output_table->activeTupleCount() << " tuples " << std::endl;
	VOLT_TRACE("\n%s\n", node->getOutputTable()->debug().c_str());
	VOLT_DEBUG("Finished Seq scanning");

	return true;
}

void PathScanExecutor::outputTuple(CountingPostfilter& postfilter, TableTuple& tuple)
{
    if (m_aggExec != NULL) {
        m_aggExec->p_execute_tuple(tuple);
        return;
    }
    //
    // Insert the tuple into our output table
    //
    assert(m_tmpOutputTable);
    m_tmpOutputTable->insertTempTuple(tuple);
}

//void PathScanExecutor::setTempOutputTable(TempTableLimits* limits, const string tempTableName)
//{

	//m_abstractNode->setOutputTable(m_tmpOutputTable);
	/*
	assert(limits);
    TupleSchema* schema = m_abstractNode->generateTupleSchema();
    int column_count = schema->columnCount();
    std::vector<std::string> column_names(column_count);
    assert(column_count >= 1);
    const std::vector<SchemaColumn*>& outputSchema = m_abstractNode->getOutputSchema();

    for (int ctr = 0; ctr < column_count; ctr++) {
        column_names[ctr] = outputSchema[ctr]->getColumnName();
    }

    m_tmpOutputTable = TableFactory::getTempTable(m_abstractNode->databaseId(),
                                                              tempTableName,
                                                              schema,
                                                              column_names,
                                                              limits);
    m_abstractNode->setOutputTable(m_tmpOutputTable);
    */
//}


PathScanExecutor::~PathScanExecutor() {
	// TODO Auto-generated destructor stub
}

}

