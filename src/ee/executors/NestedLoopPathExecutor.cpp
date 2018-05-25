/*
 * NestedLoopPathExecutor.cpp
 *
 *  Created on: May 1, 2018
 *      Author: mohamed
 */

#include "NestedLoopPathExecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "executors/aggregateexecutor.h"
#include "executors/executorutil.h"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"
#include "plannodes/nestloopnode.h"
#include "plannodes/limitnode.h"
#include "plannodes/aggregatenode.h"

#include <vector>
#include <string>
#include <stack>


#include "logging/LogManager.h"
#include "graph/GraphView.h"
#include "graph/Vertex.h"
#include "plannodes/PathScanNode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "graph/PathIterator.h"

#ifdef VOLT_DEBUG_ENABLED
#include <ctime>
#include <sys/times.h>
#include <unistd.h>
#endif

using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

bool NestedLoopPathExecutor::p_init(AbstractPlanNode* abstractNode,
                                   TempTableLimits* limits)
{
	LogManager::GLog("NestedLoopPathExecutor", "p_init", 46, "");

    VOLT_TRACE("init NLJ Executor");
    assert(limits);

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(m_abstractNode);
    assert(node);

    // Init parent first
    if (!AbstractJoinExecutor::p_init(abstractNode, limits)) {
        return false;
    }

    // NULL tuples for left and full joins
    p_init_null_tuples(node->getInputTable(), node->getInputTable(1));

    graphView = NULL;
    pathScanNode = NULL;
    startVertexColumnId = UNDEFINED;
    endVertexColumnId = UNDEFINED;

    vector<AbstractPlanNode*> children = abstractNode->getChildren();
    if (children.size() == 2)
    {
    	pathScanNode = dynamic_cast<PathScanPlanNode*>(children[1]);

    	if (pathScanNode != NULL)
    	{
    		graphView = pathScanNode->getTargetGraphView();
    	}
    }

    return true;
}

bool NestedLoopPathExecutor::p_execute(const NValueArray &params) {

	if (graphView != NULL)
	{
		LogManager::GLog("NestedLoopPathExecutor", "p_execute", 67, graphView->name());
	}
	else
	{
		LogManager::GLog("NestedLoopPathExecutor", "p_execute", 67, "no graph view...");
	}

    VOLT_DEBUG("executing NestLoop...");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(m_abstractNode);
    assert(node);
    assert(node->getInputTableCount() == 2);

    // output table must be a temp table
    assert(m_tmpOutputTable);

    Table* outer_table = node->getInputTable();
    assert(outer_table);

    Table* inner_table = node->getInputTable(1);
    assert(inner_table);

    LogManager::GLog("NestedLoopPathExecutor", "p_execute (Outer table)", 117, outer_table->name());
    LogManager::GLog("NestedLoopPathExecutor", "p_execute (Inner table)", 118, inner_table->name());

    VOLT_TRACE ("input table left:\n %s", outer_table->debug().c_str());
    VOLT_TRACE ("input table right:\n %s", inner_table->debug().c_str());

    //
    // Pre Join Expression
    //
    AbstractExpression *preJoinPredicate = node->getPreJoinPredicate();
    if (preJoinPredicate) {
        VOLT_TRACE ("Pre Join predicate: %s", preJoinPredicate == NULL ?
                    "NULL" : preJoinPredicate->debug(true).c_str());
    }
    //
    // Join Expression
    //
    AbstractExpression *joinPredicate = node->getJoinPredicate();
    if (joinPredicate) {
        VOLT_TRACE ("Join predicate: %s", joinPredicate == NULL ?
                    "NULL" : joinPredicate->debug(true).c_str());
    }
    //
    // Where Expression
    //
    AbstractExpression *wherePredicate = node->getWherePredicate();
    if (wherePredicate) {
        VOLT_TRACE ("Where predicate: %s", wherePredicate == NULL ?
                    "NULL" : wherePredicate->debug(true).c_str());
    }

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (m_joinType == JOIN_TYPE_FULL) {
        // Prepopulate the view with all inner tuples
        innerTableFilter.init(inner_table);
    }

    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limit_node) {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }

    int outer_cols = outer_table->columnCount();
    int inner_cols = inner_table->columnCount();
    //TableTuple outer_tuple(node->getInputTable(0)->schema());
    //TableTuple inner_tuple(node->getInputTable(1)->schema());
    TableTuple outer_tuple(outer_table->schema());
    TableTuple inner_tuple(inner_table->schema());
    const TableTuple& null_inner_tuple = m_null_inner_tuple.tuple();

    TableIterator iterator0 = outer_table->iteratorDeletingAsWeGo();
    ProgressMonitorProxy pmp(m_engine, this);
    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, wherePredicate, limit, offset);

    TableTuple join_tuple;
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        join_tuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema, m_tmpOutputTable, &postfilter);
    } else {
        join_tuple = m_tmpOutputTable->tempTuple();
    }

    if (graphView != NULL)
    {
    	setStartAndEndVertexes(joinPredicate, inner_table, outer_table);
    	startVertexId = UNDEFINED;
    	endVertexId = UNDEFINED;
    	graphView->pathLength = pathScanNode->getPathLength();
    	graphView->spColumnIndexInEdgesTable = pathScanNode->getSPColumnIdInEdgesTable();
    	graphView->topK = 1;
    	graphView->queryType = getQueryType();


    	std::stringstream debugParams;
    	debugParams << "startVertexColumnId = " << startVertexColumnId << ", endVertexColumnId = " << endVertexColumnId << ", length = " << graphView->pathLength
    			<< ", spColumnIndexInEdgesTable = " << graphView->spColumnIndexInEdgesTable << ", queryType = " << graphView->queryType;
    	LogManager::GLog("NestedLoopPathExecutor", "p_execute", 224, debugParams.str());
    }

    while (postfilter.isUnderLimit() && iterator0.next(outer_tuple)) {
        pmp.countdownProgress();

        // populate output table's temp tuple with outer table's values
        // probably have to do this at least once - avoid doing it many
        // times per outer tuple
        join_tuple.setNValues(0, outer_tuple, 0, outer_cols);

        // did this loop body find at least one match for this tuple?
        bool outerMatch = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (preJoinPredicate == NULL || preJoinPredicate->eval(&outer_tuple, NULL).isTrue()) {

        	if (graphView == NULL)
        	{
				// By default, the delete as we go flag is false.
				TableIterator iterator1 = inner_table->iterator();
				while (postfilter.isUnderLimit() && iterator1.next(inner_tuple)) {
					pmp.countdownProgress();
					// Apply join filter to produce matches for each outer that has them,
					// then pad unmatched outers, then filter them all
					if (joinPredicate == NULL || joinPredicate->eval(&outer_tuple, &inner_tuple).isTrue()) {
						outerMatch = true;
						// The inner tuple passed the join predicate
						if (m_joinType == JOIN_TYPE_FULL) {
							// Mark it as matched
							innerTableFilter.updateTuple(inner_tuple, MATCHED_TUPLE);
						}
						// Filter the joined tuple
						if (postfilter.eval(&outer_tuple, &inner_tuple)) {
							// Matched! Complete the joined tuple with the inner column values.
							join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
							outputTuple(postfilter, join_tuple, pmp);
						}
					}
				} // END INNER WHILE LOOP
        	}
        	else //the inner is a graph view
        	{


        		if (startVertexColumnId != UNDEFINED)
        		{
        			startVertexId = ValuePeeker::peekAsInteger(outer_tuple.getNValue(startVertexColumnId));
        		}

        		if (endVertexColumnId != UNDEFINED)
        		{
        			endVertexId = ValuePeeker::peekAsInteger(outer_tuple.getNValue(endVertexColumnId));
        		}

        		graphView->fromVertexId = startVertexId;
        		graphView->toVertexId = endVertexId;

        		PathIterator pathIterator = graphView->iteratorDeletingAsWeGo();

        		while (postfilter.isUnderLimit() && pathIterator.next(inner_tuple)) {
					pmp.countdownProgress();
					// Apply join filter to produce matches for each outer that has them,
					// then pad unmatched outers, then filter them all
					//if (joinPredicate == NULL || joinPredicate->eval(&outer_tuple, &inner_tuple).isTrue())
					{
						outerMatch = true;
						// The inner tuple passed the join predicate
						if (m_joinType == JOIN_TYPE_FULL) {
							// Mark it as matched
							innerTableFilter.updateTuple(inner_tuple, MATCHED_TUPLE);
						}
						// Filter the joined tuple
						if (postfilter.eval(&outer_tuple, &inner_tuple)) {
							// Matched! Complete the joined tuple with the inner column values.
							join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
							outputTuple(postfilter, join_tuple, pmp);
						}
					}
				} // END INNER WHILE LOOP

        	}

        } // END IF PRE JOIN CONDITION

        //
        // Left Outer Join
        //
        if (m_joinType != JOIN_TYPE_INNER && !outerMatch && postfilter.isUnderLimit()) {
            // Still needs to pass the filter
            if (postfilter.eval(&outer_tuple, &null_inner_tuple)) {
                // Matched! Complete the joined tuple with the inner column values.
                join_tuple.setNValues(outer_cols, null_inner_tuple, 0, inner_cols);
                outputTuple(postfilter, join_tuple, pmp);
            }
        } // END IF LEFT OUTER JOIN
    } // END OUTER WHILE LOOP

    //
    // FULL Outer Join. Iterate over the unmatched inner tuples
    //
    if (m_joinType == JOIN_TYPE_FULL && postfilter.isUnderLimit()) {
        // Preset outer columns to null
        const TableTuple& null_outer_tuple = m_null_outer_tuple.tuple();
        join_tuple.setNValues(0, null_outer_tuple, 0, outer_cols);

        TableTupleFilter_iter<UNMATCHED_TUPLE> endItr = innerTableFilter.end<UNMATCHED_TUPLE>();
        for (TableTupleFilter_iter<UNMATCHED_TUPLE> itr = innerTableFilter.begin<UNMATCHED_TUPLE>();
                itr != endItr && postfilter.isUnderLimit(); ++itr) {
            // Restore the tuple value
            uint64_t tupleAddr = innerTableFilter.getTupleAddress(*itr);
            inner_tuple.move((char *)tupleAddr);
            // Still needs to pass the filter
            assert(inner_tuple.isActive());
            if (postfilter.eval(&null_outer_tuple, &inner_tuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
                outputTuple(postfilter, join_tuple, pmp);
            }
        }
   }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    cleanupInputTempTable(inner_table);
    cleanupInputTempTable(outer_table);

    return (true);
}


void NestedLoopPathExecutor::setStartAndEndVertexes(const AbstractExpression* joinExpression, const Table* inner, const Table* outer)
{
	if (joinExpression == NULL)
		return;

	const ComparisonExpression<CmpEq>* singlePredicate = dynamic_cast<const ComparisonExpression<CmpEq>*>(joinExpression);

	if (singlePredicate == NULL)
	{
		setStartAndEndVertexes(joinExpression->getLeft(), inner, outer);
		setStartAndEndVertexes(joinExpression->getRight(), inner, outer);
		return;
	}

	const TupleValueExpression* left = dynamic_cast<const TupleValueExpression*>(singlePredicate->getLeft());
	const TupleValueExpression* right = dynamic_cast<const TupleValueExpression*>(singlePredicate->getRight());
	if (left != NULL && right != NULL)
	{
		//make sure that left* points to the inner (i.e., graph view) and that right* points to the outer
		if (left->getTableId() == 0)
		{
			const TupleValueExpression* temp = left;
			left = right;
			right = temp;
		}

		assert(left->getTableId() == 1);
		assert(right->getTableId() == 0);

		if (inner->columnName(left->getColumnId()) == StartVertexLiteral)
		{
			startVertexColumnId = right->getColumnId();
		}
		else if (inner->columnName(left->getColumnId()) == EndVertexLiteral)
		{
			endVertexColumnId = right->getColumnId();
		}
	}
}

int NestedLoopPathExecutor::getQueryType()
{
	int queryType = UNDEFINED;
	if (startVertexColumnId != UNDEFINED && endVertexColumnId == UNDEFINED && pathScanNode->getPathLength() != UNDEFINED)
	{
		queryType = 1;
	}
	else if (startVertexColumnId != UNDEFINED && endVertexColumnId != UNDEFINED)
	{
		if (pathScanNode->getSPColumnIdInEdgesTable() == UNDEFINED)
		{
			queryType = 3; //reachability
		}
		else
		{
			queryType = 21; //shortest path
		}
	}

	return queryType;
}
