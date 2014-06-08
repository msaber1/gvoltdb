/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "nestloopindexexecutor.h"

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/ProgressMonitorProxy.h"
#include "executors/indexscanexecutor.h"
#include "expressions/abstractexpression.h"
#include "plannodes/nestloopindexnode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/limitnode.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"

namespace voltdb {

bool NestLoopIndexExecutor::p_init(TempTableLimits* limits)
{
    VOLT_TRACE("init NLIJ Executor");
    assert(limits);

    NestLoopIndexPlanNode* node = dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode);
    assert(node);

    m_join_type = node->getJoinType();
    m_prejoin_expression = node->getPreJoinPredicate();
    m_where_expression = node->getWherePredicate();
    m_output_expression_array = node->getOutputExpressionArray();

    // We need exactly one input table and a target table
    assert(m_input_tables.size() == 1);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    IndexScanPlanNode* inline_node =
        dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN));
    assert(inline_node);
    VOLT_TRACE("<NestLoopIndexPlanNode> %s, <IndexScanPlanNode> %s",
               node->debug().c_str(), inline_node->debug().c_str());
    //
    // Make sure that we actually have search keys
    //
    const std::vector<AbstractExpression*>& search_keys = inline_node->getSearchKeyExpressions();
    m_num_of_search_keys = (int)search_keys.size();
    AbstractExpression** search_key_array = new AbstractExpression*[m_num_of_search_keys];
    for (int ctr = 0; ctr < m_num_of_search_keys; ctr++) {
        search_key_array[ctr] = search_keys[ctr];
    }
    m_search_key_array_ptr.reset(search_key_array);
    m_lookupType = inline_node->getLookupType();
    m_sortDirection = inline_node->getSortDirection();
    m_inner_target_tcd = m_engine->getTableDelegate(inline_node->getTargetTableName());
    m_index_name = inline_node->getTargetIndexName();
    m_end_expression = inline_node->getEndExpression();
    m_post_expression = inline_node->getPredicate();
    m_initial_expression = inline_node->getInitialExpression();
    m_skip_null_predicate = inline_node->getSkipNullPredicate();

    VOLT_TRACE("<Nested Loop Index exec, INIT...> Number of searchKeys: %d \n", m_num_of_search_keys);

    IndexScanExecutor* child = dynamic_cast<IndexScanExecutor*>(inline_node->getExecutor());
    assert(child);
    PersistentTable* inner_table = dynamic_cast<PersistentTable*>(child->getTargetTable());
    assert(inner_table);

    // NULL tuple for outer join
    if (m_join_type == JOIN_TYPE_LEFT) {
        // It is unusual to have to write to the m_input_tables this late in AbstractExecutor.init,
        // but putting the inline child index scan's unused pseudo-output-table has the desired
        // effect of sizing/shaping the null tuple.
        Table* inner_output_table = appendInlineInputTable(child);
        assert(inner_output_table);
        m_null_tuple.init(inner_output_table->schema());
    }

    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    TableIndex* index = inner_table->index(m_index_name);
    if (index == NULL) {
        VOLT_ERROR("Failed to retreive index '%s' from inner table '%s' for"
                   " internal PlanNode '%s'",
                   m_index_name.c_str(),
                   inner_table->name().c_str(), inline_node->debug().c_str());
        return false;
    }


#ifdef DEBUG
    for (int ctr = 0; ctr < m_num_of_search_keys; ctr++) {
        VOLT_TRACE("Search Key[%d]:\n%s", ctr, search_keys[ctr]->debug(true).c_str());
    }
    if (m_end_expression) {
        VOLT_TRACE("End Expression:\n%s", m_end_expression->debug(true).c_str());
    }
    if (m_post_expression) {
        VOLT_TRACE("Post Expression:\n%s", m_post_expression->debug(true).c_str());
    }
    if (m_prejoin_expression) {
        VOLT_TRACE("Prejoin Expression:\n%s", m_prejoin_expression->debug(true).c_str());
    }
    if (m_where_expression) {
        VOLT_TRACE("Where Expression:\n%s", m_where_expression->debug(true).c_str());
    }
    if (m_initial_expression) {
        VOLT_TRACE("Initial Expression:\n%s", m_initial_expression->debug(true).c_str());
    }
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (m_skip_null_predicate) {
        VOLT_DEBUG("Skip NULL Expression:\n%s", m_skip_null_predicate->debug(true).c_str());
    }
#endif

    m_index_values.init(index->getKeySchema());

    // pickup an inlined limit, if one exists
    LimitPlanNode* limit_node =
        static_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    if (limit_node) {
        m_inlineLimitOffset = limit_node->getState();
    }

    return true;
}

bool NestLoopIndexExecutor::p_execute()
{
    assert(dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode));

    TempTable* output_table = getTempOutputTable();
    assert(output_table);

    // outer_table is the input table that has tuples to be iterated
    Table* outer_table = m_input_tables[0].getTable();
    assert (outer_table);

    PersistentTable* inner_table = getInnerTargetTable();
    assert(inner_table);

    TableIndex* index = inner_table->index(m_index_name);
    assert(index);

    VOLT_TRACE("executing NestLoopIndex with outer table: %s, inner table: %s",
               outer_table->debug().c_str(), inner_table->debug().c_str());

    // NULL tuple for outer join
    if (m_join_type == JOIN_TYPE_LEFT) {
        Table* inner_output_table = m_input_tables[1].getTable();
        m_null_tuple.resetWithCompatibleSchema(inner_output_table->schema());
    }

    m_index_values.resetWithCompatibleSchema(index->getKeySchema());
    TableTuple searchKey = m_index_values;

    int limit = -1;
    int offset = -1;
    m_inlineLimitOffset.getLimitAndOffsetByReference(m_engine, limit, offset);
    int tuple_ctr = 0;
    int tuple_skipped = 0;

    //
    // OUTER TABLE ITERATION
    //
    TableTuple outer_tuple(outer_table->schema());
    TableTuple inner_tuple(inner_table->schema());
    TableIterator outer_iterator = outer_table->iteratorDeletingAsWeGo();
    int num_of_outer_cols = outer_table->columnCount();
    assert (outer_tuple.sizeInValues() == outer_table->columnCount());
    assert (inner_tuple.sizeInValues() == inner_table->columnCount());
    TableTuple &join_tuple = output_table->tempTuple();
    TableTuple null_tuple = m_null_tuple;
    int num_of_inner_cols = (m_join_type == JOIN_TYPE_LEFT) ? null_tuple.sizeInValues() : 0;

    AbstractExpression** search_key_array = m_search_key_array_ptr.get();

    const AbstractExpression* const* output_expression_array = m_output_expression_array;

    ProgressMonitorProxy pmp(m_engine, this, inner_table);
    VOLT_TRACE("<num_of_outer_cols>: %d\n", num_of_outer_cols);
    while ((limit == -1 || tuple_ctr < limit) && outer_iterator.next(outer_tuple)) {
        VOLT_TRACE("outer_tuple:%s",
                   outer_tuple.debug(outer_table->name()).c_str());
        pmp.countdownProgress();
        // Set the outer tuple columns. Must be outside the inner loop
        // in case of the empty inner table
        join_tuple.setNValues(0, outer_tuple, 0, num_of_outer_cols);

        // did this loop body find at least one match for this tuple?
        bool match = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (m_prejoin_expression == NULL || m_prejoin_expression->eval(&outer_tuple, NULL).isTrue()) {

            int activeNumOfSearchKeys = m_num_of_search_keys;
            VOLT_TRACE ("<Nested Loop Index exec, WHILE-LOOP...> Number of searchKeys: %d \n", m_num_of_search_keys);
            IndexLookupType localLookupType = m_lookupType;
            SortDirectionType localSortDirection = m_sortDirection;
            VOLT_TRACE("Lookup type: %d\n", m_lookupType);
            VOLT_TRACE("SortDirectionType: %d\n", m_sortDirection);

            // did setting the search key fail (usually due to overflow)
            bool keyException = false;

            //
            // Now use the outer table tuple to construct the search key
            // against the inner table
            //
            searchKey.setAllNulls();
            for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
                // in a normal index scan, params would be substituted here,
                // but this scan fills in params outside the loop
                NValue candidateValue = search_key_array[ctr]->eval(&outer_tuple, NULL);
                try {
                    searchKey.setNValue(ctr, candidateValue);
                }
                catch (const SQLException &e) {
                    // This next bit of logic handles underflow and overflow while
                    // setting up the search keys.
                    // e.g. TINYINT > 200 or INT <= 6000000000

                    // re-throw if not an overflow or underflow
                    // currently, it's expected to always be an overflow or underflow
                    if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW)) == 0) {
                        throw e;
                    }

                    // Handle the case where this is a comparison, rather than an equality match.
                    // A comparison is the only place where the executor might return matching tuples
                    // e.g. TINYINT < 1000 should return all values
                    if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                        (ctr == (activeNumOfSearchKeys - 1))) {

                        if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                            if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                                (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {

                                // gt or gte when key overflows breaks out
                                // and only returns for left-outer
                                keyException = true;
                                break; // the outer while loop
                            }
                            else {
                                // overflow of LT or LTE should be treated as LTE
                                // to issue an "initial" forward scan
                                localLookupType = INDEX_LOOKUP_TYPE_LTE;
                            }
                        }
                        if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                            if ((localLookupType == INDEX_LOOKUP_TYPE_LT) ||
                                (localLookupType == INDEX_LOOKUP_TYPE_LTE)) {
                                // overflow of LT or LTE should be treated as LTE
                                // to issue an "initial" forward scans
                                localLookupType = INDEX_LOOKUP_TYPE_LTE;
                            }
                            else {
                                // don't allow GTE because it breaks null handling
                                localLookupType = INDEX_LOOKUP_TYPE_GT;
                            }
                        }

                        // if here, means all tuples with the previous searchkey
                        // columns need to be scaned.
                        activeNumOfSearchKeys--;
                        if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                            localSortDirection = SORT_DIRECTION_TYPE_ASC;
                        }
                    }
                    // if a EQ comparison is out of range, then the tuple from
                    // the outer loop returns no matches (except left-outer)
                    else {
                        keyException = true;
                    }
                    break;
                }
            }
            VOLT_TRACE("Searching %s", searchKey.debug("").c_str());

            // if a search value didn't fit into the targeted index key, skip this key
            if (!keyException) {
                //
                // Our index scan on the inner table is going to have three parts:
                //  (1) Lookup tuples using the search key
                //
                //  (2) For each tuple that comes back, check whether the
                //      end_expression is false.  If it is, then we stop
                //      scanning. Otherwise...
                //
                //  (3) Check whether the tuple satisfies the post expression.
                //      If it does, then add it to the output table
                //
                // Use our search key to prime the index iterator
                // The loop through each tuple given to us by the iterator
                //
                // Essentially cut and pasted this if ladder from
                // index scan executor
                if (m_num_of_search_keys > 0) {
                    if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
                        index->moveToKey(&searchKey);
                    }
                    else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                        index->moveToGreaterThanKey(&searchKey);
                    }
                    else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
                        index->moveToKeyOrGreater(&searchKey);
                    }
                    else if (localLookupType == INDEX_LOOKUP_TYPE_LT) {
                        index->moveToLessThanKey(&searchKey);
                    } else if (localLookupType == INDEX_LOOKUP_TYPE_LTE) {
                        // find the entry whose key is greater than search key,
                        // do a forward scan using initialExpr to find the correct
                        // start point to do reverse scan
                        bool isEnd = index->moveToGreaterThanKey(&searchKey);
                        if (isEnd) {
                            index->moveToEnd(false);
                        } else {
                            while (!(inner_tuple = index->nextValue()).isNullTuple()) {
                                pmp.countdownProgress();
                                if (m_initial_expression != NULL &&
                                    ! m_initial_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                                    // just passed the first failed entry, so move 2 backward
                                    index->moveToBeforePriorEntry();
                                    break;
                                }
                            }
                            if (inner_tuple.isNullTuple()) {
                                index->moveToEnd(false);
                            }
                        }
                    }
                    else {
                        return false;
                    }
                } else {
                    bool toStartActually = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
                    index->moveToEnd(toStartActually);
                }

                AbstractExpression* skipNullExprIteration = m_skip_null_predicate;

                while ((limit == -1 || tuple_ctr < limit) &&
                       ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
                        !(inner_tuple = index->nextValueAtKey()).isNullTuple()) ||
                       ((localLookupType != INDEX_LOOKUP_TYPE_EQ || m_num_of_search_keys == 0) &&
                        !(inner_tuple = index->nextValue()).isNullTuple()))) {
                    VOLT_TRACE("inner_tuple:%s",
                               inner_tuple.debug(inner_table->name()).c_str());
                    pmp.countdownProgress();

                    //
                    // First check to eliminate the null index rows for UNDERFLOW case only
                    //
                    if (skipNullExprIteration != NULL) {
                        if (skipNullExprIteration->eval(&outer_tuple, &inner_tuple).isTrue()) {
                            VOLT_DEBUG("Index scan: find out null rows or columns.");
                            continue;
                        } else {
                            skipNullExprIteration = NULL;
                        }
                    }

                    //
                    // First check whether the end_expression is now false
                    //
                    if (m_end_expression != NULL &&
                        ! m_end_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                        VOLT_TRACE("End Expression evaluated to false, stopping scan\n");
                        break;
                    }
                    //
                    // Then apply our post-predicate to do further filtering
                    //
                    if (m_post_expression != NULL &&
                        ! m_post_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                        continue;
                    }

                    match = true;
                    // Still need to pass where filtering
                    if (m_where_expression != NULL &&
                        ! m_where_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                        continue;
                    }

                    // Check if we have to skip this tuple because of offset
                    if (tuple_skipped < offset) {
                        tuple_skipped++;
                        continue;
                    }

                    ++tuple_ctr;
                    //
                    // Try to put the tuple into our output table
                    // Append the inner values to the end of our join tuple
                    //
                    for (int col_ctr = num_of_outer_cols;
                         col_ctr < join_tuple.sizeInValues();
                         ++col_ctr) {
                        // For the sake of consistency, we don't try to do
                        // output expressions here with columns from both tables.
                        join_tuple.setNValue(col_ctr,
                            output_expression_array[col_ctr]->eval(&outer_tuple, &inner_tuple));
                    }
                    VOLT_TRACE("join_tuple tuple: %s",
                               join_tuple.debug(output_table->name()).c_str());
                    VOLT_TRACE("MATCH: %s",
                               join_tuple.debug(output_table->name()).c_str());
                    output_table->insertTempTuple(join_tuple);
                    pmp.countdownProgress();
                }
            }
        }
        //
        // Left Outer Join
        //
        if ((limit == -1 || tuple_ctr < limit) && m_join_type == JOIN_TYPE_LEFT && !match ) {
            if (m_where_expression != NULL &&
                ! m_where_expression->eval(&outer_tuple, &null_tuple).isTrue()) {
                continue;
            }

            // Check if we have to skip this tuple because of offset
            if (tuple_skipped < offset) {
                tuple_skipped++;
                continue;
            }

            ++tuple_ctr;
            join_tuple.setNValues(num_of_outer_cols, m_null_tuple, 0, num_of_inner_cols);
            output_table->insertTempTuple(join_tuple);
            pmp.countdownProgress();
        }
    }

    VOLT_TRACE ("result table:\n %s", output_table->debug().c_str());
    VOLT_TRACE("Finished NestLoopIndex");
    return true;
}

PersistentTable* NestLoopIndexExecutor::getInnerTargetTable()
{
    return dynamic_cast<PersistentTable*>(m_inner_target_tcd->getTable());
}

}
