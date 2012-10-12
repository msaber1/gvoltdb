/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#include "indexscanexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "indexes/tableindex.h"

// Inline PlanNodes
#include "plannodes/indexscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"

#include "projectionexecutor.h"

#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

void IndexScanExecutor::p_setOutputTable(TempTableLimits* limits)
{
    // Create output table based on output schema from the plan
    setTempOutputTable(limits, m_targetTable->name());
}

bool IndexScanExecutor::p_init()
{
    VOLT_TRACE("init IndexScan Executor");
    IndexScanPlanNode* node = dynamic_cast<IndexScanPlanNode*>(m_abstractNode);
    assert(node);
    assert(m_targetTable);

    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());

    //
    // INLINE PROJECTION
    //
    ProjectionPlanNode* projectionNode = static_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
    if (projectionNode) {
        m_columnExpressions = ProjectionExecutor::outputExpressions(projectionNode);
        m_columnsOnly = ProjectionExecutor::indexesIfAllTupleValues(m_columnExpressions);
    }

    //
    // Make sure that the search keys are not null
    //
    const std::vector<AbstractExpression*>& searchKeyArray = node->getSearchKeyExpressions();
    m_numOfSearchkeys = (int)searchKeyArray.size();

    //printf ("<INDEX SCAN> num of search keys: %d\n", m_numOfSearchkeys);
    for (int ctr = 0; ctr < m_numOfSearchkeys; ctr++) {
        if (searchKeyArray[ctr] == NULL) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for PlanNode "
                       "'%s'", ctr, node->debug().c_str());
            return false;
        }
    }

    //
    // Initialize local variables
    //

    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    m_index = m_targetTable->index(node->getTargetIndexName());
    if (m_index == NULL)
    {
        VOLT_ERROR("Failed to retreive index '%s' from table '%s' for PlanNode"
                   " '%s'", node->getTargetIndexName().c_str(),
                   m_targetTable->name().c_str(), node->debug().c_str());
        return false;
    }
    m_searchKey.allocateTupleNoHeader(m_index->getKeySchema());
    VOLT_TRACE("Index key schema: '%s'", m_index->getKeySchema()->debug().c_str());

    m_tuple = TableTuple(m_targetTable->schema());
    //
    // Miscellanous Information
    //
    m_lookupType = node->getLookupType();
    m_sortDirection = node->getSortDirection();

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // the planner sometimes lies in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary. Make the change here.
    if (m_lookupType == INDEX_LOOKUP_TYPE_EQ &&
        m_searchKey.getSchema()->columnCount() > m_numOfSearchkeys)
    {
        VOLT_TRACE("Setting lookup type to GTE for partial covering key.");
        m_lookupType = INDEX_LOOKUP_TYPE_GTE;
    }

    return true;
}

bool IndexScanExecutor::p_execute()
{
    IndexScanPlanNode* node = dynamic_cast<IndexScanPlanNode*>(m_abstractNode);
    assert(node);

    // output must be a temp table
    assert(m_outputTable);
    assert(m_outputTable == node->getOutputTable());
    assert(m_outputTable == dynamic_cast<TempTable*>(m_outputTable));
    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);

    assert(m_targetTable);
    assert(m_targetTable == node->getTargetTable());
    VOLT_DEBUG("IndexScan: %s.%s\n", m_targetTable->name().c_str(),
               m_index->getName().c_str());

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    SortDirectionType localSortDirection = m_sortDirection;

    assert(m_numOfColumns == m_outputTable->columnCount());

    //
    // INLINE LIMIT
    //
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(m_abstractNode->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));

    //
    // INLINE PROJECTION
    //
    bool hasInlineProjection = ! m_columnExpressions.empty();
    bool projectsColumnsOnly = ! m_columnsOnly.empty();

    //
    // SEARCH KEY
    //
    m_searchKey.setAllNulls();
    VOLT_TRACE("Initial (all null) search key: '%s'", m_searchKey.debugNoHeader().c_str());
    const std::vector<AbstractExpression*>& searchKeyExpressions = node->getSearchKeyExpressions();
    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        NValue candidateValue = searchKeyExpressions[ctr]->eval();
        try {
            m_searchKey.setNValue(ctr, candidateValue);
        }
        catch (SQLException e) {
            // This next bit of logic handles underflow and overflow while
            // setting up the search keys.
            // e.g. TINYINT > 200 or INT <= 6000000000

            // re-throw if not an overflow or underflow
            // currently, it's expected to always be an overflow or underflow
            if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW)) == 0) {
                throw e;
            }

            // handle the case where this is a comparison, rather than equality match
            // comparison is the only place where the executor might return matching tuples
            // e.g. TINYINT < 1000 should return all values
            if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                (ctr == (activeNumOfSearchKeys - 1))) {

                if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                        (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {

                        // gt or gte when key overflows returns nothing
                        return true;
                    }
                    else {
                        // VoltDB should only support LT or LTE with
                        // empty search keys for order-by without lookup
                        throw e;
                    }
                }
                if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_LT) ||
                        (localLookupType == INDEX_LOOKUP_TYPE_LTE)) {

                        // lt or lte when key underflows returns nothing
                        return true;
                    }
                    else {
                        // don't allow GTE because it breaks null handling
                        localLookupType = INDEX_LOOKUP_TYPE_GT;
                    }
                }

                // if here, means all tuples with the previous searchkey
                // columns need to be scaned. Note, if only one column,
                // then all tuples will be scanned
                activeNumOfSearchKeys--;
                if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                    localSortDirection = SORT_DIRECTION_TYPE_ASC;
                }
            }
            // if a EQ comparison is out of range, then return no tuples
            else {
                return true;
            }
            break;
        }
    }
    assert((activeNumOfSearchKeys == 0) || (m_searchKey.getSchema()->columnCount() > 0));
    VOLT_TRACE("Search key after substitutions: '%s'", m_searchKey.debugNoHeader().c_str());

    //
    // END EXPRESSION
    //
    AbstractExpression* end_expression = node->getEndExpression();
    if (end_expression != NULL)
    {
        VOLT_DEBUG("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    //
    // POST EXPRESSION
    //
    AbstractExpression* post_expression = node->getPredicate();
    if (post_expression != NULL)
    {
        VOLT_DEBUG("Post Expression:\n%s", post_expression->debug(true).c_str());
    }

    assert (m_index);
    assert (m_index == m_targetTable->index(node->getTargetIndexName()));

    //
    // An index scan has three parts:
    //  (1) Lookup tuples using the search key
    //  (2) For each tuple that comes back, check whether the
    //  end_expression is false.
    //  If it is, then we stop scanning. Otherwise...
    //  (3) Check whether the tuple satisfies the post expression.
    //      If it does, then add it to the output table
    //
    // Use our search key to prime the index iterator
    // Now loop through each tuple given to us by the iterator
    //
    if (activeNumOfSearchKeys > 0)
    {
        VOLT_TRACE("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, m_searchKey.debugNoHeader().c_str());

        if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
            m_index->moveToKey(&m_searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
            m_index->moveToGreaterThanKey(&m_searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
            m_index->moveToKeyOrGreater(&m_searchKey);
        }
        else {
            return false;
        }
    }

    //printf ("<INDEX SCAN> localSortDirection: %d\n", localSortDirection);
    if (localSortDirection != SORT_DIRECTION_TYPE_INVALID) {
        if (activeNumOfSearchKeys == 0) {
            bool order_by_asc = true;
            if (localSortDirection == SORT_DIRECTION_TYPE_ASC) {
                // nothing now
            }
            else {
                order_by_asc = false;
            }
            m_index->moveToEnd(order_by_asc);
        }
    }
    else if (localSortDirection == SORT_DIRECTION_TYPE_INVALID && activeNumOfSearchKeys == 0) {
        m_index->moveToEnd(true);
    }

    int tuple_ctr = 0;
    int tuples_skipped = 0;     // for offset
    int limit = -1;
    int offset = -1;
    if (limit_node != NULL) {
        limit_node->getLimitAndOffsetByReference(limit, offset);
    }

    //
    // We have to different nextValue() methods for different lookup types
    //
    while ((limit == -1 || tuple_ctr < limit) &&
           ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
             !(m_tuple = m_index->nextValueAtKey()).isNullTuple()) ||
           ((localLookupType != INDEX_LOOKUP_TYPE_EQ || activeNumOfSearchKeys == 0) &&
            !(m_tuple = m_index->nextValue()).isNullTuple()))) {
        VOLT_TRACE("LOOPING in indexscan: tuple: '%s'\n", m_tuple.debug("tablename").c_str());
        //
        // First check whether the end_expression is now false
        //
        if (end_expression != NULL &&
            end_expression->eval(&m_tuple).isFalse())
        {
            VOLT_TRACE("End Expression evaluated to false, stopping scan");
            break;
        }
        //
        // Then apply our post-predicate to do further filtering
        //
        if (post_expression == NULL ||
            post_expression->eval(&m_tuple).isTrue())
        {
            //
            // INLINE OFFSET
            //
            if (tuples_skipped < offset)
            {
                tuples_skipped++;
                continue;
            }
            tuple_ctr++;

            if (hasInlineProjection) {
                TableTuple &temp_tuple = m_outputTable->tempTuple();
                if (projectsColumnsOnly) {
                    VOLT_TRACE("sweet, all tuples");
                    for (int ctr = m_numOfColumns - 1; ctr >= 0; --ctr) {
                        temp_tuple.setNValue(ctr, m_tuple.getNValue(m_columnsOnly[ctr]));
                    }
                } else {
                    for (int ctr = m_numOfColumns - 1; ctr >= 0; --ctr) {
                        temp_tuple.setNValue(ctr, m_columnExpressions[ctr]->eval(&m_tuple));
                    }
                }
                output_temp_table->insertTempTuple(temp_tuple);
            } else {
                // Try to put the tuple into our output table
                output_temp_table->insertTempTuple(m_tuple);
            }
        }
    }

    VOLT_DEBUG ("Index Scanned :\n %s", m_outputTable->debug().c_str());
    return true;
}

IndexScanExecutor::~IndexScanExecutor() { }
