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

#include "indexscanexecutor.h"

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/ProgressMonitorProxy.h"
#include "executors/projectionexecutor.h"
#include "expressions/abstractexpression.h"
#include "indexes/tableindex.h"

// Inline PlanNodes
#include "plannodes/indexscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"

#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool IndexScanExecutor::p_initMore(TempTableLimits* limits)
{
    VOLT_TRACE("init IndexScan Executor");

    Table* targetTable = getTargetTable();
    assert(targetTable);
    //target table should be persistenttable
    assert(static_cast<PersistentTable*>(targetTable));
    // Create output table based on output schema from the plan
    setTempOutputTable(limits, targetTable->name());

    IndexScanPlanNode* node = dynamic_cast<IndexScanPlanNode*>(m_abstractNode);
    assert(node);

    //
    // Miscellanous Information
    //
    m_lookupType = node->getLookupType();
    m_sortDirection = node->getSortDirection();
    m_end_expression = node->getEndExpression();
    m_post_expression = node->getPredicate();
    m_initial_expression = node->getInitialExpression();
    m_skip_null_predicate = node->getSkipNullPredicate();
    const std::vector<AbstractExpression*>& searchKeyExprs = node->getSearchKeyExpressions();
    m_num_of_search_keys = (int)searchKeyExprs.size();
    //
    // Make sure that we have search keys and that they're not null
    //
    AbstractExpression** search_keys = new AbstractExpression*[m_num_of_search_keys];
    for (int ctr = 0; ctr < m_num_of_search_keys; ctr++) {
        if (searchKeyExprs[ctr] == NULL) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " PlanNode '%s'", ctr, node->debug().c_str());
            delete [] search_keys;
            return false;
        }
        search_keys[ctr] = searchKeyExprs[ctr];
    }
    m_search_key_array_ptr.reset(search_keys);

    m_index_name = node->getTargetIndexName();
    TableIndex *tableIndex = targetTable->index(m_index_name);
    assert(tableIndex);
    const TupleSchema* keySchema = tableIndex->getKeySchema();
    m_search_key.init(keySchema);

    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    VOLT_TRACE("Index key schema: '%s'", keySchema->debug().c_str());
    VOLT_DEBUG("IndexScan: %s.%s\n", targetTable->name().c_str(), m_index_name.c_str());
    return true;
}

bool IndexScanExecutor::p_execute()
{
    assert(dynamic_cast<IndexScanPlanNode*>(m_abstractNode));

    TempTable* output_table = getTempOutputTable();
    assert(output_table);
    int num_of_columns = output_table->columnCount();
    TableTuple &temp_tuple = output_table->tempTuple();

    // update local target table with its most recent reference
    Table* targetTable = getTargetTable();

    TableIndex *tableIndex = targetTable->index(m_index_name);
    int activeNumOfSearchKeys = m_num_of_search_keys;
    IndexLookupType localLookupType = m_lookupType;
    SortDirectionType localSortDirection = m_sortDirection;

    //
    // SEARCH KEY
    //
    const TupleSchema* keySchema = tableIndex->getKeySchema();
    m_search_key.resetWithCompatibleSchema(keySchema);
    TableTuple searchKey = m_search_key;
    searchKey.setAllNulls();
    AbstractExpression** search_key_array = m_search_key_array_ptr.get();

    VOLT_TRACE("Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());
    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        NValue candidateValue = search_key_array[ctr]->eval(NULL, NULL);
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
                        // for overflow on reverse scan, we need to
                        // do a forward scan to find the correct start
                        // point, which is exactly what LTE would do.
                        // so, set the lookupType to LTE and the missing
                        // searchkey will be handled by extra post filters
                        localLookupType = INDEX_LOOKUP_TYPE_LTE;
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
    assert(localLookupType != INDEX_LOOKUP_TYPE_EQ ||
           keySchema->columnCount() == activeNumOfSearchKeys);

    assert((activeNumOfSearchKeys == 0) || (keySchema->columnCount() > 0));
    VOLT_TRACE("Search key after substitutions: '%s'", searchKey.debugNoHeader().c_str());
#ifdef DEBUG
    if (m_end_expression != NULL) {
        VOLT_DEBUG("End Expression:\n%s", m_end_expression->debug(true).c_str());
    }

    if (m_post_expression != NULL) {
        VOLT_DEBUG("Post Expression:\n%s", m_post_expression->debug(true).c_str());
    }

    if (m_initial_expression != NULL) {
        VOLT_DEBUG("Initial Expression:\n%s", m_initial_expression->debug(true).c_str());
    }

    // For reverse scan edge case NULL values and forward scan underflow case.
    if (m_skip_null_predicate != NULL) {
        VOLT_DEBUG("COUNT NULL Expression:\n%s", m_skip_null_predicate->debug(true).c_str());
    }
#endif
    AbstractExpression* const* projection_expressions = NULL;
    const int* projection_columns = getProjectionColumns();
    if (projection_columns == NULL) {
        projection_expressions = getProjectionExpressions();
    }

    ProgressMonitorProxy pmp(m_engine, this, targetTable);
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

    TableTuple tuple;
    if (activeNumOfSearchKeys > 0) {
        VOLT_TRACE("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());

        if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
            tableIndex->moveToKey(&searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
            tableIndex->moveToGreaterThanKey(&searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
            tableIndex->moveToKeyOrGreater(&searchKey);
        } else if (localLookupType == INDEX_LOOKUP_TYPE_LT) {
            tableIndex->moveToLessThanKey(&searchKey);
        } else if (localLookupType == INDEX_LOOKUP_TYPE_LTE) {
            // find the entry whose key is greater than search key,
            // do a forward scan using initialExpr to find the correct
            // start point to do reverse scan
            bool isEnd = tableIndex->moveToGreaterThanKey(&searchKey);
            if (isEnd) {
                tableIndex->moveToEnd(false);
            } else {
                while (!(tuple = tableIndex->nextValue()).isNullTuple()) {
                    pmp.countdownProgress();
                    if (m_initial_expression != NULL &&
                        ! m_initial_expression->eval(&tuple, NULL).isTrue()) {
                        // just passed the first failed entry, so move 2 backward
                        tableIndex->moveToBeforePriorEntry();
                        break;
                    }
                }
                if (tuple.isNullTuple()) {
                    tableIndex->moveToEnd(false);
                }
            }
        }
        else {
            return false;
        }
    } else {
        bool toStartActually = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
        tableIndex->moveToEnd(toStartActually);
    }

    int limit = -1;
    int offset = -1;
    getLimitAndOffsetByReference(limit, offset);
    int tuple_ctr = 0;
    int tuples_skipped = 0;     // for offset

    AbstractExpression* skipNullExprIteration = m_skip_null_predicate;

    //
    // We have to different nextValue() methods for different lookup types
    //
    while ((limit == -1 || tuple_ctr < limit) &&
           ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
             !(tuple = tableIndex->nextValueAtKey()).isNullTuple()) ||
           ((localLookupType != INDEX_LOOKUP_TYPE_EQ || activeNumOfSearchKeys == 0) &&
            !(tuple = tableIndex->nextValue()).isNullTuple()))) {
        VOLT_TRACE("LOOPING in indexscan: tuple: '%s'\n", tuple.debug("tablename").c_str());
        pmp.countdownProgress();
        //
        // First check to eliminate the null index rows for UNDERFLOW case only
        //
        if (skipNullExprIteration != NULL) {
            if (skipNullExprIteration->eval(&tuple, NULL).isTrue()) {
                VOLT_DEBUG("Index scan: find out null rows or columns.");
                continue;
            } else {
                skipNullExprIteration = NULL;
            }
        }
        //
        // First check whether the end_expression is now false
        //
        if (m_end_expression != NULL && ! m_end_expression->eval(&tuple, NULL).isTrue()) {
            VOLT_TRACE("End Expression evaluated to false, stopping scan");
            break;
        }
        //
        // Then apply our post-predicate to do further filtering
        //
        if (m_post_expression == NULL || m_post_expression->eval(&tuple, NULL).isTrue()) {
            //
            // INLINE OFFSET
            //
            if (tuples_skipped < offset) {
                tuples_skipped++;
                continue;
            }
            tuple_ctr++;

            ProjectionExecutor::insertTempOutputTuple(output_table, tuple, temp_tuple, num_of_columns,
                                                      projection_columns, projection_expressions);
            pmp.countdownProgress();
        }
    }

    VOLT_DEBUG ("Index Scanned :\n %s", m_outputTable->debug().c_str());
    return true;
}
