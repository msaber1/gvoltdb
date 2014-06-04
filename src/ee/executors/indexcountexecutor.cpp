/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "indexcountexecutor.h"

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "expressions/abstractexpression.h"
#include "indexes/tableindex.h"
#include "plannodes/indexcountnode.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

static long countNulls(TableIndex * tableIndex, AbstractExpression * countNULLExpr);

bool IndexCountExecutor::p_initMore(TempTableLimits* limits)
{
    VOLT_DEBUG("init IndexCount Executor");

    IndexCountPlanNode* node = dynamic_cast<IndexCountPlanNode*>(m_abstractNode);
    assert(node);
    assert(getTargetTable());
    assert(node->getPredicate() == NULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    //
    // Make sure that we have search keys and that they're not null
    //
    const std::vector<AbstractExpression*>& search_exprs_vector = node->getSearchKeyExpressions();
    m_lookupType = INDEX_LOOKUP_TYPE_INVALID;
    m_num_of_search_keys = (int)search_exprs_vector.size();
    if (m_num_of_search_keys != 0) {
        m_lookupType = node->getLookupType();
        AbstractExpression** search_key_array = new AbstractExpression*[m_num_of_search_keys];
        m_search_key_array_ptr.reset(search_key_array);
        for (int ctr = 0; ctr < m_num_of_search_keys; ctr++) {
            if (search_exprs_vector[ctr] == NULL) {
                VOLT_ERROR("The search key expression at position '%d' is NULL for PlanNode '%s'",
                    ctr, node->debug().c_str());
                return false;
            }
            search_key_array[ctr] = search_exprs_vector[ctr];
        }
    }

    const std::vector<AbstractExpression*>& end_exprs_vector = node->getEndKeyExpressions();
    m_num_of_end_keys = (int)end_exprs_vector.size();
    if (m_num_of_end_keys != 0) {
        m_endType = node->getEndType();
        AbstractExpression** end_key_array = new AbstractExpression*[m_num_of_end_keys];
        m_end_key_array_ptr.reset(end_key_array);
        for (int ctr = 0; ctr < m_num_of_end_keys; ctr++) {
            if (end_exprs_vector[ctr] == NULL) {
                VOLT_ERROR("The end key expression at position '%d' is NULL for PlanNode '%s'",
                    ctr, node->debug().c_str());
                return false;
            }
            end_key_array[ctr] = end_exprs_vector[ctr];
        }
    }

    // Miscellanous Information
    m_countNULLExpr = node->getSkipNullPredicate();

    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    Table* targetTable = getTargetTable();
    //target table should be persistenttable
    assert(dynamic_cast<PersistentTable*>(targetTable));

    m_index_name = node->getTargetIndexName();
    TableIndex *tableIndex = targetTable->index(m_index_name);
    assert (tableIndex);
    // This index should have a true countable flag
    assert(tableIndex->isCountableIndex());

    m_search_key.init(tableIndex->getKeySchema());
    m_end_key.init(tableIndex->getKeySchema());

    VOLT_DEBUG("IndexCount: %s.%s\n", targetTable->name().c_str(), m_index_name.c_str());

    return true;
}

bool IndexCountExecutor::p_execute()
{
    // update local target table with its most recent reference
    Table* targetTable = getTargetTable();
    TableIndex* tableIndex = targetTable->index(m_index_name);

    TempTable* output_table = getTempOutputTable();

    int activeNumOfSearchKeys = m_num_of_search_keys;
    IndexLookupType localLookupType = m_lookupType;
    bool searchKeyUnderflow = false, endKeyOverflow = false;
    // Overflow cases that can return early without accessing the index need this
    // default 0 count as their result.
    TableTuple& tmptup = output_table->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));

    const TupleSchema* key_schema = tableIndex->getKeySchema();
    //
    // SEARCH KEY
    //
    m_search_key.resetWithCompatibleSchema(key_schema);
    TableTuple searchKey = m_search_key;
    if (m_num_of_search_keys != 0) {
        searchKey.setAllNulls();
        AbstractExpression** search_key_array = m_search_key_array_ptr.get();
        VOLT_DEBUG("<Index Count>Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());
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
                    assert (localLookupType == INDEX_LOOKUP_TYPE_GT ||
                            localLookupType == INDEX_LOOKUP_TYPE_GTE);

                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        output_table->insertTempTuple(tmptup);
                        return true;
                    } else if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        searchKeyUnderflow = true;
                        break;
                    } else {
                        throw e;
                    }
                }
                // if a EQ comparision is out of range, then return no tuples
                else {
                    output_table->insertTempTuple(tmptup);
                    return true;
                }
                break;
            }
        }
        VOLT_TRACE("Search key after substitutions: '%s'", searchKey.debugNoHeader().c_str());
    }

    //
    // END KEY
    //
    m_end_key.resetWithCompatibleSchema(key_schema);
    TableTuple endKey = m_end_key;
    if (m_num_of_end_keys != 0) {
        endKey.setAllNulls();
        AbstractExpression** end_key_array = m_end_key_array_ptr.get();
        VOLT_DEBUG("Initial (all null) end key: '%s'", endKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < m_num_of_end_keys; ctr++) {
            NValue endKeyValue = end_key_array[ctr]->eval(NULL, NULL);
            try {
                endKey.setNValue(ctr, endKeyValue);
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

                if (ctr == (m_num_of_end_keys - 1)) {
                    assert (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE);
                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        output_table->insertTempTuple(tmptup);
                        return true;
                    } else if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        endKeyOverflow = true;
                        const ValueType type = endKey.getSchema()->columnType(ctr);
                        NValue tmpEndKeyValue = ValueFactory::getBigIntValue(getMaxTypeValue(type));
                        endKey.setNValue(ctr, tmpEndKeyValue);

                        VOLT_DEBUG("<Index count> end key out of range, MAX value: %ld...\n", (long)getMaxTypeValue(type));
                        break;
                    } else {
                        throw e;
                    }
                }
                // if a EQ comparision is out of range, then return no tuples
                else {
                    output_table->insertTempTuple(tmptup);
                    return true;
                }
                break;
            }
        }
        VOLT_TRACE("End key after substitutions: '%s'", endKey.debugNoHeader().c_str());
    }

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // The planner sometimes used to lie in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary.
    assert(m_lookupType != INDEX_LOOKUP_TYPE_EQ ||
           (key_schema->columnCount() == m_num_of_search_keys &&
            key_schema->columnCount() == m_num_of_end_keys));

    //
    // COUNT NULL EXPRESSION
    //
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (m_countNULLExpr != NULL) {
        VOLT_DEBUG("COUNT NULL Expression:\n%s", m_countNULLExpr->debug(true).c_str());
    }

    bool reverseScanNullEdgeCase = false;
    bool reverseScanMovedIndexToScan = false;
    if (m_num_of_search_keys < m_num_of_end_keys &&
            (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE)) {
        reverseScanNullEdgeCase = true;
        VOLT_DEBUG("Index count: reverse scan edge null case." );
    }

    // An index count has two cases: unique and non-unique
    int64_t rkStart = 0, rkEnd = 0, rkRes = 0;
    int leftIncluded = 0, rightIncluded = 0;

    if (m_num_of_search_keys != 0) {
        // Deal with multi-map
        VOLT_DEBUG("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());
        if (searchKeyUnderflow == false) {
            if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                rkStart = tableIndex->getCounterLET(&searchKey, true);
            } else {
                // handle start inclusive cases.
                if (tableIndex->hasKey(&searchKey)) {
                    leftIncluded = 1;
                    rkStart = tableIndex->getCounterLET(&searchKey, false);

                    if (reverseScanNullEdgeCase) {
                        tableIndex->moveToKeyOrGreater(&searchKey);
                        reverseScanMovedIndexToScan = true;
                    }
                } else {
                    rkStart = tableIndex->getCounterLET(&searchKey, true);
                }
            }
        } else {
            // Do not count null row or columns
            tableIndex->moveToKeyOrGreater(&searchKey);
            assert(m_countNULLExpr);
            long numNULLs = countNulls(tableIndex, m_countNULLExpr);
            rkStart += numNULLs;
            VOLT_DEBUG("Index count[underflow case]: "
                    "find out %ld null rows or columns are not counted in.", numNULLs);

        }
    }
    if (reverseScanNullEdgeCase) {
        // reverse scan case
        if (!reverseScanMovedIndexToScan && localLookupType != INDEX_LOOKUP_TYPE_GT) {
            tableIndex->moveToEnd(true);
        }
        assert(m_countNULLExpr);
        long numNULLs = countNulls(tableIndex, m_countNULLExpr);
        rkStart += numNULLs;
        VOLT_DEBUG("Index count[reverse case]: "
                "find out %ld null rows or columns are not counted in.", numNULLs);
    }

    if (m_num_of_end_keys != 0) {
        if (endKeyOverflow) {
            rkEnd = tableIndex->getCounterGET(&endKey, true);
        } else {
            IndexLookupType localEndType = m_endType;
            if (localEndType == INDEX_LOOKUP_TYPE_LT) {
                rkEnd = tableIndex->getCounterGET(&endKey, false);
            } else {
                if (tableIndex->hasKey(&endKey)) {
                    rightIncluded = 1;
                    rkEnd = tableIndex->getCounterGET(&endKey, true);
                } else {
                    rkEnd = tableIndex->getCounterGET(&endKey, false);
                }
            }
        }
    } else {
        rkEnd = tableIndex->getSize();
        rightIncluded = 1;
    }
    rkRes = rkEnd - rkStart - 1 + leftIncluded + rightIncluded;
    VOLT_DEBUG("Index Count ANSWER %ld = %ld - %ld - 1 + %d + %d\n",
            (long)rkRes, (long)rkEnd, (long)rkStart, leftIncluded, rightIncluded);
    tmptup.setNValue(0, ValueFactory::getBigIntValue( rkRes ));
    output_table->insertTempTuple(tmptup);

    VOLT_DEBUG ("Index Count :\n %s", output_table->debug().c_str());
    return true;
}


static long countNulls(TableIndex * tableIndex, AbstractExpression * countNULLExpr)
{
    if (countNULLExpr == NULL) {
        return 0;
    }
    long numNULLs = 0;
    TableTuple tuple;
    while ( ! (tuple = tableIndex->nextValue()).isNullTuple()) {
        if ( ! countNULLExpr->eval(&tuple, NULL).isTrue()) {
            break;
        }
        numNULLs++;
    }
    return numNULLs;
}
