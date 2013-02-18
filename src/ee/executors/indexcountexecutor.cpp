/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "common/common.h"
#include "common/SQLException.h"
#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "expressions/abstractexpression.h"
#include "indexes/tableindex.h"
#include "plannodes/indexcountnode.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"

using namespace voltdb;

bool IndexCountExecutor::p_init()
{
    VOLT_DEBUG("init IndexCount Executor");

    IndexCountPlanNode* node = dynamic_cast<IndexCountPlanNode*>(m_abstractNode);
    assert(node);
    assert(node->getPredicate() == NULL);

    //
    // Make sure that we have search keys and that they're not null
    //
    m_numOfStartKeys = (int)node->getStartKeys().size();
    for (int ctr = 0; ctr < m_numOfStartKeys; ctr++)
    {
        if (node->getStartKeys()[ctr] == NULL) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for PlanNode "
                "'%s'", ctr, node->debug().c_str());
            return false;
        }
    }

    m_numOfEndKeys = (int)node->getEndKeys().size();
    if (m_numOfEndKeys != 0) {
        for (int ctr = 0; ctr < m_numOfEndKeys; ctr++)
        {
            if (node->getEndKeys()[ctr] == NULL) {
                VOLT_ERROR("The end key expression at position '%d' is NULL for PlanNode "
                    "'%s'", ctr, node->debug().c_str());
                return false;
            }
        }
    }

    //
    // Initialize local variables
    //

    // output must be a temp table
    assert(m_outputTable);
    assert(m_outputTable == node->getOutputTable());
    assert(m_outputTable == dynamic_cast<TempTable*>(m_outputTable));
    assert(m_targetTable);
    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());

    assert(m_numOfColumns == 1);
    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    m_index = m_targetTable->index(node->getTargetIndexName());
    assert (m_index != NULL);

    // This index should have a true countable flag
    assert(m_index->isCountableIndex());

    m_startKey.allocateTupleNoHeader(m_index->getKeySchema());
    m_startType = node->getStartType();
    if (m_numOfEndKeys != 0) {
        m_endKey.allocateTupleNoHeader(m_index->getKeySchema());
        m_endType = node->getEndType();
    }

    // The planner sometimes used to lie in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary.
    assert(m_startType != INDEX_LOOKUP_TYPE_EQ ||
           m_startKey.getSchema()->columnCount() == m_numOfStartKeys);
    return true;
}

void IndexCountExecutor::p_execute()
{
    IndexCountPlanNode* node = dynamic_cast<IndexCountPlanNode*>(m_abstractNode);
    assert(node);
    // output must be a temp table
    assert(m_outputTable);
    assert(m_outputTable == m_abstractNode->getOutputTable());
    TempTable* output_temp_table = dynamic_cast<TempTable*>(m_outputTable);
    assert(output_temp_table);
    assert(m_targetTable);
    assert(m_targetTable == node->getTargetTable());

    assert (node->getPredicate() == NULL);

    assert (m_index);
    assert (m_index == m_targetTable->index(node->getTargetIndexName()));
    assert (m_index->isCountableIndex());

    VOLT_TRACE("IndexCount: %s.%s\n", m_targetTable->name().c_str(),
               m_index->getName().c_str());

    int activeNumOfStartKeys = m_numOfStartKeys;
    IndexLookupType localStartType = m_startType;
    bool startKeyUnderflow = false, endKeyOverflow = false;
    // Overflow cases that can return early without accessing the index need this
    // default 0 count as their result.
    TableTuple& tmptup = m_outputTable->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));
    TableTuple m_dummy;

    //
    // SEARCH KEY
    //
    if (m_numOfStartKeys != 0) {
        m_startKey.setAllNulls();
        const std::vector<AbstractExpression*>& startKeys = node->getStartKeys();
        VOLT_DEBUG("<Index Count>Initial (all null) search key: '%s'", m_startKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < activeNumOfStartKeys; ctr++) {
            NValue candidateValue = startKeys[ctr]->eval(&m_dummy, NULL);
            try {
                m_startKey.setNValue(ctr, candidateValue);
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

                if ((localStartType != INDEX_LOOKUP_TYPE_EQ) &&
                    (ctr == (activeNumOfStartKeys - 1))) {
                    assert (localStartType == INDEX_LOOKUP_TYPE_GT || localStartType == INDEX_LOOKUP_TYPE_GTE);

                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        output_temp_table->insertTempTuple(tmptup);
                        return;
                    } else if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        startKeyUnderflow = true;
                        break;
                    }

                    throw e;
                }
                // if a EQ comparision is out of range, then return no tuples
                output_temp_table->insertTuple(tmptup);
                return;
            }
        }
    }

    VOLT_TRACE("Start key: '%s'", m_startKey.debugNoHeader().c_str());

    if (m_numOfEndKeys != 0) {
        //
        // END KEY
        //
        m_endKey.setAllNulls();
        VOLT_DEBUG("Initial (all null) end key: '%s'", m_endKey.debugNoHeader().c_str());
        const std::vector<AbstractExpression*>& endKeys = node->getEndKeys();
        for (int ctr = 0; ctr < m_numOfEndKeys; ctr++) {
            NValue endKeyValue = endKeys[ctr]->eval(&m_dummy, NULL);
            try {
                m_endKey.setNValue(ctr, endKeyValue);
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

                if (ctr == (m_numOfEndKeys - 1)) {
                    assert (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE);
                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        output_temp_table->insertTempTuple(tmptup);
                        return;
                    } else if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        endKeyOverflow = true;
                        const ValueType type = m_endKey.getSchema()->columnType(ctr);
                        NValue tmpEndKeyValue = ValueFactory::getBigIntValue(getMaxTypeValue(type));
                        m_endKey.setNValue(ctr, tmpEndKeyValue);

                        VOLT_DEBUG("<Index count> end key out of range, MAX value: %ld...\n", (long)getMaxTypeValue(type));
                        break;
                    } else {
                        throw e;
                    }
                }
                // if a EQ comparision is out of range, then return no tuples
                else {
                    output_temp_table->insertTempTuple(tmptup);
                    return;
                }
                break;
            }
        }
        VOLT_TRACE("End key: '%s'", m_endKey.debugNoHeader().c_str());
    }

    // An index count has two cases: unique and non-unique
    int64_t rkStart = 0, rkEnd = 0, rkRes = 0;
    int leftIncluded = 0, rightIncluded = 0;

    // Deal with multi-map
    VOLT_TRACE("INDEX_LOOKUP_TYPE(%d) m_numStartKeys(%d) key:%s",
               localStartType, activeNumOfStartKeys, m_startKey.debugNoHeader().c_str());
    if (activeNumOfStartKeys != 0) {
        if (startKeyUnderflow == false) {
            if (localStartType == INDEX_LOOKUP_TYPE_GT) {
                rkStart = m_index->getCounterLET(&m_startKey, true);
            } else {
                // handle start inclusive cases.
                if (m_index->hasKey(&m_startKey)) {
                    leftIncluded = 1;
                    rkStart = m_index->getCounterLET(&m_startKey, false);
                } else {
                    rkStart = m_index->getCounterLET(&m_startKey, true);
                }
            }
        }
    }

    if (m_numOfEndKeys != 0) {
        if (endKeyOverflow) {
            rkEnd = m_index->getCounterGET(&m_endKey, true);
        } else {
            IndexLookupType localEndType = m_endType;
            if (localEndType == INDEX_LOOKUP_TYPE_LT) {
                rkEnd = m_index->getCounterGET(&m_endKey, false);
            } else {
                if (m_index->hasKey(&m_endKey)) {
                    rightIncluded = 1;
                    rkEnd = m_index->getCounterGET(&m_endKey, true);
                } else {
                    rkEnd = m_index->getCounterGET(&m_endKey, false);
                }
            }
        }
    } else {
        rkEnd = m_index->getSize();
        rightIncluded = 1;
    }
    rkRes = rkEnd - rkStart - 1 + leftIncluded + rightIncluded;
    VOLT_TRACE("Index Count ANSWER %ld = %ld - %ld - 1 + %d + %d\n", (long)rkRes, (long)rkEnd, (long)rkStart, leftIncluded, rightIncluded);
    tmptup.setNValue(0, ValueFactory::getBigIntValue( rkRes ));
    output_temp_table->insertTempTuple(tmptup);

    VOLT_TRACE("Index Count :\n %s", m_outputTable->debug().c_str());
}

IndexCountExecutor::~IndexCountExecutor() { }
