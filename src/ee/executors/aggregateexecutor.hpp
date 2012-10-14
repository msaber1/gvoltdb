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

/***
 *
 *  DO NOT INCLUDE THIS FILE ANYWHERE EXCEPT executors.h.
 *
 ****/
#ifndef HSTOREAGGREGATEEXECUTOR_H
#define HSTOREAGGREGATEEXECUTOR_H

#include "executors/abstractexecutor.h"

#include "common/Pool.hpp"
#include "common/ValueFactory.hpp"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/SerializableEEException.h"
#include "expressions/abstractexpression.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"

#include "boost/unordered_map.hpp"

#include <algorithm>
#include <limits>
#include <set>
#include <stdint.h>
#include <utility>

namespace voltdb {
/*
 * Type of the hash set used to check for column aggregate distinctness
 */
typedef boost::unordered_set<NValue,
                             NValue::hash,
                             NValue::equal_to> AggregateNValueSetType;

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class Agg
{
public:
    void* operator new(size_t size, Pool& memoryPool) { return memoryPool.allocate(size); }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    Agg(bool isDistinct = false) : m_haveAdvanced(false), mIsDistinct(isDistinct)
    {
        m_value.setNull();
    }
    virtual ~Agg()
    {
        mDistinctVals.clear();
    }
    virtual void advance(const NValue val) = 0;
    virtual NValue finalize() { return m_value; }
protected:
    bool includeValue(NValue val)
    {
        bool retval = true;
        if (mIsDistinct)
        {
            // find this value in the set.  If it doesn't exist, add
            // it, otherwise indicate it shouldn't be included in the
            // aggregate
            AggregateNValueSetType::iterator setval =
                mDistinctVals.find(val);
            if (setval == mDistinctVals.end())
            {
                mDistinctVals.insert(val);
            }
            else
            {
                retval = false;
            }
        }
        return retval;
    }

    bool m_haveAdvanced;
    bool mIsDistinct;
    AggregateNValueSetType mDistinctVals;
    NValue m_value;
};

class SumAgg : public Agg
{
  public:
    SumAgg(bool isDistinct) : Agg(isDistinct) {}

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val)) {
            return;
        }
        if (!m_haveAdvanced) {
            m_value = val;
            m_haveAdvanced = true;
        }
        else {
            m_value = m_value.op_add(val);
        }
    }
};


class AvgAgg : public Agg {
public:
    AvgAgg(bool isDistinct) : Agg(isDistinct), m_count(0) {}

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val)) {
            return;
        }
        if (m_count == 0) {
            m_value = val;
        }
        else {
            m_value = m_value.op_add(val);
        }
        ++m_count;
    }

    NValue finalize()
    {
        const NValue finalizeResult =
            m_value.op_divide(ValueFactory::getBigIntValue(m_count));
        return finalizeResult;
    }

private:
    int64_t m_count;
};

//count always holds integer
class CountAgg : public Agg
{
public:
    CountAgg(bool isDistinct) : Agg(isDistinct), m_count(0) {}

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val))
        {
            return;
        }
        m_count++;
    }

    NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

private:
    int64_t m_count;
};

class CountStarAgg : public Agg
{
public:
    CountStarAgg() : m_count(0) {}

    void advance(const NValue val)
    {
        ++m_count;
    }

    NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

private:
    int64_t m_count;
};

class MaxAgg : public Agg
{
public:
    MaxAgg() {}

    void advance(const NValue val)
    {
        if (val.isNull())
        {
            return;
        }
        if (!m_haveAdvanced)
        {
            m_value = val;
            m_haveAdvanced = true;
        }
        else
        {
            m_value = m_value.op_max(val);
        }
    }
};

class MinAgg : public Agg
{
public:
    MinAgg() { }

    void advance(const NValue val)
    {
        if (val.isNull())
        {
            return;
        }
        if (!m_haveAdvanced)
        {
            m_value = val;
            m_haveAdvanced = true;
        }
        else
        {
            m_value = m_value.op_min(val);
        }
    }
};

/**
 * A list of aggregates for a specific group.
 */
struct AggregateList
{
    void* operator new(size_t size, Pool& memoryPool, size_t nAggs)
    { return memoryPool.allocate(size + (sizeof(void*) * nAggs)); }
    void operator delete(void*, Pool& memoryPool, size_t nAggs) { /* NOOP -- on alloc error unroll */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    AggregateList(TableTuple& nxtTuple) : m_groupTuple(nxtTuple) {}

    // A tuple from the group of tuples being aggregated. Source of
    // pass through columns.
    TableTuple m_groupTuple;

    // The aggregates for each column for this group
    Agg* m_aggregates[0];
};

/*
 * Type of the hash table used to store aggregate lists for each group.
 */
typedef boost::unordered_map<TableTuple,
                             AggregateList*,
                             TableTupleHasher,
                             TableTupleEqualityChecker> HashAggregateMapType;

/**
 * Working storage whose type and API are dependent on the aggregate's PlanNodeType.
 * Wrapping the completely different member variables of the AggregateExecutor instantiations into
 * corresponding instantiations of this empty state class avoids the duplicated boilerplate code
 * that would result from explicitly instantiating the entire AggregateExecutor class.
 * This way, only the minimal set of member functions that differ need explicit instantiations
 * and each set of function instantiations can use whatever data members it needs by defining
 * (or inheriting) them in their own AggregatorState */
template<PlanNodeType aggregateType> class AggregatorState {};

/** Hash aggregates need to maintain a hash of group key tuples to Aggs */
template<> struct AggregatorState<PLAN_NODE_TYPE_HASHAGGREGATE> : public HashAggregateMapType {};

/** Serial aggregates need to maintain only one row of Aggs and the "previous" input tuple that defines
 * their associated group keys -- so group transitions can be detected.
 * In the case of table aggregates that have no grouping keys, the previous tuple has no effect and is tracked
 * for nothing -- a separate instantiation for that case could be made much simpler/faster. */
template<> struct AggregatorState<PLAN_NODE_TYPE_AGGREGATE>
{
    Agg** m_aggs;
    TableTuple m_prevTuple;
};

/**
 * The base class for aggregate executors regardless of the type of grouping that should be performed.
 */
class AggregateExecutorBase : public AbstractExecutor
{
public:
    AggregateExecutorBase() : m_groupByKeySchema(NULL) {}
    ~AggregateExecutorBase()
    {
        if (m_groupByKeySchema != NULL) {
            TupleSchema::freeTupleSchema(m_groupByKeySchema);
        }
    }

protected:
    bool initBase()
    {
        AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(m_abstractNode);
        assert(node);
        assert(hasExactlyOneInputTable());
        assert(node->getChildren()[0] != NULL);

        m_inputExpressions = node->getAggregateInputExpressions();
        for (int i = 0; i < m_inputExpressions.size(); i++)
        {
            VOLT_DEBUG("\nAGG INPUT EXPRESSIONS: %s\n",
                       m_inputExpressions[i]->debug().c_str());
        }

        /*
         * Find the difference between the set of aggregate output columns
         * (output columns resulting from an aggregate) and output columns.
         * Columns that are not the result of aggregates are being passed
         * through from the input table. Do this extra work here rather then
         * serialize yet more data.
         */
        m_aggregateOutputColumns = node->getAggregateOutputColumns();
        std::vector<bool> outputColumnsResultingFromAggregates(node->getOutputSchema().size(), false);
        for (int ii = 0; ii < m_aggregateOutputColumns.size(); ii++) {
            outputColumnsResultingFromAggregates[m_aggregateOutputColumns[ii]] = true;
        }

        /*
         * Now collect the indices in the output table of the pass
         * through columns.
         */
        for (int ii = 0; ii < outputColumnsResultingFromAggregates.size(); ii++) {
            if (outputColumnsResultingFromAggregates[ii] == false) {
                m_passThroughColumns.push_back(ii);
            }
        }

        m_aggTypes = node->getAggregates();
        m_distinctAggs = node->getDistinctAggregates();
        m_groupByExpressions = node->getGroupByExpressions();
        node->collectOutputExpressions(m_outputColumnExpressions);

        std::vector<ValueType> groupByColumnTypes;
        std::vector<int32_t> groupByColumnSizes;
        std::vector<bool> groupByColumnAllowNull;
        for (int ii = 0; ii < m_groupByExpressions.size(); ii++)
        {
            AbstractExpression* expr = m_groupByExpressions[ii];
            groupByColumnTypes.push_back(expr->getValueType());
            groupByColumnSizes.push_back(expr->getValueSize());
            groupByColumnAllowNull.push_back(true);
        }
        m_groupByKeySchema = TupleSchema::createTupleSchema(groupByColumnTypes,
                                                            groupByColumnSizes,
                                                            groupByColumnAllowNull,
                                                            true);
        m_groupByKeyTuple.allocateTupleNoHeader(m_groupByKeySchema, &m_memoryPool);

        return true;
    }

    /// Helper method responsible for inserting the results of the
    /// aggregation into a new tuple in the output table as well as passing
    /// through any additional columns from the input table.
    inline bool insertOutputTuple(Agg** aggs, TableTuple groupedTuple);

    void advanceAggs(Agg** aggs, const TableTuple& nxtTuple)
    {
        for (int i = 0; i < m_aggTypes.size(); i++) {
            aggs[i]->advance(m_inputExpressions[i]->eval(&nxtTuple));
        }
    }

    /*
     * Create an instance of an aggregator for the specified aggregate
     * type, column type, and result type. The object is constructed in
     * memory from the provided memrory pool.
     */
    void initAggInstances(Agg** aggs)
    {
        for (int i = 0; i < m_aggTypes.size(); i++) {
            Agg* agg;
            switch (m_aggTypes[i]) {
            case EXPRESSION_TYPE_AGGREGATE_COUNT:
                agg = new (m_memoryPool) CountAgg(m_distinctAggs[i]);
                break;
            case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
                agg = new (m_memoryPool) CountStarAgg();
                break;
            case EXPRESSION_TYPE_AGGREGATE_SUM:
                agg = new (m_memoryPool) SumAgg(m_distinctAggs[i]);
                break;
            case EXPRESSION_TYPE_AGGREGATE_AVG:
                agg = new (m_memoryPool) AvgAgg(m_distinctAggs[i]);
                break;
            case EXPRESSION_TYPE_AGGREGATE_MIN:
                agg = new (m_memoryPool) MinAgg();
                break;
            case EXPRESSION_TYPE_AGGREGATE_MAX  :
                agg = new (m_memoryPool) MaxAgg();
                break;
            default: {
                char message[128];
                snprintf(message, sizeof(message), "Unknown aggregate type %d", m_aggTypes[i]);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
            }
            }
            aggs[i] = agg;
        }
    }

    void tearDownAggs(Agg** aggs)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            delete aggs[ii]; // does not actually deallocate -- Aggs are pool-managed;
        }
    }

    /*
     * List of columns in the output schema that are passing through
     * the value from a column in the input table and not doing any
     * aggregation.
     */
    std::vector<int> m_passThroughColumns;
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema;
    Table* m_inputTable;
    std::vector<ExpressionType> m_aggTypes;
    std::vector<bool> m_distinctAggs;
    std::vector<AbstractExpression*> m_groupByExpressions;
    std::vector<AbstractExpression*> m_inputExpressions;
    std::vector<AbstractExpression*> m_outputColumnExpressions;
    std::vector<int> m_aggregateOutputColumns;
    PoolBackedTempTuple m_groupByKeyTuple;
};

/*
 * Helper method responsible for inserting the results of the
 * aggregation into a new tuple in the output table as well as passing
 * through any additional columns from the input table.
 */
inline bool
AggregateExecutorBase::insertOutputTuple(Agg** aggs, TableTuple groupedTuple)
{
    TableTuple& tmptup = m_outputTable->tempTuple();

    /*
     * This first pass is to add all columns that were aggregated on.
     */
    for (int ii = 0; ii < m_aggregateOutputColumns.size(); ii++)
    {
        if (aggs[ii] != NULL)
        {
            const int columnIndex = m_aggregateOutputColumns[ii];
            const ValueType columnType = tmptup.getType(columnIndex);
            tmptup.setNValue(columnIndex,
                             aggs[ii]->finalize().castAs(columnType));
        }
        else
        {
            // (rtb) This is surely not desirable code. However... I
            // think that the planner sometimes outputs aggregate
            // configuration that confuses the aggregate output
            // columns (that are aggregation f()'s) with the group by
            // columns. Maybe this only happens when aggregating DML
            // results?  Need to come back to this; but for now, this
            // arrangement satisfies valgrind (not previously the
            // case) and passes the plans group by test suite (also
            // not previously the case).
            return true;
        }
    }
    VOLT_TRACE("Setting passthrough columns");
    /*
     * Execute a second pass to set the output columns from the input
     * columns that are being passed through.  These are the columns
     * that are not being aggregated on but are still in the SELECT
     * list. These columns may violate the Single-Value rule for GROUP
     * BY (not be on the group by column reference list). This is an
     * intentional optimization to allow values that are not in the
     * GROUP BY to be passed through.
     */
    for (int i = 0; i < m_passThroughColumns.size(); i++) {
        int output_col_index = m_passThroughColumns[i];
        tmptup.setNValue(output_col_index, m_outputColumnExpressions[output_col_index]->eval(&groupedTuple));
    }

    if ( ! dynamic_cast<TempTable*>(m_outputTable)->insertTempTuple(tmptup)) {
        VOLT_ERROR("Failed to insert order-by tuple from input table '%s' into"
                   " output table '%s'",
                   m_inputTable->name().c_str(), m_outputTable->name().c_str());
        return false;
    }
    return true;
}


/**
 * The actual executor class templated on the type of grouping that
 * should be performed. If it is instantiated using
 * PLAN_NODE_TYPE_AGGREGATE then it will do a constant space
 * aggregation that expects the input table to be sorted on the group
 * by key. If it is instantiated using PLAN_NODE_TYPE_HASHAGGREGATE
 * then the input does not need to be sorted and it will hash the
 * group by key to aggregate the tuples.
 */
template<PlanNodeType aggregateType>
class AggregateExecutor : public AggregateExecutorBase
{
public:
    AggregateExecutor() { }
    ~AggregateExecutor() { purgeAggs(); }

protected:
    bool p_init();
    bool p_execute();

private:
    void prepareFirstTuple();
    bool nextTuple(TableTuple nxtTuple);
    bool finalize();
    void purgeAggs();

    AggregatorState<aggregateType> m_data;
};

/*
 * Member function specializations for an AggregateExecutor that uses a hash map to simultaneously aggregate
 * randomly ordered tuples from the input table.
 */
    template<> inline void AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>::prepareFirstTuple()
    {
        m_groupByKeyTuple.allocateTupleNoHeader(m_groupByKeySchema, &m_memoryPool);
        m_data.clear();
    }

    template<> inline void AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>::purgeAggs()
    {
        for (HashAggregateMapType::const_iterator iter = m_data.begin();
             iter != m_data.end();
             iter++) {
            tearDownAggs(iter->second->m_aggregates);
        }
    }

    template<> inline bool AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>::nextTuple(TableTuple nxtTuple)
    {
        AggregateList *aggregateList;
        // configure a tuple and search for the required group.
        for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
            m_groupByKeyTuple.setNValue(ii, m_groupByExpressions[ii]->eval(&nxtTuple));
        }
        HashAggregateMapType::const_iterator keyIter = m_data.find(m_groupByKeyTuple);

        // Group not found. Make a new entry in the hash for this new group.
        if (keyIter == m_data.end()) {
            aggregateList = new (m_memoryPool, m_aggTypes.size()) AggregateList(nxtTuple);
            m_data.insert(HashAggregateMapType::value_type(m_groupByKeyTuple, aggregateList));
            // The map is referencing the current key tuple for use by the new group,
            // so allocate a new tuple to hold the next candidate key
            m_groupByKeyTuple.reallocateTupleNoHeader();
        } else {
            // otherwise, the list is the second item of the pair...
            aggregateList = keyIter->second;
        }

        // update the aggregation calculation.
        advanceAggs(aggregateList->m_aggregates, nxtTuple);
        return true;
    }

    template<> inline bool AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>::finalize()
    {
        for (HashAggregateMapType::const_iterator iter = m_data.begin(); iter != m_data.end(); iter++) {
            if (!insertOutputTuple(iter->second->m_aggregates, iter->second->m_groupTuple)) {
                return false;
            }
        }
        return true;
    }


/*
 * Member function specializations for an aggregator that expects the input table to be
 * sorted on the group by key.
 */
    template<> inline void AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>::prepareFirstTuple()
    {
        m_data.m_aggs = static_cast<Agg**>(m_memoryPool.allocateZeroes(sizeof(void*) * m_aggTypes.size()));
        m_data.m_prevTuple = TableTuple(m_inputTable->schema());
    }

    template<> inline bool AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>::nextTuple(TableTuple nxtTuple)
    {
        bool startNewAgg = false;
        if (m_data.m_prevTuple.isNullTuple()) {
            startNewAgg = true;
        } else {
            for (int i = 0; i < m_groupByExpressions.size(); i++) {
                //TODO: In theory, at the start of each new agg, m_groupByExpressions[i]->eval(&nxtTuple)
                // could be cached in a key tuple and used for future comparisons instead of repeatedly
                // re-evaluating m_groupByExpressions[i]->eval(&m_data.m_prevTuple).
                // This cached key tuple seems to be the only aspect of m_prevTuple that needs to be retained.
                // There's a good chance that Micheal Alexeev has already solved this problem for the pullexec
                // implementation in which shorter temp tuple lifetimes may have made keeping m_prevTuple a non-option.
                startNewAgg =
                    m_groupByExpressions[i]->eval(&nxtTuple).
                    op_notEquals(m_groupByExpressions[i]->eval(&m_data.m_prevTuple)).isTrue();
                if (startNewAgg) {
                    break;
                }
            }
        }
        if (startNewAgg) {
            VOLT_TRACE("new group!");
            if (!m_data.m_prevTuple.isNullTuple() && !insertOutputTuple(m_data.m_aggs, m_data.m_prevTuple)) {
                return false;
            }
            tearDownAggs(m_data.m_aggs);
            initAggInstances(m_data.m_aggs);
        }
        advanceAggs(m_data.m_aggs, nxtTuple);
        m_data.m_prevTuple.move(nxtTuple.address());
        return true;
    }

    template<> inline bool AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>::finalize() {
        if (!m_data.m_prevTuple.isNullTuple() && !insertOutputTuple(m_data.m_aggs, m_data.m_prevTuple)) {
            return false;
        }

        // if no record exists in input_table, we have to output one record
        // only when it doesn't have GROUP BY. See difference of these cases:
        //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
        //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple
        if (m_groupByExpressions.size() == 0 && m_outputTable->activeTupleCount() == 0) {
            VOLT_TRACE("no record. outputting a NULL row..");
            initAggInstances(m_data.m_aggs);
            if (!insertOutputTuple(m_data.m_aggs, m_data.m_prevTuple)) {
                return false;
            }
        }
        return true;
    }

    template<> inline void AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>::purgeAggs()
    {
        tearDownAggs(m_data.m_aggs);
    }


template<PlanNodeType aggregateType> bool AggregateExecutor<aggregateType>::p_init()
{
    if ( ! initBase()) {
        return false;
    }
    return true;
}

template<PlanNodeType aggregateType>
bool AggregateExecutor<aggregateType>::p_execute()
{
    m_memoryPool.purge();
    VOLT_DEBUG("started AGGREGATE");
    assert(dynamic_cast<AggregatePlanNode*>(m_abstractNode));
    assert(dynamic_cast<TempTable*>(m_outputTable));
    VOLT_TRACE("input table\n%s", m_inputTable->debug().c_str());

    prepareFirstTuple();
    TableIterator it = m_inputTable->iterator();
    VOLT_TRACE("looping..");
    TableTuple cur(m_inputTable->schema());
    while (it.next(cur)) {
        if ( ! nextTuple(cur)) {
            return false;
        }
    }
    VOLT_TRACE("finalizing..");
    if ( ! finalize()) {
        return false;
    }
    VOLT_TRACE("finished");
    VOLT_TRACE("output table\n%s", m_outputTable->debug().c_str());

    return true;
}

}

#endif
