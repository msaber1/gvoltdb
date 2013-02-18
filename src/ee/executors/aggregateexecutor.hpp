/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

struct Distinct : public AggregateNValueSetType {
    bool excludeValue(const NValue& val)
    {
        // find this value in the set.  If it doesn't exist, add
        // it, otherwise indicate it shouldn't be included in the
        // aggregate
        iterator setval = find(val);
        if (setval == end())
        {
            insert(val);
            return false; // Include value just this once.
        }
        return true; // Never again this value;
    }
};

struct NotDistinct {
    void clear() { }
    bool excludeValue(const NValue& val)
    {
        return false; // Include value any number of times
    }
};

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

    Agg() : m_haveAdvanced(false)
    {
        m_value.setNull();
    }
    virtual ~Agg()
    {
        /* do nothing */
    }
    virtual void advance(const NValue& val) = 0;
    virtual NValue finalize() { return m_value; }
    virtual void purgeAgg() {};

protected:
    bool m_haveAdvanced;
    NValue m_value;
};

template<class D>
class SumAgg : public Agg
{
  public:
    SumAgg() {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
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

    virtual void purgeAgg() { ifDistinct.clear(); }

private:
    D ifDistinct;
};


template<class D>
class AvgAgg : public Agg
{
public:
    AvgAgg() : m_count(0) {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
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

    virtual NValue finalize()
    {
        if (m_count == 0)
        {
            return ValueFactory::getNullValue();
        }
        const NValue finalizeResult =
            m_value.op_divide(ValueFactory::getBigIntValue(m_count));
        return finalizeResult;
    }

    virtual void purgeAgg() { ifDistinct.clear(); }

private:
    D ifDistinct;
    int64_t m_count;
};

//count always holds integer
template<class D>
class CountAgg : public Agg
{
public:
    CountAgg() : m_count(0) {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
            return;
        }
        m_count++;
    }

    virtual NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

    virtual void purgeAgg() { ifDistinct.clear(); }

private:
    D ifDistinct;
    int64_t m_count;
};

class CountStarAgg : public Agg
{
public:
    CountStarAgg() : m_count(0) {}

    virtual void advance(const NValue& val)
    {
        ++m_count;
    }

    virtual NValue finalize()
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

    virtual void advance(const NValue& val)
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

    virtual void advance(const NValue& val)
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

private:
};


/*
 * Create an instance of an aggregator for the specified aggregate type and "distinct" flag.
 * The object is constructed in memory from the provided memrory pool.
 */
inline Agg* getAggInstance(Pool& memoryPool, ExpressionType agg_type, bool isDistinct)
{
    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
        return new (memoryPool) CountStarAgg();
    case EXPRESSION_TYPE_AGGREGATE_MIN:
        return new (memoryPool) MinAgg();
    case EXPRESSION_TYPE_AGGREGATE_MAX  :
        return new (memoryPool) MaxAgg();
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
        if (isDistinct) {
            return new (memoryPool) CountAgg<Distinct>();
        }
        return new (memoryPool) CountAgg<NotDistinct>();
    case EXPRESSION_TYPE_AGGREGATE_SUM:
        if (isDistinct) {
            return new (memoryPool) SumAgg<Distinct>();
        }
        return new (memoryPool) SumAgg<NotDistinct>();
    case EXPRESSION_TYPE_AGGREGATE_AVG:
        if (isDistinct) {
            return new (memoryPool) AvgAgg<Distinct>();
        }
        return new (memoryPool) AvgAgg<NotDistinct>();
    default:
    {
        char message[128];
        snprintf(message, sizeof(message), "Unknown aggregate type %d", agg_type);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    }
}


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
    virtual bool p_init()
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
    inline void insertOutputTuple(Agg** aggs, TableTuple groupedTuple);

    void advanceAggs(Agg** aggs, const TableTuple& nxtTuple)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            aggs[ii]->advance(m_inputExpressions[ii]->eval(&nxtTuple));
        }
    }

    /*
     * Create an instance of an aggregator for the specified aggregate type.
     * The object is constructed in memory from the provided memory pool.
     */
    void initAggInstances(Agg** aggs)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
        }
    }

    void purgeRowOfAggs(Agg** aggs)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            aggs[ii]->purgeAgg();
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
    TupleSchema* m_aggSchema;
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
inline void
AggregateExecutorBase::insertOutputTuple(Agg** aggs, TableTuple groupedTuple)
{
    PoolBackedTempTuple aggTuple;
    aggTuple.allocateTupleNoHeader(m_aggSchema, &m_memoryPool);

    // Collect the results of each aggregation as a column in a tuple.
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        const ValueType columnType = aggTuple.getType(ii);
        aggTuple.setNValue(ii, aggs[ii]->finalize().castAs(columnType));
    }

    // Produce the actual output columns by joining two tuples, the grouped/pass-through tuple
    // and the aggregated tuple.
    // The pass-through tuple is usually comprised of GROUP BY columns, but according to modern SQL standards,
    // they can be any value that is functionaly dependent on the GROUP BY keys, so that they do not violate
    // the Single-Value rule for GROUP BY. They could be included in the GROUP BY key processing, since they
    // wouldn't change the groupings, but it is more efficient to process the minimum number of key components.
    // To support such columns without factoring them into the GROUP BY key, a count of significant key columns
    // in the GROUP BY tuple would have to be tracked separately from the count of all columns and a specialized
    // TableTupleHasher and TableTupleEqualityChecker would be needed to consider only the significant columns.
    TableTuple& tmptup = m_outputTable->tempTuple();
    // TODO delegate to inline projection
    dynamic_cast<TempTable*>(m_outputTable)->insertTempTuple(tmptup);
    purgeRowOfAggs(aggs);
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
    ~AggregateExecutor() { }

protected:
    // This is implemented generically in terms of the three specialized functions and specialized state below.
    void p_execute();

private:
    // These specialized functions is implemented generically in terms of the three specialized functions below.
    void prepareFirstTuple();
    void aggregateNextTuple(TableTuple nxtTuple);
    void finalize();

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

template<> inline void AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>::aggregateNextTuple(TableTuple nxtTuple)
{
    AggregateList *aggregateList;
    // TODO: Here is where an inline projection executor could be used to initialize both a group key tuple
    // and an agg input tuple from the same raw input tuple.
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
}

template<> inline void AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>::finalize()
{
    for (HashAggregateMapType::const_iterator iter = m_data.begin(); iter != m_data.end(); iter++) {
        insertOutputTuple(iter->second->m_aggregates, iter->second->m_groupTuple);
    }
    m_data.clear();
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

template<> inline void AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>::aggregateNextTuple(TableTuple nxtTuple)
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
            // There's a chance that Micheal Alexeev has already solved this problem for the pullexec
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
        if ( ! m_data.m_prevTuple.isNullTuple()) {
            insertOutputTuple(m_data.m_aggs, m_data.m_prevTuple);
        }
        initAggInstances(m_data.m_aggs);
    }
    advanceAggs(m_data.m_aggs, nxtTuple);
    m_data.m_prevTuple.move(nxtTuple.address());
}

template<> inline void AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>::finalize() {
    // if no record exists in input_table, we have to output one record
    // only when it doesn't have GROUP BY. See difference of these cases:
    //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
    //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple
    if (m_data.m_prevTuple.isNullTuple()) {
        if (m_groupByExpressions.size() != 0) {
            purgeRowOfAggs(m_data.m_aggs);
            return;
        }
        VOLT_TRACE("no record. outputting a NULL row..");
    }
    insertOutputTuple(m_data.m_aggs, m_data.m_prevTuple);
}

template<PlanNodeType aggregateType>
void AggregateExecutor<aggregateType>::p_execute()
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
        aggregateNextTuple(cur);
    }
    VOLT_TRACE("finalizing..");
    finalize();
    VOLT_TRACE("AGGREGATE finished output table\n%s", m_outputTable->debug().c_str());
}

}

#endif
