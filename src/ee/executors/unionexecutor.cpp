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

#include "unionexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/unionnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"

#include "boost/foreach.hpp"
#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"


namespace voltdb {

namespace detail {

struct SetOperator {
    typedef boost::unordered_set<TableTuple, TableTupleHasher, TableTupleEqualityChecker>
        TupleSet;
    typedef boost::unordered_map<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
        TupleMap;

    SetOperator(const std::vector<Table*>& input_tables, TempTable* output_table, bool is_all) :
        m_input_tables(input_tables), m_output_table(output_table), m_is_all(is_all)
        {}

    virtual ~SetOperator() {}

    void processTuples() {
        processTuplesDo();
    }

    static SetOperator* getSetOperator(UnionType unionType, const std::vector<Table*>& inputs, TempTable* output);

    std::vector<Table*> m_input_tables;

    protected:
        virtual void processTuplesDo() = 0;

        TempTable* m_output_table;
        bool m_is_all;
};

struct UnionSetOperator : public SetOperator {
    UnionSetOperator(const std::vector<Table*>& input_tables, TempTable* output_table, bool is_all) :
       SetOperator(input_tables, output_table, is_all)
       {}

    protected:
        void processTuplesDo();

    private:
        bool needToInsert(const TableTuple& tuple, TupleSet& tuples);

};

void UnionSetOperator::processTuplesDo() {

    // Set to keep candidate tuples.
    TupleSet tuples;

    //
    // For each input table, grab their TableIterator and then append all of its tuples
    // to our ouput table. Only distinct tuples are retained.
    //
    BOOST_FOREACH(Table* input_table, m_input_tables) {
        assert(input_table);
        TableIterator iterator = input_table->iterator();
        TableTuple tuple(input_table->schema());
        while (iterator.next(tuple)) {
            if (m_is_all || needToInsert(tuple, tuples)) {
                // we got tuple to insert
                m_output_table->insertTempTuple(tuple);
            }
        }
    }
}

inline
bool UnionSetOperator::needToInsert(const TableTuple& tuple, TupleSet& tuples) {
    bool result = tuples.find(tuple) == tuples.end();
    if (result) {
        tuples.insert(tuple);
    }
    return result;
}

struct TableSizeLess {
    bool operator()(const Table* t1, const Table* t2) const {
        return t1->activeTupleCount() < t2->activeTupleCount();
    }
};

struct ExceptIntersectSetOperator : public SetOperator {
    ExceptIntersectSetOperator(const std::vector<Table*>& input_tables, TempTable* output_table,
        bool is_all, bool is_except);

    protected:
        void processTuplesDo();

    private:
        void collectTuples(Table& input_table, TupleMap& tuple_map);
        void exceptTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
        void intersectTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);

        bool m_is_except;
};

ExceptIntersectSetOperator::ExceptIntersectSetOperator(
    const std::vector<Table*>& input_tables, TempTable* output_table, bool is_all, bool is_except) :
        SetOperator(input_tables, output_table, is_all), m_is_except(is_except) {
    if (!is_except) {
        // For intersect we want to start with the smallest table
        std::vector<Table*>::iterator minTableIt =
            std::min_element(m_input_tables.begin(), m_input_tables.end(), TableSizeLess());
        std::swap( m_input_tables[0], *minTableIt);
    }

}

void ExceptIntersectSetOperator::processTuplesDo() {
    // Map to keep candidate tuples. The key is the tuple itself
    // The value - tuple's repeat count in the final table.
    TupleMap tuples;

    // Collect all tuples from the first set
    assert(!m_input_tables.empty());
    Table* input_table = m_input_tables[0];
    collectTuples(*input_table, tuples);

    //
    // For each remaining input table, collect its tuple into a separate map
    // and substract/intersect it from/with the first one
    //
    TupleMap next_tuples;
    for (size_t ctr = 1, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
        next_tuples.clear();
        Table* input_table = m_input_tables[ctr];
        assert(input_table);
        collectTuples(*input_table, next_tuples);
        if (m_is_except) {
            exceptTupleMaps(tuples, next_tuples);
        } else {
            intersectTupleMaps(tuples, next_tuples);
        }
    }

    // Insert remaining tuples to our ouput table
    for (TupleMap::const_iterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        for (size_t i = 0; i < mapIt->second; ++i) {
            m_output_table->insertTuple(tuple);
        }
    }
}

void ExceptIntersectSetOperator::collectTuples(Table& input_table, TupleMap& tuple_map) {
    TableIterator iterator = input_table.iterator();
    TableTuple tuple(input_table.schema());
    while (iterator.next(tuple)) {
        TupleMap::iterator mapIt = tuple_map.find(tuple);
        if (mapIt == tuple_map.end()) {
            tuple_map.insert(std::make_pair(tuple, 1));
        } else if (m_is_all) {
            ++mapIt->second;
        }
    }
}

void ExceptIntersectSetOperator::exceptTupleMaps(TupleMap& map_a, TupleMap& map_b) {
    const static size_t zero_val(0);
    TupleMap::iterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMap::iterator it_b = map_b.find(it_a->first);
        if (it_b != map_b.end()) {
            it_a->second = (it_a->second > it_b->second) ?
                std::max(it_a->second - it_b->second, zero_val) : zero_val;
            if (it_a->second == zero_val) {
                it_a = map_a.erase(it_a);
            } else {
                ++it_a;
            }
        } else {
            ++it_a;
        }
    }
}

void ExceptIntersectSetOperator::intersectTupleMaps(TupleMap& map_a, TupleMap& map_b) {
    TupleMap::iterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMap::iterator it_b = map_b.find(it_a->first);
        if (it_b == map_b.end()) {
            it_a = map_a.erase(it_a);
        } else {
            it_a->second = std::min(it_a->second, it_b->second);
            ++it_a;
        }
    }
}

SetOperator* SetOperator::getSetOperator(UnionType unionType, const std::vector<Table*>& inputs, TempTable* output)
{
    switch (unionType) {
        case UNION_TYPE_UNION_ALL:
            return new UnionSetOperator(inputs, output, true);
        case UNION_TYPE_UNION:
            return new UnionSetOperator(inputs, output, false);
        case UNION_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator(inputs, output, true, true);
        case UNION_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator(inputs, output, false, true);
        case UNION_TYPE_INTERSECT_ALL:
            return new ExceptIntersectSetOperator(inputs, output, true, false);
        case UNION_TYPE_INTERSECT:
            return new ExceptIntersectSetOperator(inputs, output, false, false);
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", unionType);
            return NULL;
    }
}

} // namespace detail

UnionExecutor::UnionExecutor() : m_setOperator() { }

bool UnionExecutor::p_init()
{
    VOLT_TRACE("init Union Executor");

    UnionPlanNode* node = dynamic_cast<UnionPlanNode*>(m_abstractNode);
    assert(node);

    const std::vector<Table*>& inputTables = getInputTables();
    TempTable* output_temp_table = dynamic_cast<TempTable*>(getOutputTable());

    //
    // First check to make sure they have the same number of columns
    //
    assert(inputTables.size() > 1);
    for (int table_ctr = 1, table_cnt = (int)getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
        assert(inputTables[0]->columnCount() == inputTables[table_ctr]->columnCount());
    }

    m_setOperator = boost::shared_ptr<detail::SetOperator>(
        detail::SetOperator::getSetOperator(node->getUnionType(), inputTables, output_temp_table));
    return true;
}

void UnionExecutor::p_execute()
{
    m_setOperator->processTuples();
}

}
