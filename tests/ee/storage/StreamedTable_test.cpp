/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"
#include "storage_test_support.h"

#include "storage/streamedtable.h"

#include "boost/foreach.hpp"

using namespace std;
using namespace voltdb;

const int COLUMN_COUNT = 5;

class StreamedTableTest : public Test {
public:
    StreamedTableTest()
      : m_table(true)
    {
        // a simple helper around the constructor that sets the
        // wrapper buffer size to the specified value
        m_table.setCapacityForTest(1024);
    }

    virtual ~StreamedTableTest() { }

protected:
    StorageTestEnvironment m_env;
    StreamedTable m_table;
};

/**
 * The goal of this test is simply to run through the mechanics.
 * Fill a buffer repeatedly and make sure nothing breaks.
 */
TEST_F(StreamedTableTest, BaseCase) {
    const int64_t tokenOffset = 2000; // just so tokens != txnIds

    TableTuple& tuple = m_env.defaultTuple();
    
    // repeat for more tuples than fit in the default buffer
    for (int i = 1; i < 1000; i++) {

        // pretend to be a plan fragment execution
        m_env.nextQuantum(i, tokenOffset);

        // fill a tuple
        for (int col = 0; col < COLUMN_COUNT; col++) {
            int value = rand();
            tuple.setNValue(col, ValueFactory::getIntegerValue(value));
        }

        m_table.insertTuple(tuple);
    }
    // a negative flush implies "now". this helps valgrind heap block test
    m_table.flushOldTuples(-1);

    // poll from the table and make sure we get "stuff", releasing as
    // we go.  This just makes sure we don't fail catastrophically and
    // that things are basically as we expect.
    int64_t uso_expected = 0;
    BOOST_FOREACH(boost::shared_ptr<StreamBlock> block, m_env.m_topEnd.blocks) {
        int64_t uso = block->uso();
        EXPECT_EQ(uso_expected, uso);
        size_t offset = block->offset();
        EXPECT_TRUE(offset != 0);
        uso_expected += offset;
    }
    EXPECT_TRUE(uso_expected != 0); // should have found something.
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
