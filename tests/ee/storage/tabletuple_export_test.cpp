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
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValueFactory.hpp"
#include "common/serializeio.h"
#include "common/ExportSerializeIo.h"
#include "common/ThreadLocalPool.h"

#include <cstdlib>

using namespace voltdb;

class TableTupleExportTest : public Test {

  protected:
    // some utility functions to verify
    size_t maxElSize(uint16_t column_count, bool useNullStrings=false);
    size_t serElSize(uint16_t column_count, uint8_t*, char*, bool nulls=false);
    void verSer(int, char*);

    TupleSchema* createSubsetTupleSchema(int column_count) {
        std::vector<ValueType> columnTypes(m_columnTypes);
        std::vector<int32_t> columnLengths(m_columnLengths);
        // Only use the specified number of columns from the full list.
        columnTypes.resize(column_count);
        columnLengths.resize(column_count);
        return TupleSchema::createTupleSchema(columnTypes, columnLengths);
    }

  private:
    ThreadLocalPool m_pool;

    std::vector<ValueType> m_columnTypes;
    std::vector<int32_t> m_columnLengths;

    void addToSchema(ValueType vt, int length) {
        m_columnTypes.push_back(vt);
        m_columnLengths.push_back(length);
    }

    void addToSchema(ValueType vt) {
        addToSchema(vt, TupleSchema::getTupleStorageSize(vt));
    }

  public:
    TableTupleExportTest() {
        // note that maxELSize() cares about the string tuple offsets..

        // set up a schema with each supported column type
        addToSchema(VALUE_TYPE_TINYINT);  // 0
        addToSchema(VALUE_TYPE_SMALLINT); // 1
        addToSchema(VALUE_TYPE_INTEGER);  // 2
        addToSchema(VALUE_TYPE_BIGINT);   // 3
        addToSchema(VALUE_TYPE_TIMESTAMP); // 4
        addToSchema(VALUE_TYPE_DECIMAL);   // 5

        // need explicit lengths for varchar columns
        addToSchema(VALUE_TYPE_VARCHAR, 15);  // 6
        addToSchema(VALUE_TYPE_VARCHAR, UNINLINEABLE_OBJECT_LENGTH * 2);  // 7
    }

    ~TableTupleExportTest() {
    }

};


// helper to make a schema, a tuple and calculate EL size
size_t
TableTupleExportTest::maxElSize(uint16_t column_count, bool useNullStrings)
{
    char buf[1024]; // tuple data
    TupleSchema *ts = createSubsetTupleSchema(column_count);
    TableTuple tt(buf, ts);

    // if the tuple includes strings, add some content
    // assuming all Export tuples were allocated for persistent
    // storage and choosing set* api accordingly here.
    if (ts->columnCount() > 6) {
        NValue nv = ValueFactory::getStringValue("ABCDEabcde"); // 10 char
        if (useNullStrings) {
            nv.free(); nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(6, nv, NULL);
        nv.free();
    }
    if (ts->columnCount() > 7) {
        NValue nv = ValueFactory::getStringValue("abcdeabcdeabcdeabcde"); // 20 char
        if (useNullStrings) {
            nv.free(); nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(7, nv, NULL);
        nv.free();
    }

    // The function under test!
    size_t sz = tt.maxExportSerializationSize();

    // and cleanup
    tt.freeObjectColumns();
    TupleSchema::freeTupleSchema(ts);

    return sz;
}

/*
 * Verify that the max tuple size returns expected result
 */
TEST_F(TableTupleExportTest, maxExportSerSize_tiny)
{
    // test schema by adding successive columns from the full list.
    size_t sz = 0;
    uint16_t column_count = 0;

    // just tinyint in schema
    sz = maxElSize(++column_count);
    EXPECT_EQ(8, sz);

    // tinyint + smallint
    sz = maxElSize(++column_count);
    EXPECT_EQ(16, sz);

    // + integer
    sz = maxElSize(++column_count);
    EXPECT_EQ(24, sz);

    // + bigint
    sz = maxElSize(++column_count);
    EXPECT_EQ(32, sz);

    // + timestamp
    sz = maxElSize(++column_count);
    EXPECT_EQ(40, sz);

    // + decimal
    sz = maxElSize(++column_count);
    EXPECT_EQ(40 + 4 + 1 + 1 + 38, sz);  // length, radix pt, sign, prec.

    // + first varchar
    sz = maxElSize(++column_count);
    EXPECT_EQ(84 + 14, sz); // length, 10 chars

    // + second varchar
    sz = maxElSize(++column_count);
    EXPECT_EQ(98 + 24, sz); // length, 20 chars
}

/*
 * Verify that the max tuple size returns expected result using null strings
 */
TEST_F(TableTupleExportTest, maxExportSerSize_withNulls)
{
    // test schema by adding successive columns from the full list.
    size_t sz = 0;
    uint16_t column_count = 0;

    // just tinyint in schema
    sz = maxElSize(++column_count);
    EXPECT_EQ(8, sz);

    // tinyint + smallint
    sz = maxElSize(++column_count);
    EXPECT_EQ(16, sz);

    // + integer
    sz = maxElSize(++column_count);
    EXPECT_EQ(24, sz);

    // + bigint
    sz = maxElSize(++column_count);
    EXPECT_EQ(32, sz);

    // + timestamp
    sz = maxElSize(++column_count);
    EXPECT_EQ(40, sz);

    // + decimal
    sz = maxElSize(++column_count);
    EXPECT_EQ(40 + 4 + 1 + 1 + 38, sz);  // length, radix pt, sign, prec.

    // + first varchar
    sz = maxElSize(++column_count, true);
    EXPECT_EQ(84, sz);

    // + second varchar
    sz = maxElSize(++column_count, true);
    EXPECT_EQ(84, sz);
}

// helper to make a schema, a tuple and serialize to a buffer
size_t
TableTupleExportTest::serElSize(uint16_t column_count, uint8_t *nullArray, char *dataPtr, bool nulls)
{
    char buf[1024]; // tuple data

    TupleSchema *ts = createSubsetTupleSchema(column_count);
    TableTuple tt(buf, ts);

    // assuming all Export tuples were allocated for persistent
    // storage and choosing set* api accordingly here.

    switch (ts->columnCount()) {
    // note my sophisticated and clever use of fall through
    case 8: {
        NValue nv = ValueFactory::getStringValue("abcdeabcdeabcdeabcde"); // 20 char
        if (nulls) {
            nv.free();
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(7, nv, NULL);
        nv.free();
    }
    case 7: {
        NValue nv = ValueFactory::getStringValue("ABCDEabcde"); // 10 char
        if (nulls) {
            nv.free();
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(6, nv, NULL);
        nv.free();
    }
    case 6: {
        NValue nv = ValueFactory::getDecimalValueFromString("-12.34");
        if (nulls) {
            nv.free();
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(5, nv, NULL);
        nv.free();
    }
    case 5: {
        NValue nv = ValueFactory::getTimestampValue(9999);
        if (nulls) {
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(4, nv, NULL);
        nv.free();
    }
    case 4: {
        NValue nv = ValueFactory::getBigIntValue(1024);
        if (nulls) {
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(3, nv, NULL);
        nv.free();
    }
    case 3: {
        NValue nv = ValueFactory::getIntegerValue(512);
        if (nulls) {
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(2, nv, NULL);
        nv.free();
    }
    case 2: {
        NValue nv = ValueFactory::getSmallIntValue(256);
        if (nulls) {
            nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(1, nv, NULL);
        nv.free();
    }
    case 1: {
        NValue nv = ValueFactory::getTinyIntValue(120);
        if (nulls) {
             nv.setNull();
        }
        tt.setNValueAllocateForObjectCopies(0, nv, NULL);
        nv.free();
        break;
    }
    default:
        // this is an error in the test fixture.
        EXPECT_EQ(0,1);
        break;
    }

    // The function under test!
    ExportSerializeOutput io(dataPtr, 2048);
    tt.serializeToExport(io, 0, nullArray);

    // and cleanup
    tt.freeObjectColumns();
    TupleSchema::freeTupleSchema(ts);
    return io.position();
}

// helper to verify the data that was serialized to the buffer
void
TableTupleExportTest::verSer(int cnt, char *data)
{
    assert(cnt > 0);
    ExportSerializeInput sin(data, 2048);

    if (--cnt >= 0)
    {
        int64_t v = sin.readLong();
        EXPECT_EQ(120, v);
    }
    if (--cnt >= 0)
    {
        int64_t v = sin.readLong();
        EXPECT_EQ(256, v);
    }
    if (--cnt >= 0)
    {
        EXPECT_EQ(512, sin.readLong());
    }
    if (--cnt >= 0)
    {
        EXPECT_EQ(1024, sin.readLong());
    }
    if (--cnt >= 0)
    {
        EXPECT_EQ(9999, sin.readLong());
    }
    if (--cnt >= 0)
    {
        EXPECT_EQ(16, sin.readInt());
        EXPECT_EQ('-', sin.readChar());
        EXPECT_EQ('1', sin.readChar());
        EXPECT_EQ('2', sin.readChar());
        EXPECT_EQ('.', sin.readChar());
        EXPECT_EQ('3', sin.readChar());
        EXPECT_EQ('4', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
    }
    if (--cnt >= 0)
    {
        EXPECT_EQ(10, sin.readInt());
        EXPECT_EQ('A', sin.readChar());
        EXPECT_EQ('B', sin.readChar());
        EXPECT_EQ('C', sin.readChar());
        EXPECT_EQ('D', sin.readChar());
        EXPECT_EQ('E', sin.readChar());
        EXPECT_EQ('a', sin.readChar());
        EXPECT_EQ('b', sin.readChar());
        EXPECT_EQ('c', sin.readChar());
        EXPECT_EQ('d', sin.readChar());
        EXPECT_EQ('e', sin.readChar());
    }
    if (--cnt >= 0)
    {
        EXPECT_EQ(20, sin.readInt());
        for (int ii =0; ii < 4; ++ii) {
            EXPECT_EQ('a', sin.readChar());
            EXPECT_EQ('b', sin.readChar());
            EXPECT_EQ('c', sin.readChar());
            EXPECT_EQ('d', sin.readChar());
            EXPECT_EQ('e', sin.readChar());
        }
    }
}

/*
 * Verify that tuple serialization produces expected content
 */
TEST_F(TableTupleExportTest, serToExport)
{
    uint8_t nulls[1] = {0};
    char data[2048];
    memset(data, 0, 2048);

    size_t sz = 0;
    uint16_t column_count = 0;

    // create a schema by selecting a column from the super-set.

    // tinyiny
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(8, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // tinyint + smallint
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(16, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // + integer
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(24, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // + bigint
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(32, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // + timestamp
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(40, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // + decimal
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(40 + 14 + 1 + 1 + 4, sz);  // length, radix pt, sign, prec.
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // + first varchar
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(60 + 14, sz); // length, 10 chars
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);

    // + second varchar
    sz = serElSize(++column_count, nulls, data);
    EXPECT_EQ(74 + 24, sz); // length, 20 chars
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(column_count, data);
}


/* verify serialization of nulls */
TEST_F(TableTupleExportTest, serWithNulls)
{
    uint8_t nulls[1] = {0};
    char data[2048];
    memset(data, 0, 2048);

    size_t sz = 0;
    uint16_t column_count = 0;

    // tinyiny
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80, nulls[0]);

    // tinyint + smallint
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40, nulls[0]);  // all null

    // + integer
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20, nulls[0]);  // all null

    // + bigint
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10, nulls[0]);  // all null

    // + timestamp
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8, nulls[0]);  // all null

    // + decimal
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz);  // length, radix pt, sign, prec.
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4, nulls[0]);  // all null

    // + first varchar
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz); // length, 10 chars
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4 | 0x2, nulls[0]);  // all null

    // + second varchar
    sz = serElSize(++column_count, nulls, data, true);
    EXPECT_EQ(0, sz); // length, 20 chars
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4 | 0x2 | 0x1, nulls[0]);  // all null
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
