/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "storage/BigMemoryAllocator.h"

using namespace voltdb;

class AllocatorTest : public Test {
public:
    AllocatorTest() {
    }

    ~AllocatorTest() {
    }


};

TEST_F(AllocatorTest, AllocatorTestSimple) {
    void *first = BigMemoryAllocator::alloc(1024);
    ASSERT_NE(first, NULL);
    void *second = BigMemoryAllocator::alloc(1024);
    ASSERT_NE(second, NULL);
    void *third = BigMemoryAllocator::alloc(1024);
    ASSERT_NE(third, NULL);
    void *fourth = BigMemoryAllocator::alloc(1024);
    ASSERT_NE(fourth, NULL);

    ASSERT_EQ((char*)second - (char*)first, 1024);

    BigMemoryAllocator::free(third);
    BigMemoryAllocator::free(first);
    BigMemoryAllocator::free(fourth);

    void *next1 = BigMemoryAllocator::alloc(1024);
    ASSERT_EQ(next1, first);
    void *next2 = BigMemoryAllocator::alloc(1024);
    ASSERT_EQ(next2, third);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
