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

#include "storage/TempTableLimits.h"

#include "harness.h"
#include "common/executorcontext.hpp"
#include "common/SQLException.h"
#include "common/Topend.h"
#include "logging/LogManager.h"
#include "logging/LogProxy.h"

#include <sstream>

using namespace voltdb;
using namespace std;

class TestProxy : public LogProxy
{
public:
    LoggerId lastLoggerId;
    LogLevel lastLogLevel;
    char *lastStatement;
    /**
     * Log a statement on behalf of the specified logger at the specified log level
     * @param LoggerId ID of the logger that received this statement
     * @param level Log level of the statement
     * @param statement null terminated UTF-8 string containing the statement to log
     */
    virtual void log(LoggerId loggerId, LogLevel level,
                     const char *statement) const {
        //cout << "Logged.  ID: " << loggerId << ", level: " << level << ", statement: " << statement << endl;
        const_cast<TestProxy*>(this)->lastLoggerId = loggerId;
        const_cast<TestProxy*>(this)->lastLogLevel = level;
        const_cast<TestProxy*>(this)->lastStatement = const_cast<char*>(statement);
    };

    void reset()
    {
        lastLoggerId = LOGGERID_INVALID;
        lastLogLevel = LOGLEVEL_OFF;
        lastStatement = NULL;
    }
};

// Define a top end that is useless except to install custom log proxies.
struct MockTopend : public voltdb::Topend
{
    MockTopend(voltdb::LogProxy* logProxy) : Topend(logProxy) { }
    ~MockTopend() { }
    int loadNextDependency(int32_t dependencyId, voltdb::Pool *pool, voltdb::Table* destination) { return 0; }
    void crashVoltDB(const voltdb::FatalException& e) { }
    int64_t getQueuedExportBytes(int32_t partitionId, const std::string &signature) { return 0; }
    void pushExportBuffer(
            int64_t exportGeneration,
            int32_t partitionId,
            const std::string &signature,
            voltdb::StreamBlock *block,
            bool sync,
            bool endOfStream) { }
    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) { /* Do nothing */ }
};

struct TempTableLimitsTest : public Test
{
    // Initialize and implicitly install an executor context member
    // that is useless except to yield the top end's loggers.
    TempTableLimitsTest()
    : m_logProxy()
    , m_loggerHolder(&m_logProxy)
    // Initialize and implicitly install an executor context member
    // that is useless except to yield the top end's loggers.
    , m_logAccess(1, 1, NULL, &m_loggerHolder, NULL, NULL, false, "", 0)
    {
        m_loggerHolder.getLogManager().setLogLevels(0);
    }
    TestProxy m_logProxy;
    MockTopend m_loggerHolder;
    voltdb::ExecutorContext m_logAccess;
};

TEST_F(TempTableLimitsTest, CheckLogLatch)
{
    m_logProxy.reset();

    TempTableLimits dut;
    dut.setLogThreshold(1024 * 5);
    dut.setMemoryLimit(1024 * 10);
    // check that bump over threshold gets us logged
    dut.increaseAllocated(1024 * 6);
    EXPECT_EQ(m_logProxy.lastLoggerId, LOGGERID_SQL);
    EXPECT_EQ(m_logProxy.lastLogLevel, LOGLEVEL_INFO);
    m_logProxy.reset();
    // next bump still over does not, however
    dut.increaseAllocated(1024);
    EXPECT_EQ(m_logProxy.lastLoggerId, LOGGERID_INVALID);
    EXPECT_EQ(m_logProxy.lastLogLevel, LOGLEVEL_OFF);
    m_logProxy.reset();
    // dip below and back up, get new log
    dut.reduceAllocated(1024 * 3);
    dut.increaseAllocated(1024 * 2);
    EXPECT_EQ(m_logProxy.lastLoggerId, LOGGERID_SQL);
    EXPECT_EQ(m_logProxy.lastLogLevel, LOGLEVEL_INFO);
    m_logProxy.reset();
}

TEST_F(TempTableLimitsTest, CheckLimitException)
{
    m_logProxy.reset();

    TempTableLimits dut;
    dut.setLogThreshold(-1);
    dut.setMemoryLimit(1024 * 10);
    dut.increaseAllocated(1024 * 6);
    bool threw = false;
    try
    {
        dut.increaseAllocated(1024 * 6);
    }
    catch (SQLException& sqle)
    {
        threw = true;
    }
    EXPECT_TRUE(threw);
    // no logging with -1 threshold
    EXPECT_EQ(m_logProxy.lastLoggerId, LOGGERID_INVALID);
    EXPECT_EQ(m_logProxy.lastLogLevel, LOGLEVEL_OFF);
    m_logProxy.reset();
    // And check that we can dip below and rethrow
    dut.reduceAllocated(1024 * 6);
    threw = false;
    try
    {
        dut.increaseAllocated(1024 * 6);
    }
    catch (SQLException& sqle)
    {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
