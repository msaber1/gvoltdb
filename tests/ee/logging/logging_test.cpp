/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
#include "common/executorcontext.hpp"
#include "logging/LogManager.h"
#include "logging/LogProxy.h"
#include <stdint.h>

voltdb::LoggerId loggerIds[] = {
        voltdb::LOGGERID_SQL,
        voltdb::LOGGERID_HOST
};
int numLoggers = 2;

voltdb::LogLevel logLevels[] = {
        voltdb::LOGLEVEL_ALL,
        voltdb::LOGLEVEL_TRACE,
        voltdb::LOGLEVEL_DEBUG,
        voltdb::LOGLEVEL_INFO,
        voltdb::LOGLEVEL_WARN,
        voltdb::LOGLEVEL_ERROR,
        voltdb::LOGLEVEL_FATAL,
        voltdb::LOGLEVEL_OFF
};
int numLogLevels = 8;

class TestProxy : public voltdb::LogProxy {

public:
    voltdb::LoggerId lastLoggerId;
    voltdb::LogLevel lastLogLevel;
    char *lastStatement;
    /**
     * Log a statement on behalf of the specified logger at the specified log level
     * @param LoggerId ID of the logger that received this statement
     * @param level Log level of the statement
     * @param statement null terminated UTF-8 string containing the statement to log
     */
    virtual void log(voltdb::LoggerId loggerId, voltdb::LogLevel level, const char *statement) const {
        const_cast<TestProxy*>(this)->lastLoggerId = loggerId;
        const_cast<TestProxy*>(this)->lastLogLevel = level;
        const_cast<TestProxy*>(this)->lastStatement = const_cast<char*>(statement);
    };

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

struct LoggingTest : public Test
{
    LoggingTest()
    : m_logProxy()
    , m_loggerHolder(&m_logProxy)
    // Initialize and implicitly install an executor context member
    // that is useless except to yield the top end's loggers.
    , m_logAccess(1, 1, NULL, &m_loggerHolder, NULL, false, "", 0) { }

    voltdb::LogManager& logManager() { return m_loggerHolder.getLogManager(); }


    TestProxy m_logProxy;
    MockTopend m_loggerHolder;
    voltdb::ExecutorContext m_logAccess;
};

TEST_F(LoggingTest, TestManagerSetLevels) {
    /**
     * Try and set the level for every logger to every possible level
     * and make sure they are loggable/not loggable as they should be.
     */
    for (int loggerIndex = 0; loggerIndex < numLoggers; loggerIndex++) {
        for (int levelIndex = 0; levelIndex < numLogLevels; levelIndex++) {
            int64_t logLevelsToSet = INT64_MAX;//OFF, log nothing
            int64_t mask = 7;
            int64_t logLevel = logLevels[levelIndex];
            logLevelsToSet &= ((~logLevel & mask) << (loggerIndex * 3) ^ INT64_MAX) ;
            logManager().setLogLevels(logLevelsToSet);
            for (int ii = 1; ii < numLogLevels - 1; ii++) { //Should never log to ALL or OFF
                for (int zz = 0; zz < numLoggers; zz++) {
                    if (zz == loggerIndex) {
                        if (ii >= levelIndex) {
                            ASSERT_TRUE(voltdb::ExecutorContext::logger(loggerIds[zz])->isLoggable(logLevels[ii]));
                        } else {
                            ASSERT_FALSE(voltdb::ExecutorContext::logger(loggerIds[zz])->isLoggable(logLevels[ii]));
                        }
                    } else {
                        ASSERT_FALSE(voltdb::ExecutorContext::logger(loggerIds[zz])->isLoggable(logLevels[ii]));
                    }
                }
            }
        }
    }
}

/**
 * Similar to the previous test but also check to make sure the LogProxy receives/does not receive log statements.
 */
TEST_F(LoggingTest, TestLoggerUsesProxyLevels) {
    for (int loggerIndex = 0; loggerIndex < numLoggers; loggerIndex++) {
        for (int levelIndex = 0; levelIndex < numLogLevels; levelIndex++) {
            int64_t logLevelsToSet = INT64_MAX;//OFF, log nothing
            int64_t mask = 7;
            int64_t logLevel = logLevels[levelIndex];
            logLevelsToSet &= ((~logLevel & mask) << (loggerIndex * 3) ^ INT64_MAX) ;
            logManager().setLogLevels(logLevelsToSet);
            for (int ii = 1; ii < numLogLevels - 1; ii++) { //Should never log to ALL or OFF
                for (int zz = 0; zz < numLoggers; zz++) {
                    m_logProxy.lastLoggerId = voltdb::LOGGERID_INVALID;
                    if (zz == loggerIndex) {
                        if (ii >= levelIndex) {
                            voltdb::ExecutorContext::logger(loggerIds[zz])->log(logLevels[ii], "foo");
                            ASSERT_NE(static_cast<int>(m_logProxy.lastLoggerId), voltdb::LOGGERID_INVALID);
                        } else {
                            voltdb::ExecutorContext::logger(loggerIds[zz])->log(logLevels[ii], "foo");
                            ASSERT_EQ(static_cast<int>(m_logProxy.lastLoggerId), voltdb::LOGGERID_INVALID);
                        }
                    } else {
                        voltdb::ExecutorContext::logger(loggerIds[zz])->log(logLevels[ii], "foo");
                        ASSERT_EQ(static_cast<int>(m_logProxy.lastLoggerId), voltdb::LOGGERID_INVALID);
                    }
                }
            }
        }
    }
}
int main() {
    return TestSuite::globalInstance()->runAll();
}
