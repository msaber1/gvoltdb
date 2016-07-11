/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;
import junit.framework.Test;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;

/**
 * JUnit framework plug-in class that allows VoltDB JUnit tests to cooperate
 * with VoltDB's build.xml ant script and other test tools to report unit test
 * suite progress and results in an expected format.
 *
 * WARNING: Don't necessarily expect this class to be referenced anywhere
 * from VoltDB JUnit test framework code -- it's LIVE code, regardless,
 * loaded by JUnit configuration code according to settings in build.xml.
 **/
public class VoltJUnitFormatter implements JUnitResultFormatter {
    /** Control variable for stack depth in reports of an unexpected
     *  error caught during a test run.
     *  Paul calls this a misfeature on the grounds that MORE info
     *  is better where actual errors are concerned, so it's enough
     *  to limit the reporting to ALL org.voltdb stack frames. **/
    private static final int REASONABLE_TEST_ERROR_EXCEPTION_STACK_FRAME_LIMIT = 3;

    /** Disabler for stack depth limits in reports of test failures which
     *  always originate from relatively shallow junit test code in which
     *  all org.voltdb frames are likely relevant. Showing all stack frames
     *  supports factoring out test framework support routines, freeing us
     *  from the traditional verbose inline boilerplate style of test code. **/
    private static final int NO_TEST_FAILURE_STACK_FRAME_LIMIT = 0;

    private static final String NO_TEST_RUNNING = "no-test-running";
    private PrintStream m_out = System.out;

    /** Stats for the currently running suite **/
    private JUnitTest m_junitTestSuite;
    private volatile String m_testName = NO_TEST_RUNNING;
    private volatile int m_tests;
    private volatile int m_errs;
    private volatile int m_failures;

    /** Current time as of the start of the suite run **/
    private long m_suiteStartTimeMillis;

    /** @see { @link #DurationUpdater } **/
    private DurationUpdater m_updateTask;

    /** Helper to generate periodic "signs of life" output to the console
     *  while waiting for long-running test suites. **/
    private class DurationUpdater extends TimerTask {
        static final int DURATION_UPDATE_PERIOD_MINUTES = 2;
        // An approximate elapsed duration based on the number of times
        // the periodic timer has run out. This will tend to drift when GC
        // or other actions delay the scheduled calls to { @link #run },
        // but it is likely to be accurate enough.
        final AtomicInteger m_elapsed_minutes = new AtomicInteger(0);
        final Timer m_updateTimer = new Timer("JUnitResultFormatter.DurationUpdater output thread - temporary");

        DurationUpdater() {
            int durationMs = DURATION_UPDATE_PERIOD_MINUTES * 60 * 1000;
            m_updateTimer.schedule(this, durationMs, durationMs);
        }

        @Override
        public void run() {
            int duration = m_elapsed_minutes.addAndGet(DURATION_UPDATE_PERIOD_MINUTES);
            synchronized (m_out) {
                m_out.println("    " + duration + " minutes have passed.");
                m_out.println("        Currently running: " + m_testName);
                m_out.println("        Current score T:" + m_tests + " F:" + m_failures + " E:" + m_errs);
                m_out.flush();
            }
        }
    }

    @Override
    public void setOutput(OutputStream outputStream) {
        m_out = new PrintStream(outputStream);
    }

    @Override
    public void setSystemError(String arg0) {
        //out.println("SYSERR: " + arg0);
    }

    @Override
    public void setSystemOutput(String arg0) {
        //out.println("SYSOUT: " + arg0);
    }

    @Override
    public void startTestSuite(JUnitTest suite) throws BuildException {
        m_junitTestSuite = suite;
        m_tests = m_errs = m_failures = 0;
        m_suiteStartTimeMillis = System.currentTimeMillis();

        synchronized (m_out) {
            m_out.println("  Running test suite" + relativeTestSuiteIndex() +
                    ": " + m_junitTestSuite.getName());
            m_out.flush();
        }
        // Print a message to the console every few minutes so you know
        // roughly how long a long test has been running.
        assert(m_updateTask == null);
        m_updateTask = new DurationUpdater();
    }

    @Override
    public void endTestSuite(JUnitTest suite) throws BuildException {
        // Destroy the temporary thread.
        m_updateTask.cancel();
        m_updateTask = null;

        synchronized (m_out) {
            m_out.flush();
            m_out.print((m_failures + m_errs > 0) ? "(FAIL-JUNIT) " : "             ");
            m_out.printf("Tests run: %3d, Failures: %3d, Errors: %3d, Time elapsed: %.2f sec\n",
                    m_tests, m_failures, m_errs,
                    (System.currentTimeMillis() - m_suiteStartTimeMillis) / 1000.0);
            m_out.flush();
        }
    }

    @Override
    public void startTest(Test test) {
        if (test == null) {
            m_testName = NO_TEST_RUNNING;
            return;
        }
        m_testName = test.toString();
        int nameEnd = m_testName.indexOf('(');
        if (nameEnd > 0) {
            m_testName = m_testName.substring(0, nameEnd);
        }
    }

    @Override
    public void endTest(Test test) {
        m_testName = NO_TEST_RUNNING;
        m_tests++;
    }

    @Override
    public void addError(Test test, Throwable caught) {
        m_errs++;
        synchronized (m_out) {
            m_out.println("    " + m_testName + " had an error:");
            m_out.println("    " + caught);
            StackTraceElement[] st = caught.getStackTrace();
            // Report SOME voltdb stack frames nearest the offending throw.
            // These SHOULD be from product code, BUT the test framework code
            // may be throwing uncaught exceptions where it arguably should be
            // catching the exceptions it throws and failing junit asserts instead.
            printVoltFrames(st, REASONABLE_TEST_ERROR_EXCEPTION_STACK_FRAME_LIMIT);
            m_out.flush();
        }
    }

    @Override
    public void addFailure(Test test, AssertionFailedError caught) {
        m_failures++;
        m_out.println("    " + m_testName + " failed an assertion.");
        StackTraceElement[] st = caught.getStackTrace();
        // Report ALL voltdb stack frames for test assertions.
        printVoltFrames(st, NO_TEST_FAILURE_STACK_FRAME_LIMIT);
        m_out.flush();
    }

    /** Try to get optional information from VoltDB's ant build script
     *  RE: progress against the current batch of unit test suites. **/
    private String relativeTestSuiteIndex() {
        String testSuiteIndexStr = System.getProperty("testsuiteindex");
        String testSuiteCountStr = System.getProperty("testsuitecount");
        try {
            long testSuiteIndex = Long.parseLong(testSuiteIndexStr);
            long testSuiteCount = Long.parseLong(testSuiteCountStr);
            return " " + testSuiteIndex + " of " + testSuiteCount;
        }
        catch (Exception e) {
            return "";
        }
    }

    /** Print an org.voltdb-specific subsection of a call stack **/
    private void printVoltFrames(StackTraceElement[] st, int voltFrameLimit) {
        String indicateOmittedFrames = "";
        int voltFrameCount = 0;
        for (StackTraceElement ste : st) {
            if (ste.getClassName().contains("org.voltdb")) {

                // Report one more voltdb frame -- with a prefix line if any
                // non-voltdb frames were skipped since the last voltdb frame.
                m_out.println(indicateOmittedFrames + formatStackFrame(ste));

                if (++voltFrameCount == voltFrameLimit) {
                    break;
                }
                // Any prior omitted frames have already been reported.
                indicateOmittedFrames = "";
            }
            else {
                // Prepare to indicate when all of the org.voltdb stack frames
                // are not contiguous. This could happen from voltdb code using
                // reflection or calling library code that uses voltdb classes
                // derived from non-voltdb library base classes.
                // This only needs to be reported before the NEXT voltdb frame
                // IF there is one.
                if (voltFrameCount > 0) {
                    indicateOmittedFrames = "        <Omitted intervening non-org.voltdb frame(s).>\n";
                }
            }
        }
    }

    private static String formatStackFrame(StackTraceElement ste) {
        return String.format("        %s(%s:%d)",
                ste.getClassName(), ste.getFileName(), ste.getLineNumber());
    }

}
