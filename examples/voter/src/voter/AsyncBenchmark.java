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

/*
 * This implements the synchronous database interface used by
 * BenchmarkRunner to perform the tests.
 */

package voter;

import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.IRateLimiter;
import org.voltdb.client.exampleutils.LatencyLimiter;
import org.voltdb.client.exampleutils.PerfCounter;
import org.voltdb.client.exampleutils.RateLimiter;

import voter.PhoneCallGenerator.PhoneCall;

public class AsyncBenchmark implements BenchmarkDBInterface
{
    // Database connection for main thread
    private static ClientConnection connMain;

    private static long rateLimit       = 100000;
    private static Boolean autoTune     = true;
    private static double latencyTarget = 10.0d;

    private static class ThreadData
    {
        private final ClientConnection connection;
        private final IRateLimiter limiter;

        public ThreadData(ClientConnection connection, IRateLimiter limiter)
        {
            this.connection = connection;
            this.limiter = limiter;
        }

        public ClientConnection getConnection()
        {
            return this.connection;
        }

        public IRateLimiter getLimiter()
        {
            return this.limiter;
        }
    }

    private static class UpdateCallCallback implements ProcedureCallback
    {
        private final AtomicLongArray votingBoardResults;

        public UpdateCallCallback(AtomicLongArray votingBoardResults)
        {
            this.votingBoardResults = votingBoardResults;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception
        {
            // Track the result of the vote (Accepted, Rejected, Failure...)
            if (response.getStatus() == ClientResponse.SUCCESS)
            {
                this.votingBoardResults.incrementAndGet((int)response.getResults()[0].fetchRow(0).getLong(0));
            }
            else
            {
                this.votingBoardResults.incrementAndGet(3);
            }
        }
    }

    @Override
    public void initializeMain(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException
    {
        // Get a client connection - we retry for a while in case the server hasn't started yet
        try
        {
            AsyncBenchmark.connMain = ClientConnectionPool.getWithRetry(servers, port, "", "", false, 0, listener);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("AsyncBenchmark: Exception in initializeMain(): " + e);
        }
    }

    @Override
    public Object initializeThread(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException
    {
        try
        {
            // Reuse the main connection since we're asynchronous.
            // Provide an appropriate limiter based on the chosen options.
            IRateLimiter limiter = (AsyncBenchmark.autoTune
                    ? new LatencyLimiter(AsyncBenchmark.connMain, "Vote", latencyTarget, AsyncBenchmark.rateLimit)
                    : new RateLimiter(AsyncBenchmark.rateLimit));
            return new ThreadData(AsyncBenchmark.connMain, limiter);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("AsyncBenchmark: Exception in initializeThread(): " + e);
        }
    }

    @Override
    public void terminateMain()
    {
        try
        {
            AsyncBenchmark.connMain.close();
        }
        catch(Exception e)
        {
            System.err.println("AsyncBenchmark: Exception in terminateMain(): " + e);
        }
    }

    @Override
    public void terminateThread(Object data)
    {
        // Nothing to do here.
    }

    @Override
    public int getMaxContestants(int contestantCount, String ContestantNamesCSV) throws BenchmarkException
    {
        // Get a client connection - we retry for a while in case the server hasn't started yet
        try
        {
            return (int)AsyncBenchmark.connMain.execute("Initialize", contestantCount, ContestantNamesCSV).getResults()[0].fetchRow(0).getLong(0);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in initializeMain(): " + e);
        }
    }

    @Override
    public void updateCall(PhoneCall call, int maxVoteCount, Object data, AtomicLongArray votingBoardResults) throws BenchmarkException
    {
        ThreadData threadData = (ThreadData)data;
        if (threadData != null)
        {
            ClientConnection conn = threadData.getConnection();
            try
            {
                // Post the vote, asynchronously
                conn.executeAsync(new UpdateCallCallback(votingBoardResults),
                                  "Vote", call.phoneNumber, call.contestantNumber, maxVoteCount);

                // Use the limiter to throttle client activity
                threadData.getLimiter().throttle();
            }
            catch (Exception e)
            {
                votingBoardResults.incrementAndGet(3);
            }
        }
    }

    @Override
    public PerfCounter getStatistics(String name) throws BenchmarkException
    {
        try
        {
            return AsyncBenchmark.connMain.getStatistics("Vote");
        }
        catch (Exception e)
        {
            throw new BenchmarkException("AsyncBenchmark: Exception in getStatistics(): " + e);
        }
    }

    @Override
    public void saveStatistics(String csv) throws BenchmarkException
    {
        try
        {
            AsyncBenchmark.connMain.saveStatistics(csv);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("AsyncBenchmark: Exception in saveStatistics(): " + e);
        }
    }

    @Override
    public void callProcedure(String procName, boolean ignoreExceptions) throws BenchmarkException
    {
        try
        {
            AsyncBenchmark.connMain.execute(procName).getResults();
        }
        catch (Exception e)
        {
            if (!ignoreExceptions)
            {
                throw new BenchmarkException("AsyncBenchmark: Exception in callProcedure(): " + e);
            }
        }
    }

    @Override
    public void getResults(String procName, List<BenchmarkVote> results) throws BenchmarkException
    {
        try
        {
            VoltTable voltResults = AsyncBenchmark.connMain.execute(procName).getResults()[0];
            while (voltResults.advanceRow())
            {
                results.add(new BenchmarkVote(voltResults.getString(0), voltResults.getLong(2)));
            }
        }
        catch (Exception e)
        {
            throw new BenchmarkException("AsyncBenchmark: Exception in callProcedure(): " + e);
        }
    }

    @Override
    public void addOptions(AppHelper apph)
    {
        apph.add("rate-limit", "rate_limit",
                 "Rate limit to start from (number of transactions per second).",
                 AsyncBenchmark.rateLimit);
        apph.add("auto-tune", "auto_tune",
                 "Flag indicating whether the benchmark should self-tune the transaction rate for a target execution latency (true|false).",
                 AsyncBenchmark.autoTune.toString());
        apph.add("latency-target", "latency_target",
                 "Execution latency to target to tune transaction rate (in milliseconds).",
                 AsyncBenchmark.latencyTarget);
    }

    @Override
    public void getOptions(AppHelper apph)
    {
        AsyncBenchmark.rateLimit     = apph.longValue("rate-limit");
        AsyncBenchmark.autoTune      = apph.booleanValue("auto-tune");
        AsyncBenchmark.latencyTarget = apph.doubleValue("latency-target");
    }

    /**
     * @param args command line arguments
     */
    public static void main(String[] args)
    {
        BenchmarkRunner.run(new AsyncBenchmark(), args);
    }
}
