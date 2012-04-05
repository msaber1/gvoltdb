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
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.PerfCounter;

import voter.PhoneCallGenerator.PhoneCall;

public class SyncBenchmark implements BenchmarkDBInterface
{
    // Database connection for main thread
    private static ClientConnection connMain;

    @Override
    public void initializeMain(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException
    {
        // Get a client connection - we retry for a while in case the server hasn't started yet
        try
        {
            SyncBenchmark.connMain = ClientConnectionPool.getWithRetry(servers, port, "", "", false, 0, listener);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in initializeMain(): " + e);
        }
    }

    @Override
    public Object initializeThread(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException
    {
        try
        {
            return ClientConnectionPool.get(servers, port, "", "", false, 0, listener);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in initializeThread(): " + e);
        }
    }

    @Override
    public void terminateMain()
    {
        try
        {
            SyncBenchmark.connMain.close();
        }
        catch(Exception e)
        {
            System.err.println("SyncBenchmark: Exception in terminateMain(): " + e);
        }
    }

    @Override
    public void terminateThread(Object data)
    {
        try
        {
            ClientConnection conn = (ClientConnection)data;
            if (conn != null)
            {
                conn.close();
            }
        }
        catch(Exception e)
        {
            System.err.println("SyncBenchmark: Exception in terminateThread(): " + e);
        }
    }

    @Override
    public int getMaxContestants(int contestantCount, String ContestantNamesCSV) throws BenchmarkException
    {
        // Get a client connection - we retry for a while in case the server hasn't started yet
        try
        {
            return (int)SyncBenchmark.connMain.execute("Initialize", contestantCount, ContestantNamesCSV).getResults()[0].fetchRow(0).getLong(0);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in initializeMain(): " + e);
        }
    }

    @Override
    public void updateCall(PhoneCall call, int maxVoteCount, Object data, AtomicLongArray votingBoardResults) throws BenchmarkException
    {
        ClientConnection conn = (ClientConnection)data;
        if (conn != null)
        {
            try
            {
                long result = (int)conn.execute("Vote", call.phoneNumber, call.contestantNumber, maxVoteCount)
                                            .getResults()[0].fetchRow(0).getLong(0);
                votingBoardResults.incrementAndGet((int)result);
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
            return SyncBenchmark.connMain.getStatistics("Vote");
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in getStatistics(): " + e);
        }
    }

    @Override
    public void saveStatistics(String csv) throws BenchmarkException
    {
        try
        {
            SyncBenchmark.connMain.saveStatistics(csv);
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in saveStatistics(): " + e);
        }
    }

    @Override
    public void callProcedure(String procName, boolean ignoreExceptions) throws BenchmarkException
    {
        try
        {
            SyncBenchmark.connMain.execute(procName).getResults();
        }
        catch (Exception e)
        {
            if (!ignoreExceptions)
            {
                throw new BenchmarkException("SyncBenchmark: Exception in callProcedure(): " + e);
            }
        }
    }

    @Override
    public void getResults(String procName, List<BenchmarkVote> results) throws BenchmarkException
    {
        try
        {
            VoltTable voltResults = SyncBenchmark.connMain.execute(procName).getResults()[0];
            while (voltResults.advanceRow())
            {
                results.add(new BenchmarkVote(voltResults.getString(0), voltResults.getLong(2)));
            }
        }
        catch (Exception e)
        {
            throw new BenchmarkException("SyncBenchmark: Exception in callProcedure(): " + e);
        }
    }

    @Override
    public void addOptions(AppHelper apph)
    {
        // empty
    }

    @Override
    public void getOptions(AppHelper apph)
    {
        // empty
    }

    /**
     * @param args command line arguments
     */
    public static void main(String[] args)
    {
        BenchmarkRunner.run(new SyncBenchmark(), args);
    }
}
