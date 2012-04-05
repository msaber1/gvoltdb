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
 * This implements the JDBC database interface used by
 * BenchmarkRunner to perform the tests.
 */

package voter;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.PerfCounter;
import org.voltdb.jdbc.IVoltDBConnection;

import voter.PhoneCallGenerator.PhoneCall;

public class JDBCBenchmark implements BenchmarkDBInterface
{
    // Database connection for main thread
    private static Connection connMain;

    // Worker thread data
    private static class ThreadData
    {
        // Database connection for worker threads
        private Connection connThread = null;
        // Prepared statement.
        private CallableStatement voteCS = null;

        public ThreadData()
        {
            // empty
        }

        public Connection getConnThread()
        {
            return this.connThread;
        }

        public void setConnThread(Connection connThread)
        {
            this.connThread = connThread;
        }

        public CallableStatement getVoteCS()
        {
            return this.voteCS;
        }

        public void setVoteCS(CallableStatement voteCS)
        {
            this.voteCS = voteCS;
        }
    }

    @Override
    public void initializeMain(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException
    {
        // We need only do this once, to "hot cache" the JDBC driver reference so the JVM may
        // realize it's there.
        try
        {
            Class.forName("org.voltdb.jdbc.Driver");
        }
        catch (ClassNotFoundException e1)
        {
            throw new BenchmarkException("VoltDB JDBC driver class not found");
        }

        // Prepare the JDBC URL for the VoltDB driver
        String url = "jdbc:voltdb://" + servers + ":" + port;

        // Get a client connection - we retry for a while in case the server hasn't started yet
        System.out.printf("Connecting to: %s\n", url);
        int sleep = 1000;
        while(true)
        {
            try
            {
                JDBCBenchmark.connMain = DriverManager.getConnection(url, "", "");
                break;
            }
            catch (Exception e)
            {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep/1000);
                try {Thread.sleep(sleep);} catch(Exception tie){/*ignore*/}
                if (sleep < 8000)
                    sleep += sleep;
            }
        }
    }

    @Override
    public Object initializeThread(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException
    {
        String url = "jdbc:voltdb://" + servers + ":" + port;
        try
        {
            ThreadData data = new ThreadData();
            data.setConnThread(DriverManager.getConnection(url, "", ""));
            data.setVoteCS(data.getConnThread().prepareCall("{call Vote(?,?,?)}"));
            return data;
        }
        catch (SQLException e)
        {
            throw new BenchmarkException("SyncBenchmark: SQL Exception in initializeThread(): " + e);
        }
    }

    @Override
    public void terminateMain()
    {
        try
        {
            JDBCBenchmark.connMain.close();
        }
        catch(Exception e)
        {
            System.err.println("JDBCBenchmark: SQL Exception in terminateMain(): " + e);
        }
    }

    @Override
    public void terminateThread(Object data)
    {
        ThreadData threadData = (ThreadData)data;
        if (threadData != null)
        {
            try
            {
                threadData.getConnThread().close();
            }
            catch(Exception e)
            {
                System.err.println("JDBCBenchmark: SQL Exception in terminateThread(): " + e);
            }
        }
    }

    @Override
    public int getMaxContestants(int contestantCount, String ContestantNamesCSV) throws BenchmarkException
    {
        // Initialize the application and return maxContestants.
        CallableStatement initializeCS;
        try
        {
            initializeCS = JDBCBenchmark.connMain.prepareCall("{call Initialize(?,?)}");
            initializeCS.setInt(1, contestantCount);
            initializeCS.setString(2, ContestantNamesCSV);
            return initializeCS.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new BenchmarkException("JDBCBenchmark: SQL Exception in initializeMain(): " + e);
        }
    }

    @Override
    public void updateCall(PhoneCall call, int maxVoteCount, Object data, AtomicLongArray votingBoardResults) throws BenchmarkException
    {
        ThreadData threadData = (ThreadData)data;
        if (threadData != null)
        {
            try
            {
                CallableStatement voteCS = threadData.getVoteCS();
                voteCS.setLong(1, call.phoneNumber);
                voteCS.setInt(2, call.contestantNumber);
                voteCS.setLong(3, maxVoteCount);
                votingBoardResults.incrementAndGet(voteCS.executeUpdate());
            }
            catch (SQLException e)
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
            return JDBCBenchmark.connMain.unwrap(IVoltDBConnection.class).getStatistics("Vote");
        }
        catch (SQLException e)
        {
            throw new BenchmarkException("JDBCBenchmark: SQL Exception in getStatistics(): " + e);
        }
    }

    @Override
    public void saveStatistics(String csv) throws BenchmarkException
    {
        try
        {
            JDBCBenchmark.connMain.unwrap(IVoltDBConnection.class).saveStatistics(csv);
        }
        catch (SQLException e)
        {
            throw new BenchmarkException("JDBCBenchmark: SQL Exception in saveStatistics(): " + e);
        }
        catch (IOException e)
        {
            throw new BenchmarkException("JDBCBenchmark: I/O Exception in saveStatistics(): " + e);
        }
    }

    @Override
    public void getResults(String procName, List<BenchmarkVote> results) throws BenchmarkException
    {
        try
        {
            CallableStatement resultsCS = JDBCBenchmark.connMain.prepareCall("{call " + procName + "}");
            ResultSet jdbcResults = resultsCS.executeQuery();
            while (jdbcResults.next())
            {
                results.add(new BenchmarkVote(jdbcResults.getString(1), jdbcResults.getLong(3)));
            }
        }
        catch (SQLException e)
        {
            throw new BenchmarkException("JDBC SQL exception: " + e);
        }
    }

    @Override
    public void callProcedure(String procName, boolean ignoreExceptions) throws BenchmarkException
    {
        try
        {
            JDBCBenchmark.connMain.prepareCall("{call " + procName + "}");
        }
        catch (SQLException e)
        {
            throw new BenchmarkException("JDBC SQL exception: " + e);
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
        BenchmarkRunner.run(new JDBCBenchmark(), args);
    }
}
