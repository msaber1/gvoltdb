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
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 *
 * The database interface, BenchmarkDBInterface, must be implemented by
 * the caller so that we can be neutral here as to whether it's done by
 * JDBC or the VoltDB library.
 */

package voter;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;

/**
 * @author scooper
 *
 */
public abstract class BenchmarkRunner
{
    // Initialize some common constants and variables
    private static final String contestantNamesCSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    private static final AtomicLongArray votingBoardResults = new AtomicLongArray(4);

    // Class for each thread that will be run in parallel, performing JDBC requests against the VoltDB server
    private static class BenchmarkClientThread implements Runnable
    {
        private final String servers;
        private final int port;
        private final long duration;
        private final PhoneCallGenerator switchboard;
        private final BenchmarkDBInterface dbInterface;
        private final int maxVoteCount;
        private final AtomicLongArray votingBoardResults;
        private final StatusListener listener;

        public BenchmarkClientThread(
                String servers,
                int port,
                PhoneCallGenerator switchboard,
                AtomicLongArray votingBoardResults,
                long duration,
                int maxVoteCount,
                BenchmarkDBInterface dbInterface,
                StatusListener listener) throws Exception
        {
            this.servers = servers;
            this.port = port;
            this.duration = duration;
            this.switchboard = switchboard;
            this.dbInterface = dbInterface;
            this.maxVoteCount = maxVoteCount;
            this.votingBoardResults = votingBoardResults;
            this.listener = listener;
        }

        @Override
        public void run()
        {
            try
            {
                Object data = this.dbInterface.initializeThread(this.servers, this.port, this.listener);
                try
                {
                    long endTime = System.currentTimeMillis() + (1000l * this.duration);
                    while (!this.listener.isDisconnected() && endTime > System.currentTimeMillis())
                    {
                        PhoneCallGenerator.PhoneCall call = this.switchboard.receive();
                        this.dbInterface.updateCall(call, this.maxVoteCount, data, this.votingBoardResults);
                    }
                }
                catch(BenchmarkException e)
                {
                    if (!this.listener.isDisconnected())
                    {
                        System.err.println("Exception: " + e);
                        e.printStackTrace();
                    }
                }
                finally
                {
                    this.dbInterface.terminateThread(data);
                }
            }
            catch(BenchmarkException e)
            {
                System.err.println("Exception: " + e);
                e.printStackTrace();
            }
        }
    }

    // Timer that prints out periodic statistics.
    private static class StatisticsTimer extends TimerTask
    {
        private final BenchmarkDBInterface dbInterface;

        public StatisticsTimer(BenchmarkDBInterface dbInterface)
        {
            this.dbInterface = dbInterface;
        }

        @Override
        public void run()
        {
            try
            {
                System.out.print(this.dbInterface.getStatistics("Vote"));
            }
            catch (BenchmarkException e)
            {
                System.err.println("Exception: " + e);
                e.printStackTrace();
            }
        }
    }

    // Status event listener/handler.
    private static class StatusListener extends ClientStatusListenerExt
    {
        // Set to true when all connections disappear. Concurrency is not an issue because it only
        // changes in one direction (false -> true).
        private static boolean disconnected = false;

        public StatusListener()
        {
            // empty
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft,
                                   DisconnectCause cause)
        {
            System.out.printf("* Database connection dropped. %d connections remain. *\n",
                              connectionsLeft);
            // Detect full disconnect.
            if (connectionsLeft == 0)
            {
                StatusListener.disconnected = true;
            }
        }

        @Override
        public void backpressure(boolean status)
        {
            System.out.println("* Database backpressure is delaying request processing. *");
        }

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e)
        {
            System.out.println("* Uncaught exception - see stack trace below. *");
            e.printStackTrace();
        }

        @Override
        public void lateProcedureResponse(ClientResponse response, String hostname, int port)
        {
            System.out.printf("* Timed out procedure on host %s:%d has now responded. *\n",
                              hostname, port);
        }

        public boolean isDisconnected() {
            return disconnected;
        }
    }

    private static class DelayedProcCallThread implements Runnable
    {
        private final String servers;
        private final int port;
        private final long sleepDuration;
        private final String strProc;
        private final boolean ignoreExceptions;
        private final BenchmarkDBInterface dbInterface;
        private final StatusListener listener;

        public DelayedProcCallThread(String servers, int port, long sleepDuration,
                                     String strProc, boolean ignoreExceptions,
                                     BenchmarkDBInterface dbInterface, StatusListener listener) throws Exception
        {
            this.servers = servers;
            this.port = port;
            this.sleepDuration = sleepDuration;
            this.strProc = strProc;
            this.ignoreExceptions = ignoreExceptions;
            this.dbInterface = dbInterface;
            this.listener = listener;
        }

        @Override
        public void run()
        {
            try
            {
                Thread.sleep(this.sleepDuration);
                Object data = this.dbInterface.initializeThread(this.servers, this.port, this.listener);
                try
                {
                    this.dbInterface.callProcedure(this.strProc, this.ignoreExceptions);
                }
                catch(BenchmarkException e)
                {
                    System.err.println("Exception: " + e);
                    e.printStackTrace();
                }
                finally
                {
                    this.dbInterface.terminateThread(data);
                }
            }
            catch (InterruptedException e)
            {
                System.err.println("<Interrupted>");
            }
            catch (BenchmarkException e)
            {
                System.err.println("Exception: " + e);
                e.printStackTrace();
            }
        }
    }

    public BenchmarkRunner()
    {
    }

    /**
     * @param args
     */
    public static void run(BenchmarkDBInterface dbInterface, String[] args)
    {
        String canonicalName = dbInterface.getClass().getCanonicalName();

        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(canonicalName)
                .add("threads", "thread_count", "Number of concurrent threads attacking the database.", 1)
                .add("display-interval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("contestants", "contestant_count", "Number of contestants in the voting contest (from 1 to 10).", 6)
                .add("max-votes", "max_votes_per_phone_number", "Maximum number of votes accepted for a given voter (phone number).", 2)
                .add("abort-after", "abort_after", "Shutdown server after the given number of seconds.", 0);

            dbInterface.addOptions(apph);
            apph.setArguments(args);

            // Retrieve parameters
            int threadCount      = apph.intValue("threads");
            long displayInterval = apph.longValue("display-interval");
            long duration        = apph.longValue("duration");
            String servers       = apph.stringValue("servers");
            int port             = apph.intValue("port");
            int contestantCount  = apph.intValue("contestants");
            int maxVoteCount     = apph.intValue("max-votes");
            int abortAfter       = apph.intValue("abort-after");
            final String csv     = apph.stringValue("stats");

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("display-interval", (displayInterval > 0))
                .validate("threads", (threadCount > 0))
                .validate("contestants", (contestantCount > 0))
                .validate("max-votes", (maxVoteCount > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            StatusListener listener = new StatusListener();
            dbInterface.initializeMain(servers, port, listener);

            int maxContestants = dbInterface.getMaxContestants(contestantCount, contestantNamesCSV);

            System.out.println("Connected.  Starting benchmark.");

            // Get a Phone Call Generator that will simulate voter entries from the call center
            PhoneCallGenerator switchboard = new PhoneCallGenerator(maxContestants);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the Vote procedure
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new StatisticsTimer(dbInterface),
                                      displayInterval*1000l,
                                      displayInterval*1000l);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++)
            {
                threads.add(new Thread(
                        new BenchmarkClientThread(servers, port, switchboard, votingBoardResults,
                                                  duration, maxVoteCount, dbInterface, listener)));
            }

            // Add auto-shutdown thread to break connection
            if (abortAfter > 0)
            {
                threads.add(new Thread(
                        new DelayedProcCallThread(servers, port, abortAfter*1000,
                                                  "@Shutdown", true, dbInterface, listener)));
            }

            // Start the threads
            for (Thread thread : threads)
            {
                thread.start();
            }

            // Wait for threads to complete
            for (Thread thread : threads)
            {
                thread.join();
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Now print application results (if it wasn't interrupted):

            if (!listener.isDisconnected())
            {

                // 1. Voting Board statistics, Voting results and performance statistics
                System.out.printf(
                  "-------------------------------------------------------------------------------------\n"
                + " Voting Results\n"
                + "-------------------------------------------------------------------------------------\n\n"
                + "A total of %d votes was received...\n"
                + " - %,9d Accepted\n"
                + " - %,9d Rejected (Invalid Contestant)\n"
                + " - %,9d Rejected (Maximum Vote Count Reached)\n"
                + " - %,9d Failed (Transaction Error)\n"
                + "\n\n"
                + "-------------------------------------------------------------------------------------\n"
                + "Contestant Name\t\tVotes Received\n"
                , dbInterface.getStatistics("Vote").getExecutionCount()
                , votingBoardResults.get(0)
                , votingBoardResults.get(1)
                , votingBoardResults.get(2)
                , votingBoardResults.get(3)
                );

                // 2. Voting results
                ArrayList<BenchmarkVote> result = new ArrayList<BenchmarkVote>();
                dbInterface.getResults("Results", result);
                String winner = "";
                long winnerVoteCount = 0;
                for (BenchmarkVote vote : result)
                {
                    if (vote.getCount() > winnerVoteCount)
                    {
                        winnerVoteCount = vote.getCount();
                        winner = vote.getName();
                    }
                    System.out.printf("%s\t\t%,14d\n", vote.getName(), vote.getCount());
                }
                System.out.printf("\n\nThe Winner is: %s\n-------------------------------------------------------------------------------------\n", winner);

                // 3. Performance statistics (we only care about the Vote procedure that we're benchmarking)
                System.out.println(
                  "\n\n-------------------------------------------------------------------------------------\n"
                + " System Statistics\n"
                + "-------------------------------------------------------------------------------------\n\n");
                System.out.print(dbInterface.getStatistics("Vote").toString(false));

                // Dump statistics to a CSV file
                dbInterface.saveStatistics(csv);

            }

            // Close the connection, etc.
            dbInterface.terminateMain();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
        }
    }

}
