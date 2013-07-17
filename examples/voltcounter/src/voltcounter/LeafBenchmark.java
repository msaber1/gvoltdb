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
package voltcounter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.CLIConfig;

import org.voltdb.client.*;
import org.voltdb.client.Client;
import static voltcounter.AsyncBenchmark.HORIZONTAL_RULE;

public class LeafBenchmark {

    // Reference to the database connection we will use
    final Client client;
    final CounterConfig config;

    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    AtomicLong countersIncremented = new AtomicLong(0);
    AtomicLong failedCountersIncremented = new AtomicLong(0);

    Map<Long, Long> counterToClass = new HashMap<Long, Long>();
    /**
     * Uses included {@link CLIConfig} class to declaratively state command line
     * options with defaults and validation.
     */
    static class CounterConfig extends CLIConfig {

        @CLIConfig.Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";
        @CLIConfig.Option(desc = "Max Counter Classes")
        int maxcounterclass = 500;
        @CLIConfig.Option(desc = "Max Depth of Counter Hierarchy")
        int depth = 4;
        @CLIConfig.Option(desc = "Max Leaf Nodes per Counter class")
        int leaves = 40;
        @CLIConfig.Option(desc = "Max Counter Levels in a Class")
        int rolluptime = 60;
        @CLIConfig.Option(desc = "Number of Threads")
        int numthreads = 10;
        @CLIConfig.Option(desc = "Total Increment Transaction Per Thread")
        int incrementtimes = 100;
        @CLIConfig.Option(desc = "Initialize Data?")
        boolean init = true;
        @CLIConfig.Option(desc = "Initialize Data And Quit?")
        boolean initonly = false;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Override
        public void validate() {
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff. No
     * timeout. This will run until the process is killed if it's not able to
     * connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            } catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000) {
                    sleep += sleep;
                }
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    public LeafBenchmark(CounterConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new LeafBenchmark.StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }

    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, ClientStatusListenerExt.DisconnectCause cause) {
            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Load up data
     *
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    public long InitializeData() throws IOException, NoConnectionsException, ProcCallException {
        if (config.init) {
            ClientResponse cresponse =
                    client.callProcedure("CleanCounters");
            if (cresponse.getStatus() != ClientResponse.SUCCESS) {
                throw new RuntimeException(cresponse.getStatusString());
            }
        }
        long counter_id = 0;
        for (long i = 0; i < config.maxcounterclass; i++) {
            if (config.init) {
                client.callProcedure(new NullCallback(), "AddCounterClass", i, "Class-" + i);
                // Add root counter in new class
                client.callProcedure(new NullCallback(), "AddCounter", i, counter_id,
                        "Counter-" + counter_id, config.rolluptime, -1);
            }
            counterToClass.put(counter_id, i);

            counter_id++;
            for (int j = 0; j < config.depth-1; j++) {
                if (config.init) {
                    client.callProcedure(new NullCallback(), "AddCounter", i, counter_id,
                            "Counter-" + counter_id, config.rolluptime, counter_id-1);
                }
                counterToClass.put(counter_id, i);
                counter_id++;
            }
            if (config.depth > 0) {
                for (int k = 0; k < config.leaves; k++) {
                    if (config.init) {
                        client.callProcedure(new NullCallback(), "AddCounter", i, counter_id+k,
                                "Counter-" + (counter_id+k), config.rolluptime, counter_id-1);
                    }
                    counterToClass.put(counter_id+k, i);
                }
                counter_id += config.leaves;
            }
        }
        return counter_id;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Running Leaf Benchmark");
        try {
            CounterConfig config = new CounterConfig();
            config.parse(LeafBenchmark.class.getName(), args);

            LeafBenchmark bmrk = new LeafBenchmark(config);
            bmrk.connect(config.servers);
            bmrk.InitializeData();
            if (!config.initonly) {
                bmrk.runIncrements();
            }
            // cancel periodic stats printing
            bmrk.timer.cancel();

            // block until all outstanding txns return
            bmrk.client.drain();
            try {
                // print the summary results
                bmrk.printResults();
            } catch (Exception ex) {
                Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
            }

            // close down the client connections
            bmrk.client.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ProcCallException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException ex) {
            Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" +
                         HORIZONTAL_RULE +
                         " Counter Results\n" +
                         HORIZONTAL_RULE +
                         "\n - %,9d Incremented\n" +
                         " - %,9d Failed to Increment\n\n";
        System.out.printf(display,
                countersIncremented.get(), failedCountersIncremented.get());


        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        if(this.config.latencyreport) {
            System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.1));
            System.out.printf("25th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.25));
            System.out.printf("50th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.5));
            System.out.printf("75th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.75));
            System.out.printf("90th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.9));
            System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
            System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));
            System.out.printf("99.5th percentile latency:     %,9d ms\n", stats.kPercentileLatency(.995));
            System.out.printf("99.9th percentile latency:     %,9d ms\n", stats.kPercentileLatency(.999));

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }
        // 4. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * Create a Timer task to display performance data on the Increment procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if(this.config.latencyreport) {
            System.out.printf(", Avg/95%% Latency %.2f/%dms", stats.getAverageLatency(),
                stats.kPercentileLatency(0.95));
        }
        System.out.printf("\n");
    }

    class IncrementRunner implements Runnable {

        private Client client;

        public IncrementRunner(Client iclient) {
            client = iclient;
        }

        @Override
        public void run() {
            for (Map.Entry pairs : counterToClass.entrySet()) {
                try {
                    for (int i = 0; i < config.incrementtimes; i++) {
                        client.callProcedure(new CounterIncrementCallback(), "Increment", pairs.getValue(), pairs.getKey());
                    }
                } catch (IOException ex) {
                    Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            cdLatch.countDown();
        }
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class CounterIncrementCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                long cnt = response.getResults()[0].asScalarLong();
                countersIncremented.addAndGet(cnt);
            }
            else {
                countersIncremented.incrementAndGet();
            }
        }
    }
    CountDownLatch cdLatch = null;
    /**
     *
     * @param client
     * @param maxCounters
     * @param maxCountersPerClass
     * @param maxLevels
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    public void runIncrements() throws IOException, NoConnectionsException, ProcCallException, InterruptedException {
        System.out.println("Starting Incrementing Counters.");
        cdLatch = new CountDownLatch(config.numthreads);
        for (int i = 0; i < config.numthreads; i++) {
            (new Thread(new IncrementRunner(client))).start();
        }
        cdLatch.await();
    }
}
