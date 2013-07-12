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
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.CLIConfig;

import org.voltdb.client.*;
import org.voltdb.client.Client;

public class LeafBenchmark {

    // Reference to the database connection we will use
    final Client client;
    final CounterConfig config;

    Map<Long, Long> counterToClass = new HashMap<Long, Long>();
    /**
     * Uses included {@link CLIConfig} class to declaratively state command line
     * options with defaults and validation.
     */
    static class CounterConfig extends CLIConfig {

        @CLIConfig.Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";
        @CLIConfig.Option(desc = "Max Counter Classes")
        int maxcounterclass = 5;
        @CLIConfig.Option(desc = "Max Depth of Counter Hierarchy")
        int depth = 4;
        @CLIConfig.Option(desc = "Max Leaf Nodes per Counter class")
        int leaves = 10;
        @CLIConfig.Option(desc = "Max Counter Levels in a Class")
        int rolluptime = 60; // 2 Seconds;
        @CLIConfig.Option(desc = "Number of Threads")
        int numthreads = 1;
        @CLIConfig.Option(desc = "Total Increment Transaction Per Thread")
        int incrementtimes = 10;
        @CLIConfig.Option(desc = "Initialize Data?")
        boolean init = true;
        @CLIConfig.Option(desc = "Initialize Data And Quit?")
        boolean initonly = false;

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

        client = ClientFactory.createClient();

    }

    /**
     * Load up data
     *
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    public long InitializeData() throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cresponse =
                client.callProcedure("CleanCounters");
        if (cresponse.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException(cresponse.getStatusString());
        }
        long counter_id = 0;
        for (long i = 0; i < config.maxcounterclass; i++) {
            cresponse =
                    client.callProcedure("AddCounterClass", i, "Class-" + i);
            if (cresponse.getStatus() != ClientResponse.SUCCESS) {
                throw new RuntimeException(cresponse.getStatusString());
            }
            // Add root counter in new class
            cresponse =
                    client.callProcedure("AddCounter", i, counter_id,
                    "Counter-" + counter_id, config.rolluptime, -1);
            counterToClass.put(counter_id, i);

            counter_id++;
            for (int j = 0; j < config.depth-1; j++) {
                cresponse =
                        client.callProcedure("AddCounter", i, counter_id,
                        "Counter-" + counter_id, config.rolluptime, counter_id-1);
                counterToClass.put(counter_id, i);
                counter_id++;
            }
            for (int k = 0; k < config.leaves; k++) {
                cresponse =
                        client.callProcedure("AddCounter", i, counter_id+k,
                        "Counter-" + (counter_id+k), config.rolluptime, counter_id-1);
                counterToClass.put(counter_id+k, i);
            }
            counter_id += config.leaves;
        }
        return counter_id;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Running Simple Benchmark");
        try {
            CounterConfig config = new CounterConfig();
            config.parse(LeafBenchmark.class.getName(), args);

            LeafBenchmark bmrk = new LeafBenchmark(config);
            bmrk.connect(config.servers);
            if (config.init) {
                bmrk.InitializeData();
            }
            if (!config.initonly) {
                bmrk.runIncrements();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ProcCallException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException ex) {
            Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        }
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
                        ClientResponse response =
                                client.callProcedure("Increment", pairs.getValue(), pairs.getKey());

                        if (response.getStatus() != ClientResponse.SUCCESS) {
                            throw new RuntimeException(response.getStatusString());
                        }
                    }
                } catch (IOException ex) {
                    Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ProcCallException ex) {
                    Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

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
        for (int i = 0; i < config.numthreads; i++) {
            new Thread(new IncrementRunner(client)).run();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Logger.getLogger(LeafBenchmark.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
