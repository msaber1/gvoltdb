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
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.CLIConfig;

import org.voltdb.client.*;
import org.voltdb.client.Client;

public class SimpleBenchmark {

    // Reference to the database connection we will use
    final Client client;
    final CounterConfig config;

    /**
     * Uses included {@link CLIConfig} class to declaratively state command line
     * options with defaults and validation.
     */
    static class CounterConfig extends CLIConfig {

        @CLIConfig.Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";
        @CLIConfig.Option(desc = "Max Counter Classes")
        int maxcounterclass = 10;
        @CLIConfig.Option(desc = "Max Counter Per Counter Classe")
        int maxcounterperclass = 10;
        @CLIConfig.Option(desc = "Max Counter Levels in a Class")
        int rolluptime = 2; // 2 Seconds;
        @CLIConfig.Option(desc = "Number of Threads")
        int numthreads = 5;
        @CLIConfig.Option(desc = "Total Increment Transaction Per Thread")
        int incrementtimes = 100;
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

    public SimpleBenchmark(CounterConfig config) {
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
    public void InitializeData() throws IOException, NoConnectionsException, ProcCallException {
        int maxCounters = config.maxcounterclass * config.maxcounterperclass;
        ClientResponse cresponse =
                client.callProcedure("CleanCounters");
        if (cresponse.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException(cresponse.getStatusString());
        }
        for (int i = 0; i < config.maxcounterclass; i++) {
            cresponse =
                    client.callProcedure("AddCounterClass", i);
            if (cresponse.getStatus() != ClientResponse.SUCCESS) {
                throw new RuntimeException(cresponse.getStatusString());
            }
        }
        // Add counters.
        long prev_cc = 0;
        for (int i = 0, par_idx = 0; i < maxCounters; i++, par_idx++) {
            long cc = (i / config.maxcounterperclass);
            boolean treeShift = false;
            if (prev_cc != cc) {
                par_idx = 0;
                prev_cc = cc;
                treeShift = true;
            } else {
                par_idx = i;
            }
            cresponse =
                    client.callProcedure("AddCounter", cc, i,
                    "Counter-" + i, config.rolluptime, treeShift ? (par_idx - 1) : (i -1));
            if (treeShift) {
                par_idx = i;
            }
        }
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Running Simple Benchmark");
        try {
            CounterConfig config = new CounterConfig();
            config.parse(SimpleBenchmark.class.getName(), args);

            SimpleBenchmark bmrk = new SimpleBenchmark(config);
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
            Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    class IncrementRunner implements Runnable {

        private Client client;
        private int maxCounters;
        private int maxCounterPerClass;

        public IncrementRunner(Client iclient, int max_counters, int max_counters_per_class) {
            client = iclient;
            maxCounters = max_counters;
            maxCounterPerClass = max_counters_per_class;
        }

        @Override
        public void run() {
            for (int i = 0; i < maxCounters * maxCounterPerClass; i++) {
                for (int j = 0; j < config.incrementtimes; j++) {
                    try {
                        long counter_class_id = (i / maxCounterPerClass);
                        ClientResponse response =
                                client.callProcedure("Increment", counter_class_id, i);

                        if (response.getStatus() != ClientResponse.SUCCESS) {
                            throw new RuntimeException(response.getStatusString());
                        }
                        if (j % 1000 == 0) {
                            System.out.printf(".");
                        }
                    } catch (IOException ex) {
                        Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (ProcCallException ex) {
                        Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                    }
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
            new Thread(new IncrementRunner(client, config.maxcounterclass, config.maxcounterperclass)).run();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
