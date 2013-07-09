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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;

import org.voltdb.client.*;
import org.voltdb.client.Client;

/**
 *
 * @author akhanzode
 */
public class SimpleBenchmark {

    private final static int TXNS = 100;
    private final static int THREADS = 5;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class CounterConfig extends CLIConfig {
        @CLIConfig.Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @CLIConfig.Option(desc = "Max Counter Classes")
        int maxCounterClass = 10;

        @CLIConfig.Option(desc = "Max Counter Per Counter Classe")
        int maxCounterPerClass = 100;

        @CLIConfig.Option(desc = "Max Counter Levels in a Class")
        int maxLevels = 100;

        @CLIConfig.Option(desc = "Max Counter Rollup Time")
        int rollupTime = 2; // 2 Seconds;

        @Override
        public void validate() {
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

            ClientConfig cconfig = new ClientConfig();
            cconfig.setClientAffinity(true);
            final Client client = ClientFactory.createClient(cconfig);

            client.createConnection(config.servers, Client.VOLTDB_SERVER_PORT);

            int maxCounterClass = config.maxCounterClass;
            int maxCounterPerClass = config.maxCounterPerClass;
            int maxLevels = config.maxLevels;
            int rollupTime = config.rollupTime;

            int maxCounters = maxCounterClass * maxCounterPerClass;
            ClientResponse cresponse =
                    client.callProcedure("CleanCounters");
            if (cresponse.getStatus() != ClientResponse.SUCCESS) {
                throw new RuntimeException(cresponse.getStatusString());
            }
            for (int i = 0; i < maxCounterClass; i++) {
                cresponse =
                        client.callProcedure("InitializeClass", i);
                if (cresponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException(cresponse.getStatusString());
                }
            }
            // Add counters.
            for (int i = 0, level = 0; i < maxCounters; i++) {
                long cc = (i / maxCounterPerClass);
                cresponse =
                        client.callProcedure("AddCounter", cc, i, "Counter-" + i, rollupTime, level++);
                if (level > maxLevels) {
                    level = 0;
                }
            }
            SimpleBenchmark bmrk = new SimpleBenchmark();
            bmrk.runIncrements(client, maxCounters, maxCounterPerClass, maxLevels);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ProcCallException e) {
            throw new RuntimeException(e);
        }
    }

    class IncrementRunner implements Runnable {

        private Client client;
        private int maxCounters;
        private int maxCounterPerClass;
        private int maxLevels;

        public IncrementRunner(Client iclient, int max_counters, int max_counters_per_class, int mlevel) {
            client = iclient;
            maxCounters = max_counters;
            maxCounterPerClass = max_counters_per_class;
            maxLevels = mlevel;
        }

        @Override
        public void run() {
            for (int i = 0, level = 0; i < maxCounters; i++) {
                for (int j = 0; j < SimpleBenchmark.TXNS; j++) {
                    try {
                        long counter_class_id = (i / maxCounterPerClass);
                        ClientResponse response =
                                client.callProcedure("GetCounter", counter_class_id, i);
                        if (level > maxLevels) {
                            level = 0;
                        }
                        if (response.getStatus() != ClientResponse.SUCCESS) {
                            throw new RuntimeException(response.getStatusString());
                        }
                        VoltTable results[] = response.getResults();
                        if (results[0].getRowCount() != 1) {
                            //Bad results.
                            System.out.println("Did not find Counter: " + i + " Class: " + counter_class_id);
                            continue;
                        }

                        VoltTable result = results[0];
                        result.advanceRow();
                        long value = result.getLong(3);
                        long rollup_seconds = result.getLong(4);
                        long last_update_time = result.getTimestampAsLong(5);
                        response =
                                client.callProcedure("Increment", counter_class_id, i, level++);

                        if (response.getStatus() != ClientResponse.SUCCESS) {
                            throw new RuntimeException(response.getStatusString());
                        }
                        String srollup_id = Long.toString(counter_class_id) + "-" + Long.toString(i);
                        response =
                                client.callProcedure("UpdateRollups", srollup_id, rollup_seconds, value, last_update_time);
                        if (j % 1000 == 0) {
                            System.out.printf(".");
                        }
                    } catch (IOException ex) {
                        Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (ProcCallException ex) {
                        Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                System.out.println(" completed " + SimpleBenchmark.TXNS + " transactions.");
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
    public void runIncrements(Client client, int maxCounters, int maxCountersPerClass, int maxLevels) throws IOException, NoConnectionsException, ProcCallException {
        for (int i = 0; i < SimpleBenchmark.THREADS; i++) {
            new Thread(new IncrementRunner(client, maxCounters, maxCountersPerClass, maxLevels)).run();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Logger.getLogger(SimpleBenchmark.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
