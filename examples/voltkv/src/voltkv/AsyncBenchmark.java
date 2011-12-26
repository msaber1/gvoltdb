/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
 * This samples uses the native asynchronous request processing protocol
 * to post requests to the VoltDB server, thus leveraging to the maximum
 * VoltDB's ability to run requests in parallel on multiple database
 * partitions, and multiple servers.
 *
 * While asynchronous processing is (marginally) more convoluted to work
 * with and not adapted to all workloads, it is the preferred interaction
 * model to VoltDB as it guarantees blazing performance.
 *
 * Because there is a risk of 'firehosing' a database cluster (if the
 * cluster is too slow (slow or too few CPUs), this sample performs
 * self-tuning to target a specific latency (10ms by default).
 * This tuning process, as demonstrated here, is important and should be
 * part of your pre-launch evalution so you can adequately provision your
 * VoltDB cluster with the number of servers required for your needs.
 */
package voltkv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.IRateLimiter;
import org.voltdb.client.exampleutils.LatencyLimiter;
import org.voltdb.client.exampleutils.RateLimiter;

import voltkv.KVStore.KeyUpdateIntent;
import voltkv.KVStore.Response;

public class AsyncBenchmark
{
    // Initialize some common constants and variables
    private static final AtomicLongArray GetStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray GetCompressionResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutCompressionResults = new AtomicLongArray(2);

    // Reference to the database connection we will use
    private static ClientConnection Con;

    // Application entry point
    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(AsyncBenchmark.class.getCanonicalName())
                .add("display-interval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("pool-size", "pool_size", "Size of the pool of keys to work with (10,00, 10,000, 100,000 items, etc.).", 100000)
                .add("key-size", "key_size", "Size of the keys in number of characters. Max: 250", 50)
                .add("min-value-size", "min_value_size", "Minimum size for the value blob (in bytes, uncompressed). Max: 1048576", 1000)
                .add("max-value-size", "max_value_size", "Maximum size for the value blob (in bytes, uncompressed) - set equal to min-value-size for constant size. Max: 1048576", 1000)
                .add("entropy", "entropy", "How compressible the payload should be, lower is more compressible", 127)
                .add("use-compression", "use_compression", "Whether value blobs should be compressed (GZip) for storage in the database (true|false).", false)
                .add("rate-limit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .add("auto-tune", "auto_tune", "Flag indicating whether the benchmark should self-tune the transaction rate for a target execution latency (true|false).", "true")
                .add("latency-target", "latency_target", "Execution latency to target to tune transaction rate (in milliseconds).", 10.0d)
                .setArguments(args)
            ;

            // Retrieve parameters
            long displayInterval   = apph.longValue("display-interval");
            long duration          = apph.longValue("duration");
            String servers         = apph.stringValue("servers");
            int port               = apph.intValue("port");
            int poolSize           = apph.intValue("pool-size");
            boolean preload        = true;
            int keySize            = apph.intValue("key-size");
            int minValueSize       = apph.intValue("min-value-size");
            int maxValueSize       = apph.intValue("max-value-size");
            boolean useCompression = apph.booleanValue("use-compression");
            long rateLimit         = apph.longValue("rate-limit");
            boolean autoTune       = apph.booleanValue("auto-tune");
            double latencyTarget   = apph.doubleValue("latency-target");
            final String csv       = apph.stringValue("stats");
            final int entropy      = apph.intValue("entropy");

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("display-interval", (displayInterval > 0))
                .validate("pool-size", (poolSize > 0))
                .validate("key-size", (keySize > 0) && (keySize < 251))
                .validate("min-value-size", (minValueSize > 0) && (minValueSize < 1048576))
                .validate("max-value-size", (maxValueSize > 0) && (maxValueSize < 1048576) && (maxValueSize >= minValueSize))
                .validate("rate-limit", (rateLimit > 0))
                .validate("latency-target", (latencyTarget > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Get a client connection - we retry for a while in case the server hasn't started yet
            Con = ClientConnectionPool.getWithRetry(servers, port);
            KVStore store = new KVStore();
            store.init(Con.Client);

            // Get a payload generator to create random Key-Value pairs to store in the database and process (uncompress) pairs retrieved from the database.
            final PayloadProcessor processor = new PayloadProcessor(keySize, minValueSize, maxValueSize, entropy, poolSize, useCompression);

            // Initialize the store
            if (preload)
            {
                System.out.print("Initializing data store... ");
                for(int i=0;i<poolSize;i++) {
                    store.put(
                            String.format(processor.KeyFormat, i),
                            processor.generateForStore().getStoreValue(),
                            new KVStore.NullCallback());
                }
                Con.drain();
                System.out.println(" Done.");
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the operating procedures
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    System.out.print(Con.getStatistics("doTransaction"));
                }
            }
            , displayInterval*1000l
            , displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Pick the transaction rate limiter helping object to use based on user request (rate limiting or latency targeting)
            IRateLimiter limiter = null;
            if (autoTune)
                limiter = new LatencyLimiter(Con, "Get", latencyTarget, rateLimit);
            else
                limiter = new RateLimiter(rateLimit);

            final Semaphore maxTransactions = new Semaphore(300);
            // Run the benchmark loop for the requested duration
            long endTime = System.currentTimeMillis() + (1000l * duration);
            while (endTime > System.currentTimeMillis())
            {
                    maxTransactions.acquire();
                    /*
                     * Fool the client jigger into maintaning stats
                     */
                    final ClientConnection.TrackingCallback tracker =
                            new ClientConnection.TrackingCallback(
                                    Con,
                                    "doTransaction",
                                    null);

                    final KVStore.KeyLockIntent keys[] =
                            new KVStore.KeyLockIntent[] {
                                new KVStore.KeyLockIntent( processor.generateRandomKeyForRetrieval(), true),
                                new KVStore.KeyLockIntent( processor.generateRandomKeyForRetrieval(), true) };
                    while(keys[0].m_key.equals(keys[1].m_key)){
                        keys[1] = new KVStore.KeyLockIntent( processor.generateRandomKeyForRetrieval(), true);
                    }

                    final AtomicLong storeValueLength = new AtomicLong();
                    final AtomicLong rawValueLength = new AtomicLong();
                    // Get a two key/value pair, asynchronously and transactionally swap them
                    store.doTransaction(
                            Arrays.asList(keys),
                            new KVStore.KeyMunger() {
                                private int m_executionTimes = 0;

                                @Override
                                public List<KeyUpdateIntent> mungeKeys(
                                        Map<String, byte[]> keysMap) {
                                    m_executionTimes++;
                                    if(m_executionTimes > 1) {
                                        System.out.println("Had contention on a key, munger called " + m_executionTimes);
                                    }
                                    ArrayList<KeyUpdateIntent> updateIntents = new ArrayList<KeyUpdateIntent>(2);
                                    String key1 = null;
                                    String key2 = null;
                                    byte payload1[] = null;
                                    byte payload2[] = null;
                                    for (Map.Entry<String, byte[]> entry : keysMap.entrySet()) {
                                        if (key1 == null) {
                                            key1 = entry.getKey();
                                            payload1 = entry.getValue();
                                        } else {
                                            key2 = entry.getKey();
                                            payload2 = entry.getValue();
                                        }
                                    }

                                    assert(key1 != null && payload1 != null && key2 != null && payload2 != null);

                                    if (key1 == null || key2 == null) {
                                        System.out.println("Key 1 " + keys[0].m_key + " Key 2 " + keys[1].m_key);
                                        System.exit(-1);
                                    }
                                    /*
                                     * Swap the values
                                     */
                                    updateIntents.add(new KVStore.KeyUpdateIntent(key1, payload2));
                                    updateIntents.add(new KVStore.KeyUpdateIntent(key2, payload1));

                                    /*
                                     * Record stats for the data being retrieved
                                     */
                                    GetStoreResults.addAndGet(0, 2);
                                    final PayloadProcessor.Pair pair1 = processor.retrieveFromStore( null, payload1);
                                    final PayloadProcessor.Pair pair2 = processor.retrieveFromStore( null, payload2);
                                    storeValueLength.set(pair1.getStoreValueLength() + pair2.getStoreValueLength());
                                    rawValueLength.set(pair1.getRawValueLength() + pair2.getRawValueLength());
                                    GetCompressionResults.addAndGet(0, pair1.getStoreValueLength() + pair2.getStoreValueLength());
                                    GetCompressionResults.addAndGet(1, pair1.getRawValueLength() + pair2.getRawValueLength());
                                    return updateIntents;
                                }
                            },
                            new KVStore.Callback() {
                                @Override
                                public void response(Response r) {
                                    maxTransactions.release();
                                    ClientResponseImpl response =
                                            new ClientResponseImpl(
                                                    ClientResponse.SUCCESS,
                                                    Byte.MIN_VALUE, null, new VoltTable[]{}, null);
                                    response.setClientRoundtrip((int)r.rtt);
                                    response.setClusterRoundtrip((int)r.rtt);
                                    PutStoreResults.incrementAndGet(0);
                                    PutCompressionResults.addAndGet(0, storeValueLength.get());
                                    PutCompressionResults.addAndGet(1, rawValueLength.get());

                                    /*
                                     * To generate stats
                                     */
                                    tracker.clientCallback(response);
                            }});

                // Use the limiter to throttle client activity
                limiter.throttle();
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Now print application results:

            // 1. Store statistics as tracked by the application (ops counts, payload traffic)
            System.out.printf(
              "\n-------------------------------------------------------------------------------------\n"
            + " Store Results\n"
            + "-------------------------------------------------------------------------------------\n\n"
            + "A total of %,d operations was posted...\n"
            + " - GETs: %,9d Operations (%,9d Misses/Failures)\n"
            + "         %,9d MB in compressed store data\n"
            + "         %,9d MB in uncompressed application data\n"
            + "         Network Throughput: %6.3f Gbps*\n\n"
            + " - PUTs: %,9d Operations (%,9d Failures)\n"
            + "         %,9d MB in compressed store data\n"
            + "         %,9d MB in uncompressed application data\n"
            + "         Network Throughput: %6.3f Gbps*\n\n"
            + " - Total Network Throughput: %6.3f Gbps*\n\n"
            + "* Figure includes key & value traffic but not database protocol overhead.\n"
            + "\n"
            + "-------------------------------------------------------------------------------------\n"
            , GetStoreResults.get(0)+GetStoreResults.get(1)+PutStoreResults.get(0)+PutStoreResults.get(1)
            , GetStoreResults.get(0)
            , GetStoreResults.get(1)
            , GetCompressionResults.get(0)/1048576l
            , GetCompressionResults.get(1)/1048576l
            , ((double)GetCompressionResults.get(0) + (GetStoreResults.get(0)+GetStoreResults.get(1))*keySize)/(134217728d*duration)
            , PutStoreResults.get(0)
            , PutStoreResults.get(1)
            , PutCompressionResults.get(0)/1048576l
            , PutCompressionResults.get(1)/1048576l
            , ((double)PutCompressionResults.get(0) + (PutStoreResults.get(0)+PutStoreResults.get(1))*keySize)/(134217728d*duration)
            , ((double)GetCompressionResults.get(0) + (GetStoreResults.get(0)+GetStoreResults.get(1))*keySize)/(134217728d*duration)
            + ((double)PutCompressionResults.get(0) + (PutStoreResults.get(0)+PutStoreResults.get(1))*keySize)/(134217728d*duration)
            );

            // 2. Overall performance statistics for GET/PUT operations
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.getStatistics("doTransaction").toString(false));

            // 3. Per-procedure detailed performance statistics
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " Detailed Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.getStatistics().toString(false));

            // Dump statistics to a CSV file
            Con.saveStatistics(csv);

            Con.close();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
        }
    }
}
