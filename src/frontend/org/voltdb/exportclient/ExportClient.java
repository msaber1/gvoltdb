package org.voltdb.exportclient;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;


/**
 * Pull and ack export data from a VoltDB cluster.
 */
public class ExportClient {

    // logging target for all export client log4j output
    static final VoltLogger LOG = new VoltLogger("ExportClient");

    // sleep time between advertisement polls when empty.
    private static final long QUIET_POLL_INTERVAL = 5000;

    // orders advertisements by the advertisement string.
    private static class AdvertisementComparator implements Comparator<Object[]> {
        @Override
        public int compare(Object[] o1, Object[] o2) {
            String genId1 = (String)o1[1];
            String genId2 = (String)o2[1];
            return genId1.compareTo(genId2);
        }
    }

    // unserviced advertisements (InetSocketAddress, String) pairs
    Set<Object[]> m_advertisements =
            Collections.synchronizedSet(
                    new TreeSet<Object[]>(new AdvertisementComparator()));

    // servers, configured and discovered
    private final List<InetSocketAddress> m_servers =
        new LinkedList<InetSocketAddress>();

    // pool of I/O workers
    private final ExecutorService m_workerPool =
        Executors.newFixedThreadPool(4);

    // data processor - accepts exported bytes as its input
    final ExportClientProcessorFactory m_processorFactory;

    /** Schedule an ack for a client stream connection */
    class CompletionEvent {
        private final String m_advertisement;
        private final InetSocketAddress m_server;
        private long m_ackedByteCount;

        CompletionEvent(String advertisement, InetSocketAddress server) {
            m_advertisement = advertisement;
            m_server = server;
        }

        void setProcessedByteCount(long byteCount) {
            m_ackedByteCount = byteCount;
        }

        public void run() {
            ExportClient.this.m_workerPool.execute(
                new ExportClientListingConnection(m_server,
                    ExportClient.this.m_advertisements,
                    m_advertisement, m_ackedByteCount));
        }
    }

    /** Testing processor that counts bytes and is its own factory */
    private static class NullProcessor
        implements ExportClientProcessor, ExportClientProcessorFactory
    {
        private long totalBytes = 0;
        private long lastLogged = 0;

        @Override
        public ExportClientProcessor factory() {
            return new NullProcessor();
        }

        @Override
        public ByteBuffer emptyBuffer() {
            return ByteBuffer.allocate(1024*1024*2);
        }

        @Override
        public void offer(String advertisement, ByteBuffer buf) {
            totalBytes += buf.limit();
            if (totalBytes > lastLogged + (1024*1024*5)) {
                lastLogged = totalBytes;
                LOG.info("Advertisement " + advertisement +
                        " read " + totalBytes +
                        ". Last read: " + buf.limit());
            }
        }

        @Override
        public void done(CompletionEvent completionEvent) {
            completionEvent.run();
        }
    }

    /** Create an export client that will push data through the processor */
    public ExportClient(ExportClientProcessorFactory processorFactory) {
        if (processorFactory == null) {
            throw new NullPointerException("Must provide a valid processor.");
        }
        m_processorFactory = processorFactory;
    }

    /** Loop forever reading advertisements and processing data channels */
    public void start() {
        try {
            // seed the advertisement pool
            for (InetSocketAddress s : m_servers) {
                m_workerPool.execute(new ExportClientListingConnection(s, m_advertisements));
            }

            while (true) {
                Object[] pair = null;
                Iterator<Object[]> it = m_advertisements.iterator();
                if (it.hasNext()) {
                    pair = it.next();
                }
                if (pair == null) {
                    // block for some period of time - don't spam the server.
                    Thread.sleep(QUIET_POLL_INTERVAL);
                    for (InetSocketAddress s : m_servers) {
                        m_workerPool.execute(new ExportClientListingConnection(s, m_advertisements));
                    }
                }
                else {
                    InetSocketAddress socket = (InetSocketAddress) pair[0];
                    String advertisement =  (String) pair[1];
                    m_workerPool.execute(
                        new ExportClientStreamConnection(socket,
                            advertisement,
                            new CompletionEvent(advertisement, socket),
                            m_processorFactory.factory()));
                }
            }
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    /** Add server to server configuration list */
    void addServerInfo(String server, boolean useAdminPort) {
        InetSocketAddress addr = null;
        int defaultPort = useAdminPort ? VoltDB.DEFAULT_ADMIN_PORT : VoltDB.DEFAULT_PORT;
        String[] parts = server.trim().split(":");
        if (parts.length == 1) {
            addr = new InetSocketAddress(parts[0], defaultPort);
        }
        else {
            assert(parts.length == 2);
            int port = Integer.parseInt(parts[1]);
            addr = new InetSocketAddress(parts[0], port);
        }
        m_servers.add(addr);
    }

    /** Read command line configuration and fire up an export client */
    public static void main(String[] args) {
        LOG.info("Starting export client with arguments: " + args.toString());
        ExportClient that = new ExportClient(new NullProcessor());
        String clusterip = args[0];
        that.addServerInfo(clusterip, false);
        that.start();
    }
}

