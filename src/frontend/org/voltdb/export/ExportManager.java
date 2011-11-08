/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.InputHandler;

/**
 * Provides interfaces to the export sub-system.
 * The EE interacts with export via pushExportBuffer.
 * ClientInterface uses ExportManager to create export client handlers.
 */
public class ExportManager
{
    public static final int DEFAULT_WINDOW_MS = 60 * 5 * 1000; // 5 minutes
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    public final ExportGenerationDirectory m_generationDirectory;
    private static ExportManager m_self;
    private final int m_hostId;

    /** Thrown if the initial setup of the loader fails */
    public static class SetupException extends Exception
    {
        private static final long serialVersionUID = 1L;
        private final String m_msg;
        SetupException(final String msg) {
            m_msg = msg;
        }
        @Override
        public String getMessage() {
            return m_msg;
        }
    }

    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     */
    public static synchronized void initialize(int myHostId, CatalogContext catalogContext, boolean isRejoin)
    throws ExportManager.SetupException
    {
        ExportGenerationDirectory exportWindowDirectory;
        try {
            exportWindowDirectory = new ExportGenerationDirectory(isRejoin, catalogContext);
        } catch (IOException e) {
            throw new ExportManager.SetupException(e.getMessage());
        }

        ExportManager tmp = new ExportManager(myHostId, catalogContext, exportWindowDirectory);
        m_self = tmp;
    }

    /**
     * Get the global instance of the ExportManager.
     * @return The global single instance of the ExportManager.
     */
    public static ExportManager instance() {
        assert (m_self != null);
        return m_self;
    }

    public static void setInstanceForTest(ExportManager self) {
        m_self = self;
    }


    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private ExportManager(int myHostId, CatalogContext catalogContext, ExportGenerationDirectory windowDirectory)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;
        m_generationDirectory = windowDirectory;

        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        if (conn == null) {
            exportLog.info("System is not using any export functionality.");
            return;
        }

        if (conn.getEnabled() == false) {
            exportLog.info("Export is disabled by user configuration.");
            return;
        }

        exportLog.info(String.format("Export is enabled and can overflow to %s.", cluster.getExportoverflow()));

        try {

            // add the disk generation(s) to the directory
            m_generationDirectory.initializePersistedWindows();

            // add the current in-memory generation to the directory
            File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());
            ExportGeneration currentGeneration =
                new ExportGeneration( catalogContext.m_transactionId, exportOverflowDirectory);
            currentGeneration.initializeGenerationFromCatalog(catalogContext, conn, m_hostId);
            m_generationDirectory.offer(catalogContext.m_transactionId, currentGeneration);
        }
        catch (final Exception e) {
            throw new ExportManager.SetupException(e.getMessage());
        }
    }

    public void shutdown() {
        m_generationDirectory.closeAllGenerations();
    }

    // extract generation id from a stream name.
    public static long getGenerationFromAdvertisement(String streamname) {
        try {
            String[] parts = streamname.split("-", 3);
            if (parts.length != 3) {
                return -1L;
            }
            return Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    // extract partition id from a stream name
    public static int getPartitionIdFromAdvertisement(String streamname) {
        try {
            String[] parts = streamname.split("-", 3);
            if (parts.length != 3) {
                return -1;
            }
            return Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // extract signature from a stream name
    public static String getSignatureFromStreamName(String streamname) {
        String[] parts = streamname.split("-", 3);
        if (parts.length != 3) {
            return "";
        }
        return parts[2];
    }

    // extract tablename from a stream name
    public static String getTableNameFromStreamName(String streamname) {
        String signature = getSignatureFromStreamName(streamname);
        String[] parts = signature.split("!");
        if (parts.length != 2) {
            return "";
        }
        return parts[0];
    }

    /**
     * Create ExportDataStreams for the specified stream name.
     * streamname: generationid-partitionid-signature
     */
    public InputHandler createExportStreamHandler(String streamname)
    {
        exportLog.info("Creating export data stream for " + streamname);
        long generationId = getGenerationFromAdvertisement(streamname);
        int partitionId = getPartitionIdFromAdvertisement(streamname);
        String signature = getSignatureFromStreamName(streamname);
        // String tablename = getTableNameFromStreamName(streamname);

        ExportGeneration gen = m_generationDirectory.get(generationId);
        if (gen == null) {
            exportLog.error("Rejecting export data stream. Generation " + generationId + " does not exist.");
            return null;
        }

        StreamBlockQueue sbq =
            gen.checkoutExportStreamBlockQueue(partitionId, signature);

        if (sbq == null) {
            exportLog.error("Rejecting export data stream. Stream " +  signature +
                " busy or not present in generation " + generationId);
            return null;
        }

        return new ExportStreamHandler(streamname, sbq);
    }

    public InputHandler createExportListingHandler() {
        return new ExportListingHandler();
    }

    public void ackStream(String streamname, long byteCount) throws IOException {
        long generationId = getGenerationFromAdvertisement(streamname);
        int partitionId = getPartitionIdFromAdvertisement(streamname);
        String signature = getSignatureFromStreamName(streamname);
        // String tablename = getTableNameFromStreamName(streamname);

        ExportGeneration gen = m_generationDirectory.get(generationId);
        if (gen != null) {
            gen.acknowledgeExportStreamBlockQueue(partitionId, signature, byteCount);
        }
    }

    public static long getQueuedExportBytes(int partitionId, String signature) {
        ExportManager instance = instance();
        try {
            long exportBytes = instance.m_generationDirectory.estimateQueuedBytes(partitionId, signature);
            return exportBytes;
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
        return 0;
    }

    /*
     * This method pulls double duty as a means of pushing export buffers
     * and "syncing" export data to disk. Syncing doesn't imply fsync, it just means
     * writing the data to a file instead of keeping it all in memory.
     * End of stream indicates that no more data is coming from this source (signature)
     * for this generation.
     */
    public static void pushExportBuffer(
            long generationId,
            int partitionId,
            int siteId,
            String signature,
            String[] columnNames,
            long uso,
            long bufferPtr,
            ByteBuffer buffer,
            boolean sync,
            boolean endOfStream)
    {
        // The EE sends the right export generation id. If this is the
        // first time the generation has been seen, make a new one.
        exportLog.info("pushExportBuffer: gen: " + generationId +
                ", sig: " + signature +
                ", uso: " + uso +
                ", endOfStream: " + endOfStream);

        ExportManager instance = instance();
        ExportGeneration generation = null;
        try {
            exportLog.info("Looked up generation: " + generationId +
                    ", found: " + instance.m_generationDirectory.get(generationId));

            synchronized(instance)
            {
                if ((generation = instance.m_generationDirectory.get(generationId)) == null)
                {
                    generation =
                        new ExportGeneration(generationId,
                                instance.m_generationDirectory.m_exportOverflowDirectory);

                    instance.m_generationDirectory.offer(generationId, generation);
                }
                generation.addDataSource(signature, partitionId, siteId, columnNames);
            }

            generation.pushExportBuffer(partitionId, signature, uso, bufferPtr, buffer, sync, endOfStream);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error("Failure to push export buffer for generation: " + generationId +
                    ", partition: " + partitionId, e);
        }
    }

    public void truncateExportToTxnId(long snapshotTxnId) {
        exportLog.info("Truncating export data after txnId " + snapshotTxnId);
        m_generationDirectory.truncateExportToTxnId(snapshotTxnId);
    }

}
