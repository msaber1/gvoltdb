/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient.kinesis;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.Level;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;
import org.voltdb.importer.formatter.Formatter;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google_voltpatches.common.base.Optional;

/**
 * Importer implementation for pull socket importer. At runtime, there will
 * one instance of this per host and socket combination.
 */
public class KinesisStreamImporter extends AbstractImporter {

    private KinesisStreamImporterConfig m_config;
    private final AtomicBoolean m_eos = new AtomicBoolean(false);
    private volatile Optional<Thread> m_thread = Optional.absent();

    KinesisStreamImporter(KinesisStreamImporterConfig config)
    {
        m_config = config;
    }

    @Override
    public URI getResourceID()
    {
        return m_config.getResourceID();
    }

    @Override
    public void accept() {
        try {
        runImport();
        } catch(Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public void stop() {
        close();
    }

    @Override
    public String getName() {
        return "KinesisStreamImporter";
    }

    private void runImport() {
        System.out.println("*****Starting kinesis importer for " + getResourceID());
        if (m_eos.get()) return;

        AWSCredentialsProvider credentialsProvider = getCredentialsProvider();
        String workerId = String.valueOf(UUID.randomUUID());
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(m_config.getAppName(), m_config.getStreamName(), credentialsProvider, workerId)
            .withRegionName(m_config.getRegion())
            .withMaxRecords(10)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
            //.withCallProcessRecordsEvenForEmptyRecordList(true)
            .withCommonClientConfig(getClientConfigWithUserAgent());
        System.out.println("***Got kinesis client lib config");

        IRecordProcessorFactory recordProcessorFactory = new StreamProcessorFactory();

        // Create the KCL worker with the stock trade record processor factory
        Worker worker = new Worker(recordProcessorFactory, kclConfig);
        System.out.println("**Created worker");

        info(null, "Starting kinesis stream importer for " + m_config.getResourceID());
        m_thread = Optional.of(Thread.currentThread());
        while (!m_eos.get()) {
            try {
                System.out.println("***Calling worker.run");
                worker.run();
                System.out.println("***Done with worker.run");
            } catch (Throwable e) {
                e.printStackTrace();
                if (m_eos.get()) return;
                rateLimitedLog(Level.ERROR, e, "Error in Kinesis stream importer %s", m_config.getResourceID());
            }
        }

        info(null, "Stopping Kinesis stream importer for " + m_config.getResourceID());
    }
    
    public static AWSCredentialsProvider getCredentialsProvider() throws AmazonClientException {
        /*
         * The ProfileCredentialsProvider will return your [default] credential profile by
         * reading from the credentials file located at (~/.aws/credentials).
         */
        AWSCredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider = new ProfileCredentialsProvider("default");
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        return credentialsProvider;
    }
    
    public static ClientConfiguration getClientConfigWithUserAgent() {
        final ClientConfiguration config = new ClientConfiguration();
        final StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);

        // Separate fields of the user agent with a space
        userAgent.append(" ");
        // Append the application name followed by version number of the sample
        userAgent.append("voltdb");
        userAgent.append("/");
        userAgent.append("1.0.0");

        config.setUserAgent(userAgent.toString());

        return config;
    }

    public void close() {
        if (m_eos.compareAndSet(false, true) && m_thread.isPresent()) {
            m_thread.get().interrupt();
        }
    }
    
    private class StreamProcessorFactory implements IRecordProcessorFactory {
        
        @Override
        public IRecordProcessor createProcessor()
        {
            return new StreamProcessor();
        }
    }
    
    private class StreamProcessor implements IRecordProcessor {
        private String m_kinesisShardId;
        private final Formatter<String> m_formatter;

        public StreamProcessor() {
            m_formatter = m_config.getFormatterFactory().create();
        }
        
        @Override
        public void initialize(String shardId) {
            info(null, "Initializing kinesis record processor for %s for shard %s", m_config.getResourceID(), shardId);
            m_kinesisShardId = shardId;
            
        }

        @Override
        public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
            System.out.println("****ProcessRecords with " + (records==null ? 0 : records.size()));
            for (Record record : records) {
                String data = null;
                try {
                    data = new String(record.getData().array(), "UTF-8");
                } catch(UnsupportedEncodingException e) {
                    error(e, "Unexpected error trying to decode byte stream from %s on shard %s",
                            m_config.getResourceID(), m_kinesisShardId);
                    continue;
                }
                Invocation invocation = new Invocation(m_config.getProcedure(), m_formatter.transform(data));
                if (!callProcedure(invocation)) {
                    System.out.println("Failed to insert: " + data);
                    if (isDebugEnabled()) {
                        debug(null, "Failed to process Invocation possibly bad data: " + data);
                    }
                }
                System.out.println("Inserted: " + data);
            }
        }

        @Override
        public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
            info(null, "Shutting down record processor for %s for shard %s", m_config.getResourceID(), m_kinesisShardId);
            // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
            if (reason == ShutdownReason.TERMINATE) {
                checkpoint(checkpointer);
            }
        }
        
        private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
            info(null, "Checkpointing for %s for shard ", m_config.getResourceID(), m_kinesisShardId);
            try {
                checkpointer.checkpoint();
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                info(se, "Caught shutdown exception, skipping checkpoint.");
            } catch (ThrottlingException e) {
                // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
                error(e, "Caught throttling exception, skipping checkpoint.");
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                error(e, "Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.");
            }
        }
    }
}
