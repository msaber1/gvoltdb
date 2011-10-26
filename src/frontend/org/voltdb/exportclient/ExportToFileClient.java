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

package org.voltdb.exportclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/**
 * Uses the Export feature of VoltDB to write exported tables to files.
 *
 * command line args: --servers {comma-separated list of VoltDB server to which to connect} --type [csv|tsv] csv for
 * comma-separated values, tsv for tab-separated values --outdir {path where output files should be written} --nonce
 * {string-to-unique-ify output files} --user {username for cluster export user} --password {password for cluster export
 * user} --period {period (in minutes) to use when rolling the file over} --dateformat {format of the date/time stamp
 * added to each new rolling file}
 *
 */
public class ExportToFileClient {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    // Folders/files being written are named with this prefix
    private static final String ACTIVE_PREFIX = "active-";

    // SimpleDataFormat is not threadsafe
    protected final ThreadLocal<SimpleDateFormat> m_dateformat;

    protected final HashMap<Long, HashMap<String, ExportToFileDecoder>> m_tableDecoders;

    // configuration parsed from command line parameters
    protected final FileClientConfiguration m_cfg;

    // timer used to roll batches
    protected final Timer m_timer = new Timer();

    // record the original command line args for servers
    protected final List<String> m_commandLineServerArgs = new ArrayList<String>();

    protected final Set<String> m_globalSchemasWritten = new HashSet<String>();

    protected PeriodicExportContext m_current = null;

    protected final Object m_batchLock = new Object();

    class PeriodicExportContext {
        final File m_dirContainingFiles;
        final Map<FileHandle, CSVWriter> m_writers = new TreeMap<FileHandle, CSVWriter>();
        boolean m_hasClosed = false;
        protected final Date start;
        protected Date end = null;
        protected HashSet<ExportToFileDecoder> m_decoders = new HashSet<ExportToFileDecoder>();
        protected final Set<String> m_batchSchemasWritten = new HashSet<String>();

        class FileHandle implements Comparable<FileHandle> {
            final String tableName;
            final long generation;

            FileHandle(String tableName, long generation) {
                this.tableName = tableName;
                this.generation = generation;
            }

            String getPath(String prefix) {
                if (ExportToFileClient.this.m_cfg.batched()) {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           generation +
                           "-" +
                           tableName +
                           ExportToFileClient.this.m_cfg.extension();
                }
                else {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           prefix +
                           ExportToFileClient.this.m_cfg.nonce() +
                           "-" +
                           generation +
                           "-" +
                           tableName +
                           "-" +
                           m_dateformat.get().format(start) +
                           ExportToFileClient.this.m_cfg.extension();
                }
            }

            String getPathForSchema() {
                if (ExportToFileClient.this.m_cfg.batched()) {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           generation +
                           "-" +
                           tableName +
                           "-schema.json";
                }
                else {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           ExportToFileClient.this.m_cfg.nonce() +
                           "-" +
                           generation +
                           "-" +
                           tableName +
                           "-schema.json";
                }
            }

            @Override
            public int compareTo(FileHandle obj) {
                int first = tableName.compareTo(obj.tableName);
                if (first != 0) return first;
                long second = generation - obj.generation;
                if (second > 0) return 1;
                if (second < 0) return -1;
                return 0;
            }
        }

        PeriodicExportContext(Date batchStart) {
            start = batchStart;

            if (ExportToFileClient.this.m_cfg.batched()) {
                m_dirContainingFiles = new File(getPathOfBatchDir(ACTIVE_PREFIX));
                m_logger.trace(String.format("Creating dir for batch at %s", m_dirContainingFiles.getPath()));
                m_dirContainingFiles.mkdirs();
                if (m_dirContainingFiles.exists() == false) {
                    m_logger.error("Error: Unable to create batch directory at path: " + m_dirContainingFiles.getPath());
                    throw new RuntimeException("Unable to create batch directory.");
                }
            }
            else {
                m_dirContainingFiles = ExportToFileClient.this.m_cfg.outDir();
            }
        }

        String getPathOfBatchDir(String prefix) {
            assert(ExportToFileClient.this.m_cfg.batched());
            return ExportToFileClient.this.m_cfg.outDir().getPath() + File.separator + prefix +
                   ExportToFileClient.this.m_cfg.nonce() + "-" + m_dateformat.get().format(start);
        }

        /**
         * Release a hold on the batch. When all holds are done
         * and the roll time has passed, the batch can move out
         * of the active state.
         */
        void decref(ExportToFileDecoder decoder) {
            synchronized (m_batchLock) {
                m_decoders.remove(decoder);
                if ((end != null) && (m_decoders.size() == 0)) {
                    closeAllWriters();
                }
            }
        }

        /**
         * Flush and close all active writers, allowing the batch to
         * move from the active state into the finished state. This is
         * done as part of the roll process, or as part of the
         * unexpected shutdown process.
         *
         * Note this method is idempotent.
         *
         * @param clean True if we expect all writer have finished. False
         * if we just need to be done.
         */
        void closeAllWriters() {
            // only need to run this once per batch
            if (m_hasClosed) return;

            // flush and close any files that are open
            for (Entry<FileHandle, CSVWriter> entry : m_writers.entrySet()) {
                CSVWriter writer = entry.getValue();
                if (writer == null) continue;
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (ExportToFileClient.this.m_cfg.batched())
                closeBatch();
            else
                closeFiles();

            // empty the writer set (probably not needed)
            m_writers.clear();

            // note that we're closed now
            m_hasClosed = true;
        }

        void closeBatch() {
            // rename the file appropriately
            m_logger.trace("Renaming batch.");

            String oldPath = getPathOfBatchDir(ACTIVE_PREFIX);
            String newPath = getPathOfBatchDir("");

            File oldDir = new File(oldPath);
            assert(oldDir.exists());
            assert(oldDir.isDirectory());
            assert(oldDir.canWrite());

            if (oldDir.listFiles().length > 0) {
                File newDir = new File(newPath);
                oldDir.renameTo(newDir);
            }
            else {
                oldDir.delete();
            }
        }

        void closeFiles() {
            File[] notifySet = new File[m_writers.size()];

            int i = 0;
            for (Entry<FileHandle, CSVWriter> e : m_writers.entrySet()) {
                FileHandle handle = e.getKey();

                String oldPath = handle.getPath(ACTIVE_PREFIX);
                String newPath = handle.getPath("");

                File oldFile = new File(oldPath);
                assert(oldFile.exists());
                assert(oldFile.isFile());
                assert(oldFile.canWrite());

                File newFile = new File(newPath);
                assert(!newFile.exists());
                oldFile.renameTo(newFile);

                notifySet[i] = newFile;
                i++;
            }
        }

        CSVWriter getWriter(String tableName, long generation) {
            synchronized (m_batchLock) {
                char[] fullDelimiters = ExportToFileClient.this.m_cfg.fullDelimiters();
                char delimiter = ExportToFileClient.this.m_cfg.delimiter();

                FileHandle handle = new FileHandle(tableName, generation);
                CSVWriter writer = m_writers.get(handle);
                if (writer != null)
                    return writer;

                String path = handle.getPath(ACTIVE_PREFIX);
                File newFile = new File(path);
                if (newFile.exists()) {
                    m_logger.error("Error: Output file for next period already exists at path: " + newFile.getPath());
                    m_logger.error("Consider using a more specific timestamp in your filename or cleaning up your export data directory.");
                    m_logger.error("ExportToFileClient will stop to prevent data loss.");
                    throw new RuntimeException();
                }
                try {
                    OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, false), "UTF-8");
                    if (fullDelimiters != null) {
                        writer = new CSVWriter(new BufferedWriter(osw, 1048576),
                                fullDelimiters[0], fullDelimiters[1], fullDelimiters[2], String.valueOf(fullDelimiters[3]));
                    }
                    else if (delimiter == ',')
                        // CSV
                        writer = new CSVWriter(new BufferedWriter(osw, 1048576), delimiter);
                    else {
                        // TSV
                        writer = CSVWriter.getStrictTSVWriter(new BufferedWriter(osw, 1048576));
                    }
                }
                catch (Exception e) {
                    m_logger.error(e.getMessage());
                    m_logger.error("Error: Failed to create output file: " + path);
                    throw new RuntimeException();
                }
                m_writers.put(handle, writer);
                return writer;
            }
        }

        void writeSchema(String tableName, long generation, String schema) {
            // if no schema's enabled pretend like this worked
            if (ExportToFileClient.this.m_cfg.withSchema()) {
                return;
            }

            FileHandle handle = new FileHandle(tableName, generation);
            String path = handle.getPathForSchema();

            // only write the schema once per batch
            if (ExportToFileClient.this.m_cfg.batched()) {
                if (m_batchSchemasWritten.contains(path)) {
                    return;
                }
            }
            else {
                if (m_globalSchemasWritten.contains(path)) {
                    return;
                }
            }

            File newFile = new File(path);
            try {
                OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, false), "UTF-8");
                BufferedWriter writer = new BufferedWriter(osw, 1048576);
                writer.write(schema);
                writer.flush();
                writer.close();
                if (ExportToFileClient.this.m_cfg.batched()) {
                    m_batchSchemasWritten.add(path);
                }
                else {
                    m_globalSchemasWritten.add(path);
                }
            } catch (Exception e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file: " + path);
                throw new RuntimeException();
            }
        }

        /**
         * Try to ensure file descriptors are closed.
         * This probably only really is useful for the crl-c case.
         * Still, it's not very well tested.
         */
        @Override
        protected void finalize() throws Throwable {
            synchronized (m_batchLock) {
                closeAllWriters();
            }
            super.finalize();
        }
    }



    public ExportToFileClient(final FileClientConfiguration cfg)
    {
        m_cfg = cfg;

        m_tableDecoders = new HashMap<Long, HashMap<String, ExportToFileDecoder>>();

        // SimpleDateFormat isn't threadsafe
        // ThreadLocal variables should protect them, lamely.
        m_dateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(cfg.dateFormatOriginalString());
            }
        };

        // init the batch system with the first batch
        assert(m_current == null);
        m_current = new PeriodicExportContext(new Date());

        // schedule rotations every m_period minutes
        TimerTask rotateTask = new TimerTask() {
            @Override
            public void run() {
                roll(new Date());
            }
        };
        m_timer.scheduleAtFixedRate(rotateTask, 1000 * 60 * cfg.period(), 1000 * 60 * cfg.period());
    }

    PeriodicExportContext getCurrentContextAndAddref(ExportToFileDecoder decoder) {
        synchronized(m_batchLock) {
            m_current.m_decoders.add(decoder);
            return m_current;
        }
    }

    public ExportToFileDecoder constructExportDecoder(AdvertisedDataSource source) {
        // For every source that provides part of a table, use the same
        // export decoder.
        String table_name = source.tableName;
        HashMap<String, ExportToFileDecoder> decoders = m_tableDecoders.get(source.m_generation);
        if (decoders == null) {
            decoders = new HashMap<String, ExportToFileDecoder>();
            m_tableDecoders.put(source.m_generation, decoders);
        }
        ExportToFileDecoder decoder = decoders.get(table_name);
        if (decoder == null) {
            decoder = new ExportToFileDecoder(source, m_cfg);
            decoders.put(table_name, decoder);
        }
        decoder.m_sources.add(source);
        return decoders.get(table_name);
    }

    /**
     * Deprecate the current batch and create a new one. The old one will still
     * be active until all writers have finished writing their current blocks
     * to it.
     */
    void roll(Date rollTime) {
        synchronized(m_batchLock) {
            m_logger.trace("Rolling batch.");
            m_current.end = rollTime;
            if (m_current.m_decoders.size() == 0)
                m_current.closeAllWriters();
            m_current = new PeriodicExportContext(rollTime);
        }
    }

    protected void logConfigurationInfo() {

        StringBuilder sb = new StringBuilder();
        sb.append("Address for ").append(m_commandLineServerArgs.size());
        sb.append(" given as command line arguments:");
        for (String server : m_commandLineServerArgs) {
            sb.append("\n  ").append(server);
        }
        m_logger.info(sb.toString());

        m_logger.info(String.format("Connecting to cluster on %s ports",
                ExportToFileClient.this.m_cfg.useAdminPorts() ? "admin" :  "client"));

        String username = ExportToFileClient.this.m_cfg.user();
        if ((username != null) && (username.length() > 0)) {
            m_logger.info("Connecting as user " + username);
        }
        else {
            m_logger.info("Connecting anonymously");
        }

        m_logger.info(String.format("Writing to disk in %s format",
                (ExportToFileClient.this.m_cfg.delimiter() == ',') ? "CSV" : "TSV"));
        m_logger.info(String.format("Prepending export data files with nonce: %s",
                ExportToFileClient.this.m_cfg.nonce()));
        m_logger.info(String.format("Using date format for file names: %s",
                ExportToFileClient.this.m_cfg.dateFormatOriginalString()));
        m_logger.info(String.format("Rotate export files every %d minute%s",
                ExportToFileClient.this.m_cfg.period(),
                ExportToFileClient.this.m_cfg.period() == 1 ? "" : "s"));
        m_logger.info(String.format("Writing export files to dir: %s",
                ExportToFileClient.this.m_cfg.outDir()));
        if (ExportToFileClient.this.m_cfg.firstField() == 0) {
            m_logger.info("Including VoltDB export metadata");
        }
        else {
            m_logger.info("Not including VoltDB export metadata");
        }
    }


    public static void main(String[] args) {
        FileClientConfiguration config = new FileClientConfiguration(args);
        ExportToFileClient client = new ExportToFileClient(config);

        // add all of the servers specified
        for (String server : config.voltServers()) {
            server = server.trim();
            client.m_commandLineServerArgs.add(server);
            // client.addServerInfo(server, config.connect() == 'a');
        }

        // add credentials (default blanks used if none specified)
        //client.addCredentials(config.user(), config.password());

    }
}
