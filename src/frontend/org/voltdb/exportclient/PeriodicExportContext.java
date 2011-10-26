package org.voltdb.exportclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.voltdb.logging.VoltLogger;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/**
 * Manages a directory and set of files being written by an
 * export client. This class has multiple threads changing it
 * concurrently - to start file roll over, to get a new
 * writer, to close a writer.
 *
 * It is made thread safe through synchronization of its
 * public API and strictly private internal state.
 */
public class PeriodicExportContext {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    private final FileClientConfiguration m_cfg;
    private final Set<String> m_globalSchemasWritten = new HashSet<String>();
    private final File m_dirContainingFiles;
    private final Map<FileHandle, CSVWriter> m_writers = new TreeMap<FileHandle, CSVWriter>();
    private boolean m_hasClosed = false;
    private final Date start;
    private Date end = null;
    private final HashSet<ExportToFileDecoder> m_decoders = new HashSet<ExportToFileDecoder>();
    private final Set<String> m_batchSchemasWritten = new HashSet<String>();

    // SimpleDataFormat is not threadsafe
    private final ThreadLocal<SimpleDateFormat> m_dateformat;

    // Folders/files being written are named with this prefix
    private static final String ACTIVE_PREFIX = "active-";

    public PeriodicExportContext(Date batchStart, FileClientConfiguration config) {
        m_cfg = config;
        start = batchStart;

        m_dateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(m_cfg.dateFormatOriginalString());
            }
        };

        if (m_cfg.batched()) {
            m_dirContainingFiles = new File(getPathOfBatchDir(ACTIVE_PREFIX));
            m_logger.trace(String.format("Creating dir for batch at %s", m_dirContainingFiles.getPath()));
            m_dirContainingFiles.mkdirs();
            if (m_dirContainingFiles.exists() == false) {
                m_logger.error("Error: Unable to create batch directory at path: " + m_dirContainingFiles.getPath());
                throw new RuntimeException("Unable to create batch directory.");
            }
        }
        else {
            m_dirContainingFiles = m_cfg.outDir();
        }
    }


    /**
     * Release a hold on the batch. When all holds are done
     * and the roll time has passed, the batch can move out
     * of the active state.
     */
    synchronized public void decref(ExportToFileDecoder decoder) {
        m_decoders.remove(decoder);
        if ((end != null) && (m_decoders.size() == 0)) {
            closeAllWriters();
        }
    }

    /**
     * The roll timer expired
     */
    synchronized public void roll(Date rollTime) {
        end = rollTime;
        if (m_decoders.size() == 0)
            closeAllWriters();
    }

    /**
     * Add a new decoder / writer to the context.
     */
    synchronized public void addDecoder(ExportToFileDecoder decoder) {
        m_decoders.add(decoder);
    }


    synchronized public CSVWriter getWriter(String tableName, long generation) {
        char[] fullDelimiters = m_cfg.fullDelimiters();
        char delimiter = m_cfg.delimiter();

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

    synchronized public void writeSchema(String tableName, long generation, String schema) {
        // if no schema's enabled pretend like this worked
        if (m_cfg.withSchema()) {
            return;
        }

        FileHandle handle = new FileHandle(tableName, generation);
        String path = handle.getPathForSchema();

        // only write the schema once per batch
        if (m_cfg.batched()) {
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
            if (m_cfg.batched()) {
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


    //
    //  Internals
    //


    private class FileHandle implements Comparable<FileHandle> {
        final String tableName;
        final long generation;

        FileHandle(String tableName, long generation) {
            this.tableName = tableName;
            this.generation = generation;
        }

        String getPath(String prefix) {
            if (m_cfg.batched()) {
                return m_dirContainingFiles.getPath() +
                        File.separator +
                        generation +
                        "-" +
                        tableName +
                        m_cfg.extension();
            }
            else {
                return m_dirContainingFiles.getPath() +
                        File.separator +
                        prefix +
                        m_cfg.nonce() +
                        "-" +
                        generation +
                        "-" +
                        tableName +
                        "-" +
                        m_dateformat.get().format(start) +
                        m_cfg.extension();
            }
        }

        String getPathForSchema() {
            if (m_cfg.batched()) {
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
                        m_cfg.nonce() +
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

    private String getPathOfBatchDir(String prefix) {
        assert(m_cfg.batched());
        return m_cfg.outDir().getPath() + File.separator + prefix +
               m_cfg.nonce() + "-" + m_dateformat.get().format(start);
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
    private void closeAllWriters() {
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

        if (m_cfg.batched())
            closeBatch();
        else
            closeFiles();

        // empty the writer set (probably not needed)
        m_writers.clear();

        // note that we're closed now
        m_hasClosed = true;
    }

    private void closeBatch() {
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

    private void closeFiles() {
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


    /**
     * Try to ensure file descriptors are closed.
     * This probably only really is useful for the crl-c case.
     * Still, it's not very well tested.
     */
    @Override
    protected void finalize() throws Throwable {
        closeAllWriters();
        super.finalize();
    }

}
