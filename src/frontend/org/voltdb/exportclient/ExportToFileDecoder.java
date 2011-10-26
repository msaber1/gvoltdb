package org.voltdb.exportclient;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;

import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportToFileClient.PeriodicExportContext;
import org.voltdb.logging.VoltLogger;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

// This class outputs exported rows converted to CSV or TSV values
// for the table named in the constructor's AdvertisedDataSource
class ExportToFileDecoder extends ExportDecoderBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    protected String m_schemaString = "ERROR SERIALIZING SCHEMA";
    private final FileClientConfiguration m_cfg;
    final HashSet<AdvertisedDataSource> m_sources = new HashSet<AdvertisedDataSource>();

    // use thread-local to avoid SimpleDateFormat thread-safety issues
    protected final ThreadLocal<SimpleDateFormat> m_ODBCDateformat;

    // ODBC time-stamp format: millisecond granularity
    protected static final String ODBC_DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";


    // transient per-block state
    protected PeriodicExportContext m_context = null;
    protected CSVWriter m_writer = null;

    public ExportToFileDecoder(
            AdvertisedDataSource source,
            FileClientConfiguration config)
    {
        super(source);
        m_cfg = config;
        setSchemaForSource(source);

        m_ODBCDateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(ODBC_DATE_FORMAT_STRING);
            }
        };
    }

    /**
     * Given the data source, construct a JSON serialization
     * of its schema to be written to disk with the export
     * data.
     */
    void setSchemaForSource(AdvertisedDataSource source) {
        try {
            JSONStringer json = new JSONStringer();
            json.object();
            json.key("table name").value(source.tableName);
            json.key("generation id").value(source.m_generation);
            json.key("columns").array();

            for (int i = 0; i < source.columnNames.size(); i++) {
                json.object();
                json.key("name").value(source.columnNames.get(i));
                json.key("type").value(source.columnTypes.get(i).name());
                json.endObject();
            }

            json.endArray();
            json.endObject();

            // get the json string
            m_schemaString = json.toString();
            // the next two lines pretty print the json string
            JSONObject jsonObj = new JSONObject(m_schemaString);
            m_schemaString = jsonObj.toString(4);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean processRow(int rowSize, byte[] rowData) {
        int firstfield = m_cfg.firstField();

        // Grab the data row
        Object[] row = null;
        try {
            row = decodeRow(rowData);
        } catch (IOException e) {
            m_logger.error("Unable to decode row for table: " + m_source.tableName);
            return false;
        }

        try {
            String[] fields = new String[m_tableSchema.size() - firstfield];

            for (int i = firstfield; i < m_tableSchema.size(); i++) {
                if (row[i] == null) {
                    fields[i - firstfield] = "NULL";
                } else if (m_tableSchema.get(i) == VoltType.VARBINARY) {
                    fields[i - firstfield] = Encoder.hexEncode((byte[]) row[i]);
                } else if (m_tableSchema.get(i) == VoltType.STRING) {
                    fields[i - firstfield] = (String) row[i];
                } else if (m_tableSchema.get(i) == VoltType.TIMESTAMP) {
                    TimestampType timestamp = (TimestampType) row[i];
                    fields[i - firstfield] = m_ODBCDateformat.get().format(timestamp.asApproximateJavaDate());
                } else {
                    fields[i - firstfield] = row[i].toString();
                }
            }
            m_writer.writeNext(fields);
        }
        catch (Exception x) {
            x.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Release the current batch folder.
     */
    @Override
    public void onBlockCompletion() {
        try {
            if (m_writer != null)
                m_writer.flush();
        }
        catch (Exception e) {
            m_logger.error(e.getMessage());
            throw new RuntimeException();
        }
        finally {
            if (m_context != null) {
                m_context.decref(this);
            }
            m_context = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (m_writer != null) {
                m_writer.flush();
                m_writer.close();
                m_writer = null;
            }
        }
        catch (Exception e) {
            m_logger.error(e.getMessage());
            throw new RuntimeException();
        }
        finally {
            if (m_context != null)
                m_context.decref(this);
        }
        super.finalize();
    }

}

