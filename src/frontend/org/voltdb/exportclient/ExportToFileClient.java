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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;

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

    protected final HashMap<Long, HashMap<String, ExportToFileDecoder>> m_tableDecoders;

    // configuration parsed from command line parameters
    protected final FileClientConfiguration m_cfg;

    // timer used to roll batches
    protected final Timer m_timer = new Timer();

    // record the original command line args for servers
    protected final List<String> m_commandLineServerArgs = new ArrayList<String>();

    protected AtomicReference<PeriodicExportContext> m_current =
            new AtomicReference<PeriodicExportContext>();

    public ExportToFileClient(final FileClientConfiguration cfg)
    {
        m_cfg = cfg;
        m_tableDecoders = new HashMap<Long, HashMap<String, ExportToFileDecoder>>();
        m_current.set(new PeriodicExportContext(new Date(), m_cfg));

        // schedule rotations every m_period minutes
        TimerTask rotateTask = new TimerTask() {
            @Override
            public void run() {
                roll(new Date());
            }
        };

        m_timer.scheduleAtFixedRate(rotateTask,
                1000 * 60 * cfg.period(),
                1000 * 60 * cfg.period());
    }

    PeriodicExportContext getCurrentContextAndAddref(ExportToFileDecoder decoder) {
        PeriodicExportContext ctx = m_current.get();
        ctx.addDecoder(decoder);
        return ctx;
    }

    public ExportToFileDecoder constructExportDecoder(AdvertisedDataSource source) {
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
        m_logger.trace("Rolling batch.");

        // A set and cannot interleave between the get and set of ctx.getAndSet().
        // As long as all callers do getAndSet and close the returned context,
        // all contexts will be closed.
        PeriodicExportContext oldctx =
                m_current.getAndSet(new PeriodicExportContext(rollTime, m_cfg));
        oldctx.roll(rollTime);
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
