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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClient.CompletionEvent;
import org.voltdb.logging.VoltLogger;

public class FileClient implements ExportClientProcessorFactory
{
    static final VoltLogger LOG = new VoltLogger("ExportClient");

    // Configuration parsed from the command line.
    final FileClientConfiguration m_cfg;

    FileClient(FileClientConfiguration config) {
        m_cfg = config;
    }

    private static class DecoderWrapper implements ExportClientProcessor
    {
        final ExportToFileDecoder m_decoder;

        DecoderWrapper(AdvertisedDataSource ad, FileClientConfiguration config) {
            // need to coordinate with the file roller here.
            m_decoder = new ExportToFileDecoder(ad, config);
        }

        /** ExportClientProcessor interface to obtain a new buffer */
        @Override
        public ByteBuffer emptyBuffer() {
            return ByteBuffer.allocate(1024 * 1024 * 2);
        }

        /** ExportClientProcessor interface to receive a new buffer */
        @Override
        public void offer(AdvertisedDataSource advertisement, ByteBuffer buf) throws IOException {
            m_decoder.decode(buf);
        }

        /** ExportClientProcessor interface to complete an advertisement */
        @Override
        public void done(CompletionEvent completionEvent) {
            // Complete with the ExportClient
            completionEvent.run();
        }

        /** ExportClientProcess interface to indicate error */
        @Override
        public void error(Exception e) {
            // TODO: need to clean up the file with the ExportToFileDecoder
        }
    }

    /** ECPFactory interface to make an ECP for a new advertisement */
    @Override
    public ExportClientProcessor factory(AdvertisedDataSource advertisement) {
        return new DecoderWrapper(advertisement, m_cfg);
    }

    /** Configure and start exporting */
    public static void main(String[] args) {
        try {
            FileClientConfiguration config = new FileClientConfiguration(args);
            FileClient fileClient = new FileClient(config);
            ExportClient exportClient = new ExportClient(fileClient);

            for (String server : config.voltServers()) {
                exportClient.addServerInfo(server, config.useAdminPorts());
            }

            exportClient.start();
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            LOG.fatal("Unexpected exception running FileClient", e);
        }
    }


}
