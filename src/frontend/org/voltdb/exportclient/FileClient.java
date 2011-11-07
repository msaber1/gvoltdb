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
import java.util.Date;

import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClient.CompletionEvent;
import org.voltdb.logging.VoltLogger;

public class FileClient implements ExportClientProcessorFactory
{
    static final VoltLogger LOG = new VoltLogger("ExportClient");

    // Configuration parsed from the command line.
    final FileClientConfiguration m_cfg;

    // Use a PeriodicExportContext to manage filenames and CSV configuration
    private final PeriodicExportContext m_currentContext;

    FileClient(FileClientConfiguration config) {
        m_cfg = config;
        m_currentContext = new PeriodicExportContext(new Date(), m_cfg);
    }

    // DecoderWrapper maps the processer interface to ExportToFileDecoder.
    private static class DecoderWrapper implements ExportClientProcessor
    {
        private final ExportToFileDecoder m_decoder;
        private final AdvertisedDataSource m_ad;

        DecoderWrapper(ExportToFileDecoder decoder, AdvertisedDataSource ad) {
            m_decoder = decoder;
            m_ad = ad;
        }

        /** ECP interface to obtain a new buffer */
        @Override
        public ByteBuffer emptyBuffer() {
            return ByteBuffer.allocate(1024 * 1024 * 2);
        }

        /** ECP interface to receive a new buffer */
        @Override
        public void offer(ByteBuffer buf) throws IOException {
            m_decoder.decode(buf);
        }

        /** ECP interface to complete an advertisement */
        @Override
        public void done(CompletionEvent completionEvent) {
            // Complete with the Decoder
            LOG.info("Completing advertisement " + m_ad);
            m_decoder.onBlockCompletion();

            // Complete with the ExportClient
            completionEvent.run();
        }

        /** ECP interface to indicate error */
        @Override
        public void error(Exception e) {
            // TODO: need to clean up the file with the ExportToFileDecoder
        }
    }

    /**
     * ECPFactory interface to make an ECP for a new advertisement
     * The factory is the only public interface to the FileClient and
     * can be invoked concurrently by ExportClient. Synchronization here
     * keeps changes to m_currentContext thread safe.
     *
     * Note that the periodic context is handed off to multiple decoders.
     * The PeriodicExportContext is responsible for its own safety.
     *
     * Once synchronized here, the DecoderWrapper is never reentrantly
     * invoked by ExportClient.
     */
    @Override
    synchronized public ExportClientProcessor factory(AdvertisedDataSource advertisement) {
        ExportToFileDecoder decoder =
                new ExportToFileDecoder(m_currentContext, advertisement, m_cfg);
        return new DecoderWrapper(decoder, advertisement);
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
