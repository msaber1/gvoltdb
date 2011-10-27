package org.voltdb.exportclient;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.exportclient.ExportClient.CompletionEvent;

public class FileClient implements ExportClientProcessorFactory
{
    // Configuration parsed from the command line.
    final FileClientConfiguration m_cfg;

    FileClient(FileClientConfiguration config) {
        m_cfg = config;
    }

    private static class DecoderWrapper implements ExportClientProcessor
    {
        final ExportToFileDecoder m_decoder;

        DecoderWrapper(FileClientConfiguration config) {
            // need to coordinate with the file roller here.
            m_decoder = new ExportToFileDecoder(null, config);
        }

        /** ExportClientProcessor interface to obtain a new buffer */
        @Override
        public ByteBuffer emptyBuffer() {
            return ByteBuffer.allocate(1024 * 1024 * 2);
        }

        /** ExportClientProcessor interface to receive a new buffer */
        @Override
        public void offer(String advertisement, ByteBuffer buf) throws IOException {
            m_decoder.decode(buf);
        }

        /** ExportClientProcessor interface to complete an advertisement */
        @Override
        public void done(CompletionEvent completionEvent) {
            // Complete with the ExportClient
            completionEvent.run();
        }
    }

    /** ECPFactory interface to make an ECP for a new advertisement */
    @Override
    public ExportClientProcessor factory(String advertisement) {
        return new DecoderWrapper(m_cfg);
    }

    /** Configure and start exporting */
    public static void main(String[] args) {
        FileClientConfiguration config = new FileClientConfiguration(args);
        FileClient fileClient = new FileClient(config);
        ExportClient exportClient = new ExportClient(fileClient);

       for (String server : config.voltServers()) {
           exportClient.addServerInfo(server, config.useAdminPorts());
       }

       exportClient.start();
    }


}
