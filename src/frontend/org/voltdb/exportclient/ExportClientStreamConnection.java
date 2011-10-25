package org.voltdb.exportclient;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.exportclient.ExportClient.CompletionEvent;
import org.voltdb.logging.VoltLogger;


/** Connect to a server data feed */
class ExportClientStreamConnection implements Runnable {
    static final VoltLogger LOG = new VoltLogger("ExportClient");
    private final String m_advertisement;
    private final InetSocketAddress m_server;
    private final CompletionEvent m_onCompletion;
    private final ExportClientProcessor m_processor;

    public ExportClientStreamConnection(
            InetSocketAddress server,
            String nextAdvertisement,
            CompletionEvent onCompletion,
            ExportClientProcessor processor)
    {
        m_advertisement = nextAdvertisement;
        m_server = server;
        m_onCompletion = onCompletion;
        m_processor = processor;
    }

    @Override
    public void run()
    {
        BufferedInputStream reader;
        SocketChannel socket = null;
        long bytesRead = 0;
        long totalBytes = 0;

        LOG.info("Retrieving data for advertisement: " + m_advertisement);

        try {
            Object[] cxndata = ConnectionUtil.getAuthenticatedExportStreamConnection(
                m_advertisement,
                m_server.getHostName(),
                null,
                null,
                m_server.getPort());
            socket = (SocketChannel) cxndata[0];
            socket.configureBlocking(true);
            reader = new BufferedInputStream(socket.socket().getInputStream());
            do {
                ByteBuffer buf = m_processor.emptyBuffer();
                bytesRead = reader.read(buf.array());
                buf.flip();
                totalBytes += buf.limit();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Advertisement " + m_advertisement + " read " + bytesRead);
                }
                m_processor.offer(m_advertisement, buf);
            } while(bytesRead > 0);

            // trigger the ack for this advertisement
            m_onCompletion.setProcessedByteCount(totalBytes);
        }
        catch (IOException e) {
            LOG.error(e);
        }
        finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            }
            catch (IOException e) {
                LOG.error(e);
            }
        }
    }
}
