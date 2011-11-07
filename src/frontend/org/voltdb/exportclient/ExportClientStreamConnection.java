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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClient.CompletionEvent;
import org.voltdb.logging.VoltLogger;


/** Connect to a server data feed */
class ExportClientStreamConnection implements Runnable {
    static final VoltLogger LOG = new VoltLogger("ExportClient");
    private final AdvertisedDataSource m_advertisement;
    private final InetSocketAddress m_server;
    private final String m_username;
    private final byte[] m_hashedPassword;
    private final CompletionEvent m_onCompletion;
    private final ExportClientProcessor m_processor;

    public ExportClientStreamConnection(
            InetSocketAddress server,
            String username,
            byte[] hashedPassword,
            AdvertisedDataSource nextAdvertisement,
            CompletionEvent onCompletion,
            ExportClientProcessor processor)
    {
        m_advertisement = nextAdvertisement;
        m_server = server;
        m_username = username;
        m_hashedPassword = hashedPassword;
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

        String compositeIdentity =
                m_advertisement.m_generation + "-" +
                m_advertisement.partitionId + "-" +
                m_advertisement.signature;

        try {
            Object[] cxndata = ConnectionUtil.getAuthenticatedExportStreamConnection(
                    compositeIdentity,
                    m_server.getHostName(),
                    m_username,
                    m_hashedPassword,
                    m_server.getPort());
            socket = (SocketChannel) cxndata[0];
            socket.configureBlocking(true);
            reader = new BufferedInputStream(socket.socket().getInputStream());
            do {
                ByteBuffer buf = m_processor.emptyBuffer();
                bytesRead = reader.read(buf.array());
                buf.position((int) bytesRead);
                buf.flip();
                totalBytes += buf.limit();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Advertisement " + m_advertisement + " read " + bytesRead);
                }
                m_processor.offer(m_advertisement, buf);
            } while(bytesRead > 0);

            // trigger the ack for this advertisement
            m_onCompletion.setProcessedByteCount(totalBytes);
            m_processor.done(m_onCompletion);
        }
        catch (Exception e) {
            LOG.error("Unexpected error terminating stream " + m_advertisement, e);
            m_processor.error(e);
        }
        finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            }
            catch (IOException ignored) {
            }
        }
    }
}
