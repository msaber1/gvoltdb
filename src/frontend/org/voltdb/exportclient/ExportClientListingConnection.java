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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.logging.VoltLogger;

/** Connect to a server and publish a list of available data channels */
public class ExportClientListingConnection implements Runnable {
    static final VoltLogger LOG = new VoltLogger("ExportClient");
    final InetSocketAddress m_server;
    final String m_username;
    final byte[] m_hashedPassword;
    final Set<Object[]> m_results;
    final AdvertisedDataSource m_ackedAdvertisement;
    final long m_ackedBytes;

    // Allow users to query simple pass/fail.
    public AtomicBoolean m_failed = new AtomicBoolean(false);

    /** Create a connection to read the advertisement listing */
    public ExportClientListingConnection(InetSocketAddress server,
        String username,
        byte[] hashedPassword,
        Set<Object[]> results)
    {
        this(server, username, hashedPassword, results, null, 0L);
    }

    /** Create a connection to ack an advertisement and read the current listing */
    public ExportClientListingConnection(InetSocketAddress server,
            String username,
            byte[] hashedPassword,
            Set<Object[]> results,
            AdvertisedDataSource ackedAdvertisement,
        long ackedByteCount)
    {
        m_server = server;
        m_username = username;
        m_hashedPassword = hashedPassword.clone();
        m_results = results;
        m_ackedBytes = ackedByteCount;
        m_ackedAdvertisement = ackedAdvertisement;
    }

    // helper for ack de/serialization
    private void ack(SocketChannel socket) throws IOException
    {
        if (m_ackedAdvertisement == null) {
            return;
        }
        ExportProtoMessage m =
                new ExportProtoMessage(0, 0, m_ackedAdvertisement.signature);
        m.ack(m_ackedBytes);
        socket.write(m.toBuffer());
    }

    // helper for listing de/serialization
    private void poll(SocketChannel socket) throws IOException
    {
        LinkedList<AdvertisedDataSource> advertisements =
                new LinkedList<AdvertisedDataSource>();

        // poll for advertisements
        ExportProtoMessage m = new ExportProtoMessage(0,0,null);
        m.poll();
        socket.write(m.toBuffer());

        // read the advertisement count
        ByteBuffer adCount = ByteBuffer.allocate(4);
        int read = socket.read(adCount);
        if (read < 0) {
            LOG.error("Failed to read advertisement count from: " + m_server);
            return;
        }
        if (read != 4) {
            LOG.error("Invalid read reading advertisements from: " + m_server);
            return;
        }
        adCount.flip();
        int count = adCount.getInt();
        LOG.info("Found " + count + " advertisements from " + m_server);

        for (int i=0; i < count; i++) {
            ByteBuffer msgSize = ByteBuffer.allocate(4);
            read = socket.read(msgSize);
            if (read < 0) {
                LOG.error("Failed to read an advertisement count from: " + m_server);
                return;
            }
            if (read != 4) {
                LOG.error("Failed to read an advertisement count from: " + m_server);
                return;
            }
            msgSize.flip();
            ByteBuffer msgBytes = ByteBuffer.allocate(msgSize.getInt());
            read = socket.read(msgBytes);
            if (read < 0) {
                LOG.error("Failed to read an advertisement count from: " + m_server);
                return;
            }
            if (read != msgBytes.capacity()) {
                LOG.error("Failed to read an advertisement count from: " + m_server);
                return;
            }
            msgBytes.flip();
            AdvertisedDataSource ad = AdvertisedDataSource.deserialize(msgBytes);
            advertisements.add(ad);
        }
        for (AdvertisedDataSource a : advertisements) {
            m_results.add(new Object[] {m_server, a});
        }
    }


    @Override
    public void run() {
        LOG.info("Retrieving advertisments from " + m_server);
        SocketChannel socket = null;
        try {
            Object[] cxndata = ConnectionUtil.getAuthenticatedExportListingConnection(
                m_server.getHostName(),
                m_username,
                m_hashedPassword,
                m_server.getPort());
            socket = (SocketChannel) cxndata[0];
            if (socket == null) {
                LOG.error("Failed to create an export listing connection.");
                return;
            }
            socket.configureBlocking(true);
            ack(socket);
            poll(socket);
        } catch(Exception e) {
            LOG.error("Unexpected exception terminating export listing request", e);
            m_failed.set(true);
        } finally {
            try {
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                LOG.error(e);
            }
        }
    }
}