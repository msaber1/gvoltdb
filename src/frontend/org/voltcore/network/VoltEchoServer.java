/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.network;

import org.voltcore.utils.CoreUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class VoltEchoServer implements Runnable {
    //private final ExecutorService m_executor = CoreUtils.getBoundedThreadPoolExecutor(128, 10L, TimeUnit.SECONDS,
    //        CoreUtils.getThreadFactory("Client acceptor threads", "Client acceptor"));
    private VoltNetworkPool m_networkPool;
    private int m_port;
    private ServerSocketChannel m_serverSocket;
    private Thread m_thread;
    private volatile boolean m_running;

    public static void main(String[] args) {
        VoltEchoServer server = new VoltEchoServer(9876);
        server.start();
        server.waitForShutdown();
    }

    public VoltEchoServer(int port) {
        m_port = port;
    }

    public void start() {
        m_networkPool = new VoltNetworkPool(4, 0, null, "Echo Server");
        m_networkPool.start();
        try {
            m_serverSocket = ServerSocketChannel.open();
            m_serverSocket.socket().bind(new InetSocketAddress(m_port));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        m_running = true;
        m_thread = new Thread(null, this, "Echo Server", 262144);
        m_thread.setDaemon(true);
        m_thread.start();
    }

    public void waitForShutdown() {
        try {
            m_thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void shutdown() throws InterruptedException {
        synchronized (this) {
            m_running = false;
            m_thread.interrupt();
        }
        m_thread.join();
    }

    @Override
    public void run() {
        try {
            do {
                final SocketChannel socket = m_serverSocket.accept();
                final AcceptorRunnable acceptorRunnable = new AcceptorRunnable(socket);
                acceptorRunnable.run();
            } while (m_running);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                m_serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    class AcceptorRunnable implements Runnable {
        private final SocketChannel m_socket;

        public AcceptorRunnable(SocketChannel socket) {
            m_socket = socket;
        }

        @Override
        public void run() {
            try {
                m_socket.configureBlocking(false);
                m_socket.socket().setTcpNoDelay(true);
                m_socket.socket().setKeepAlive(true);

                m_networkPool.registerChannel(m_socket, new EchoInputHandler(), 0, ReverseDNSPolicy.ASYNCHRONOUS);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    m_socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}

class EchoInputHandler implements InputHandler {
    @Override
    public int getMaxRead() {
        return 32768;
    }

    @Override
    public ByteBuffer retrieveNextMessage(NIOReadStream inputStream) throws IOException {
        int dataAvailable = inputStream.dataAvailable();
        if (dataAvailable > 0) {
            ByteBuffer result = ByteBuffer.allocate(dataAvailable);
            inputStream.getBytes(result.array());
            return result;
        }
        return null;
    }

    @Override
    public void handleMessage(ByteBuffer message, Connection c) {
        c.writeStream().enqueue(message);
    }

    @Override
    public void starting(Connection c) {

    }

    @Override
    public void started(Connection c) {
        c.enableReadSelection();
    }

    @Override
    public void stopping(Connection c) {

    }

    @Override
    public void stopped(Connection c) {

    }

    @Override
    public Runnable onBackPressure() {
        return null;
    }

    @Override
    public Runnable offBackPressure() {
        return null;
    }

    @Override
    public QueueMonitor writestreamMonitor() {
        return null;
    }

    @Override
    public long connectionId() {
        return 0;
    }
}
