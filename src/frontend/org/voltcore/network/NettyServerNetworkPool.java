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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class NettyServerNetworkPool {
    private String m_poolName;
    private int m_numThreads;
    private int m_port;
    private ChannelInitializer<SocketChannel> m_childInitializer;

    private EventLoopGroup m_parentEventLoopGroup;
    private EventLoopGroup m_childEventLoopGroup;

    public NettyServerNetworkPool(String poolName, int numThreads, int port,
                                  ChannelInitializer<SocketChannel> childInitializer) {
        checkArgument(numThreads > 0, "number of threads should be positive");
        checkNotNull(childInitializer, "child initializer cannot be null");
        m_poolName = poolName;
        m_numThreads = numThreads;
        m_port = port;
        m_childInitializer = childInitializer;
    }

    public ChannelFuture start() throws Exception {
        boolean isOnLinux = System.getProperty("os.name").toLowerCase().contains("linux");
        if (isOnLinux) {
            m_parentEventLoopGroup = new EpollEventLoopGroup(1);
            m_childEventLoopGroup = new EpollEventLoopGroup(m_numThreads);
        }
        else {
            m_parentEventLoopGroup = new NioEventLoopGroup(1);
            m_childEventLoopGroup = new NioEventLoopGroup(m_numThreads);
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(m_parentEventLoopGroup, m_childEventLoopGroup)
                .channel(isOnLinux ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(m_port))
                .childHandler(m_childInitializer);

        return b.bind().sync();
    }

    public void shutdown() throws Exception {
        m_parentEventLoopGroup.shutdownGracefully().sync();
        m_childEventLoopGroup.shutdownGracefully().sync();
    }
}
