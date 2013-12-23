/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.client;

import io.netty_voltpatches.channel.socket.SocketChannel;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A utility class for opening a connection to a Volt server and authenticating as well
 * as sending invocations and receiving responses. It is safe to queue multiple requests
 *
 */
public class ConnectionUtil {

    private static final AtomicLong m_handle = new AtomicLong(Long.MIN_VALUE);

    public static Future<Long> sendInvocation(final ExecutorService executor, final SocketChannel channel, final String procName,final Object ...parameters) {
        return executor.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                final long handle = m_handle.getAndIncrement();
                final ProcedureInvocation invocation =
                    new ProcedureInvocation(handle, procName, parameters);

                ByteBuffer buf = ByteBuffer.allocate(4 + invocation.getSerializedSize());
                buf.position(4);
                invocation.flattenToBuffer(buf);
                buf.putInt(0, buf.capacity() - 4);
                buf.flip();
                do {
                    channel.write(buf);
                    if (buf.hasRemaining()) {
                        Thread.yield();
                    }
                }
                while(buf.hasRemaining());
                return handle;
            }
        });
    }
}
