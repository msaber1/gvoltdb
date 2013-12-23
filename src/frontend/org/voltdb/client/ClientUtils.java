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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper methods duplicated from MiscUtils or CoreUtils to avoid linking with
 * some of the stuff they bring in.
 *
 */
public class ClientUtils {

    public static final int SMALL_STACK_SIZE = 1024 * 256;
    public static final int MEDIUM_STACK_SIZE = 1024 * 512;

    /**
     * I heart commutativity
     * @param buffer ByteBuffer assumed position is at end of data
     * @return the cheesy checksum of this VoltTable
     */
    public static final long cheesyBufferCheckSum(ByteBuffer buffer) {
        final int mypos = buffer.position();
        buffer.position(0);
        long checksum = 0;
        if (buffer.hasArray()) {
            final byte bytes[] = buffer.array();
            final int end = buffer.arrayOffset() + mypos;
            for (int ii = buffer.arrayOffset(); ii < end; ii++) {
                checksum += bytes[ii];
            }
        } else {
            for (int ii = 0; ii < mypos; ii++) {
                checksum += buffer.get();
            }
        }
        buffer.position(mypos);
        return checksum;
    }

    /**
     * Serialize a file into bytes. Used to serialize catalog and deployment
     * file for UpdateApplicationCatalog on the client.
     *
     * @param path
     * @return a byte array of the file
     * @throws IOException
     *             If there are errors reading the file
     */
    public static byte[] fileToBytes(File path) throws IOException {
        FileInputStream fin = new FileInputStream(path);
        byte[] buffer = new byte[(int) fin.getChannel().size()];
        try {
            if (fin.read(buffer) == -1) {
                throw new IOException("File " + path.getAbsolutePath() + " is empty");
            }
        } finally {
            fin.close();
        }
        return buffer;
    }

    public static ThreadFactory getThreadFactory(String name) {
        return getThreadFactory(name, SMALL_STACK_SIZE);
    }

    public static ThreadFactory getThreadFactory(String groupName, String name) {
        return getThreadFactory(groupName, name, SMALL_STACK_SIZE, true, null);
    }

    public static ThreadFactory getThreadFactory(String name, int stackSize) {
        return getThreadFactory(null, name, stackSize, true, null);
    }

    /**
     * Creates a thread factory that creates threads within a thread group if
     * the group name is given. The threads created will catch any unhandled
     * exceptions and log them to the HOST logger.
     *
     * @param groupName
     * @param name
     * @param stackSize
     * @return
     */
    public static ThreadFactory getThreadFactory(
            final String groupName,
            final String name,
            final int stackSize,
            final boolean incrementThreadNames,
            final Queue<String> coreList) {
        ThreadGroup group = null;
        if (groupName != null) {
            group = new ThreadGroup(Thread.currentThread().getThreadGroup(), groupName);
        }
        final ThreadGroup finalGroup = group;

        return new ThreadFactory() {
            private final AtomicLong m_createdThreadCount = new AtomicLong(0);
            private final ThreadGroup m_group = finalGroup;
            @Override
            public synchronized Thread newThread(final Runnable r) {
                final String threadName = name +
                        (incrementThreadNames ? " - " + m_createdThreadCount.getAndIncrement() : "");
                String coreTemp = null;
                if (coreList != null && !coreList.isEmpty()) {
                    coreTemp = coreList.poll();
                }
                final String core = coreTemp;
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        if (core != null) {
                            // Remove Affinity for now to make this dependency dissapear from the client.
                            // Goal is to remove client dependency on this class in the medium term.
                            //PosixJNAAffinity.INSTANCE.setAffinity(core);
                        }
                        try {
                            r.run();
                        } catch (Throwable t) {
                            //hostLog.error("Exception thrown in thread " + threadName, t);
                            // TODO
                            System.exit(-1);
                        }
                    }
                };

                Thread t = new Thread(m_group, runnable, threadName, stackSize);
                t.setDaemon(true);
                return t;
            }
        };
    }
}
