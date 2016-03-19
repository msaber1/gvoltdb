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

package org.voltcore.utils;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


/**
 * An {@link OutputStream} implementation that writes to an underlying
 * {@link ByteBuffer}.
 *
 * It is CATEGORICALLY NOT THREAD SAFE
 */
public class CappedByteBufferOutputStream extends OutputStream {
    protected static final int DEFAULT_ALLOCATION_SIZE = 256 * 1024;

    protected final ByteBuffer m_bb;

    public CappedByteBufferOutputStream(ByteBuffer bb) {
        super();
        m_bb = checkNotNull(bb, "backing byte buffer is null");
    }

    public CappedByteBufferOutputStream() {
        this(ByteBuffer.allocateDirect(DEFAULT_ALLOCATION_SIZE));
    }

    @Override
    public void write(byte[] b) throws IOException {
        ensureCapacity(b.length);
        m_bb.put(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureCapacity(len);
        m_bb.put(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        ensureCapacity(1);
        m_bb.put((byte) (b & 0xff));
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    /**
     * Returns a copy of the backing {@link ByteBuffer}. Its contents
     * may be modified if any other write or resets occur while the
     * returned {@link ByteBuffer} is processed.
     *
     * @return a read only view of the backing {@link ByteBuffer}
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer bb = m_bb.asReadOnlyBuffer();
        bb.flip();
        return bb;
    }

    /**
     * Clears the backing {@link ByteBuffer}
     */
    public void reset() {
        m_bb.clear();
    }

    /**
     * @return a byte array copy of the backing {@link ByteBuffer} content
     */
    public byte [] toByteArray() {
        ByteBuffer bb = m_bb.asReadOnlyBuffer();
        byte [] byteArray = new byte [bb.flip().limit()];
        bb.get(byteArray);
        return byteArray;
    }

    /**
     * Expand the underlying byte buffer to contain the specified delta
     * @param delta amount needed to accommodate the next buffer append
     */
    protected void ensureCapacity(int delta) throws IOException {
        if (m_bb.position() + delta > m_bb.capacity()) {
            throw new IOException("not enough space left in the backing byte buffer");
        }
    }
}
