/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb;

import java.io.IOException;
import java.util.concurrent.Future;

import org.voltdb.utils.CompressionService;

public class TableCompressor {
    public static byte[] getCompressedBytes(final VoltTable table) throws IOException {
        final int startPosition = table.m_buffer.position();
        try {
            table.m_buffer.position(0);
            if (table.m_buffer.isDirect()) {
                return CompressionService.compressBuffer(table.m_buffer);
            } else {
                assert(table.m_buffer.hasArray());
                return CompressionService.compressBytes(
                        table.m_buffer.array(),
                        table.m_buffer.arrayOffset() + table.m_buffer.position(),
                        table.m_buffer.limit());
            }
        } finally {
            table.m_buffer.position(startPosition);
        }
    }

    public static Future<byte[]> getCompressedBytesAsync(final VoltTable table) throws IOException {
        final int startPosition = table.m_buffer.position();
        try {
            table.m_buffer.position(0);
            if (table.m_buffer.isDirect()) {
                return CompressionService.compressBufferAsync(table.m_buffer.duplicate());
            } else {
                assert(table.m_buffer.hasArray());
                return CompressionService.compressBytesAsync(
                        table.m_buffer.array(),
                        table.m_buffer.arrayOffset() + table.m_buffer.position(),
                        table.m_buffer.limit());
            }
        } finally {
            table.m_buffer.position(startPosition);
        }
    }
}
