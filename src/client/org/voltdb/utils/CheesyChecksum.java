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

package org.voltdb.utils;

import java.nio.ByteBuffer;

public class CheesyChecksum {
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
}
