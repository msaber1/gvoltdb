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
package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;

/**
 * The Export data source metadata returned in a kOpenResponse message.
 */
public class AdvertisedDataSource implements Comparable<AdvertisedDataSource> {
    final public int partitionId;
    final public String signature;
    final public String tableName;
    final public long m_generation;
    final public long systemStartTimestamp;
    final public ArrayList<String> columnNames = new ArrayList<String>();
    final public ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();

    @Override
    public int hashCode() {
        return (((int)m_generation) + ((int)(m_generation >> 32))) + partitionId + signature.hashCode();
    }

    /** Advertised data sources with equal generations, signatures
        and partition IDs are equal. */
    @Override
    public boolean equals(Object o) {
        if (o instanceof AdvertisedDataSource) {
            AdvertisedDataSource other = (AdvertisedDataSource)o;
            if (other.m_generation == m_generation &&
                    other.signature.equals(signature) &&
                    other.partitionId == partitionId) {
                return true;
            }
        }
        return false;
    }

    /** returns a negative integer, zero, or a positive integer as this object is less than,
     * equal to, or greater than the specified object. */
    @Override
    public int compareTo(AdvertisedDataSource that) {
        if (this.m_generation < that.m_generation) {
            return -1;
        } else if (this.m_generation > that.m_generation) {
            return 1;
        }

        int stringCompare = this.signature.compareTo(that.signature);
        if (stringCompare != 0) {
            return stringCompare;
        }

        return this.partitionId - that.partitionId;
    }


    public AdvertisedDataSource(int p_id, String t_signature, String t_name,
                                long systemStartTimestamp,
                                long generation,
                                ArrayList<String> names,
                                ArrayList<VoltType> types)
    {
        partitionId = p_id;
        signature = t_signature;
        tableName = t_name;
        m_generation = generation;
        this.systemStartTimestamp = systemStartTimestamp;

        // null checks are for happy-making test time
        if (names != null)
            columnNames.addAll(names);
        if (types != null)
            columnTypes.addAll(types);
    }

    /** Deserialize the format produced by ExportDataSource.writeAdvertisementTo()
     * @throws IOException */
    public static AdvertisedDataSource deserialize(ByteBuffer buf) throws IOException {
        FastDeserializer fds = new FastDeserializer(buf);
        long generationId = fds.readLong();
        int partitionId = fds.readInt();
        String signature = fds.readString();
        String tableName = fds.readString();
        long startTime = fds.readLong();
        int columncnt = fds.readInt();
        ArrayList<String> columnNames = new ArrayList<String>(columncnt);
        ArrayList<VoltType> columnTypes = new ArrayList<VoltType>(columncnt);
        for (int i=0; i < columncnt; i++) {
            columnNames.add(fds.readString());
            columnTypes.add(VoltType.get((byte) fds.readInt()));
        }
        return new AdvertisedDataSource(
                partitionId, signature, tableName,
                startTime, generationId, columnNames, columnTypes);
    }

    public VoltType columnType(int index) {
        return columnTypes.get(index);
    }

    public String columnName(int index) {
        return columnNames.get(index);
    }

    @Override
    public String toString() {
        return "Generation: " + m_generation + " Table: " + tableName + " partition " + partitionId + " signature " + signature;
    }

}
