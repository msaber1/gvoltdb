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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;

public class RequestDataMessage extends TransactionInfoBaseMessage {

    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    private long m_sourceSiteId;
    private long m_destinationSiteId;
    private String m_tableName;
    private String m_graphViewName;

    // Empty constructor for de-serialization
    RequestDataMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataMessage(long sourceSiteId,
                              long destinationSiteId,
                              String tableName,
                              String graphViewName,
                              long txnId,
                              long uniqueId,
                              boolean isReadOnly,
                              boolean isForReplay) {
        super(sourceSiteId, destinationSiteId, txnId, uniqueId, isReadOnly, isForReplay);
        m_sourceSiteId = sourceSiteId;
        m_destinationSiteId = destinationSiteId;
        m_tableName = tableName;
        m_graphViewName = graphViewName;
        m_subject = Subject.DEFAULT.getId();
    }

    // getters and setters
    // request destination is the destination of the message.
    // a set of destinations (for chained messaging) is accessed as a stack.
/*
    public void pushRequestDestination(long destHSId) {
        m_requestDestinations.add(destHSId);
    }

    public long popRequestDestination() {
        return m_requestDestinations.remove(m_requestDestinations.size()-1);
    }

    public long getRequestDestination() {
        return m_requestDestinations.get(m_requestDestinations.size()-1);
    }

    public void setRequestDestinations(ArrayList<Long> dest) {
        m_requestDestinations = dest;
    }

    public ArrayList<Long> getRequestDestinations() {
        return m_requestDestinations;
    }
*/
    public String getTableName() {
        return m_tableName;
    }

    public String getGraphViewName() {
        return m_graphViewName;
    }

    public long getSourceSiteId() {
        return m_sourceSiteId;
    }

    public void setSourceSiteId(long srcSiteId) {
        m_sourceSiteId = srcSiteId;
    }

    public long getDestinationSiteId() {
        return m_destinationSiteId;
    }

    public void setDestinationSiteId(long destSiteId) {
        m_destinationSiteId = destSiteId;
    }

    //  foreign host = 24
    //  message id = 1
    //  transaction = 58
    //  additional = 4 (table name length)
    //             + ? (table name)
    //             + 4 (graph view name length)
    //             + ? (graph view name)
    @Override
    public int getSerializedSize()
    {
        int additional = 4 + m_tableName.length() + 4 + m_graphViewName.length();
        return super.getSerializedSize() + additional;
    }

    //  capacity = 100
    //  limit = 100
    //  position = 24
    //  remaining = 76
    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        System.out.println("REQUEST flatten");

        //  message id
        buf.put(VoltDbMessageFactory.REQUEST_DATA_ID);

        //  transaction info
        super.flattenToBuffer(buf);

        //  table name length
        buf.putInt(m_tableName.length());

        //  table name
        buf.put(m_tableName.getBytes());

        //  graph view name length
        buf.putInt(m_graphViewName.length());

        //  graph view name
        buf.put(m_graphViewName.getBytes());

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    //  capacity = 76
    //  limit = 76
    //  position = 0
    //  remaining = 76
    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        System.out.println("REQUEST init");

        super.initFromBuffer(buf);

        //  table name length
        int tableNameLength = buf.getInt();

        //  table name
        byte[] bytes = new byte[tableNameLength];
        buf.get(bytes);
        m_tableName = new String(bytes);

        //  graph view name length
        int graphViewNameLength = buf.getInt();

        //  graph view name
        byte[] bytes2 = new byte[graphViewNameLength];
        buf.get(bytes2);
        m_graphViewName = new String(bytes2);
    }
}
