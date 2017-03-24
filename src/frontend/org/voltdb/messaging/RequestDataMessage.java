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
//import java.util.ArrayList;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
//import org.voltdb.iv2.InitiatorMailbox;

public class RequestDataMessage extends TransactionInfoBaseMessage {

    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    private long m_sourceSiteId;
    private long m_destinationSiteId;
    private String m_tableName;

    // Empty constructor for de-serialization
    RequestDataMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataMessage(long sourceSiteId,
                              long destinationSiteId,
                              String tableName,
                              long txnId,
                              long uniqueId,
                              boolean isReadOnly,
                              boolean isForReplay) {
        super(sourceSiteId, destinationSiteId, txnId, uniqueId, isReadOnly, isForReplay);
        m_sourceSiteId = sourceSiteId;
        m_destinationSiteId = destinationSiteId;
        m_tableName = tableName;
        m_subject = Subject.DEFAULT.getId();
    }
/*
    public RequestDataMessage(long sourceSiteId, long destinationSiteId, RequestDataMessage msg) {
        super(sourceSiteId, destinationHSId, msg.m_txnId, msg.m_uniqueId, msg.m_isReadOnly, msg.m_isForReplay);
        m_sourceSiteId = sourceSiteId;
        m_destinationSiteId = destinationSiteId;
        m_subject = Subject.DEFAULT.getId();
    }
*/
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
/*
    // an original sender
    public void setSender(InitiatorMailbox sender) {
        m_sender = sender;
    }

    public InitiatorMailbox getSender() {
        return m_sender;
    }
*/
    @Override
    public int getSerializedSize()
    {
        int additional = 4 + m_tableName.length();
        return super.getSerializedSize() + additional;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        System.out.println("REQUEST flatten");
        System.out.println(buf.capacity());
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.remaining());

        //  message id
        buf.put(VoltDbMessageFactory.REQUEST_DATA_ID);
        super.flattenToBuffer(buf);

        //  table name
        buf.putInt(m_tableName.length());
        buf.put(m_tableName.getBytes());

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());

        // buf.position(buf.capacity());
        // buf.limit(buf.position());

        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.remaining());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        System.out.println("REQUEST init");
        System.out.println(buf.capacity());
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.remaining());

        super.initFromBuffer(buf);

        //  table name
        int tableNameLength = buf.getInt();
        byte[] bytes = new byte[tableNameLength];
        buf.get(bytes);
        m_tableName = new String(bytes);
    }
}
