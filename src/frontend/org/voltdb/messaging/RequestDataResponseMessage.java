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

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable;
//import org.voltdb.iv2.InitiatorMailbox;

public class RequestDataResponseMessage extends VoltMessage {

    public static final byte SUCCESS          = 1;
    public static final byte USER_ERROR       = 2;
    public static final byte UNEXPECTED_ERROR = 3;

    //private InitiatorMailbox m_sender;

    private long m_sourceSiteId;
    private long m_destinationSiteId;
    private long m_txnId;
    private long m_spHandle;

    private ByteBuffer m_requestTableBuffer;

    // Empty constructor for de-serialization
    RequestDataResponseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataResponseMessage(long destinationSiteId, RequestDataMessage requestMessage) {
        m_sourceSiteId = requestMessage.getDestinationSiteId();
        m_destinationSiteId = destinationSiteId;
        m_txnId = requestMessage.getTxnId();
        m_spHandle = requestMessage.getSpHandle();
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
    // executor site = site that will be sending the data to destination site
    public long getSourceSiteId() {
        return m_sourceSiteId;
    }

    public void setSourceSiteId(long srcSiteId) {
        m_sourceSiteId = srcSiteId;
    }

    // destination site = site that requested the data
    public long getDestinationSiteId() {
        return m_destinationSiteId;
    }

    public void setDestinationSiteId(long destSiteId) {
        m_destinationSiteId = destSiteId;
    }

    public ByteBuffer getRequestTableBuffer() {
        return m_requestTableBuffer;
    }

    public void setRequestTableBuffer(ByteBuffer tableBuffer) {
        m_requestTableBuffer = tableBuffer;
    }
/*
    // a list of tables that holds the requested set of data, in stack order
    public void pushRequestData(VoltTable table) {
        m_requestData.add(table);
    }

    public VoltTable popRequestData() {
        return m_requestData.remove(m_requestData.size()-1);
    }

    public ArrayList<VoltTable> getRequestDatas() {
        return m_requestData;
    }

    // an original sender
    public void setSender(InitiatorMailbox sender) {
        m_sender = sender;
    }

    public InitiatorMailbox getSender() {
        return m_sender;
    }

    public VoltTable getRequestData() {
        return m_requestData.get(m_requestData.size()-1);
    }
*/
    public long getTxnId() {
        return m_txnId;
    }

    public long getSpHandle() {
        return m_spHandle;
    }

    @Override
    public int getSerializedSize()
    {
        int additional = m_requestTableBuffer.capacity();
        System.out.println("RESPONSE table size: " + additional);
        return super.getSerializedSize() + additional;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf)
    {
        System.out.println("RESPONSE flatten");

        //  message id
        buf.put(VoltDbMessageFactory.REQUEST_DATA_RESPONSE_ID);

        //  table byte buffer
        buf.put(m_requestTableBuffer);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    //  capacity = 1024
    //  limit = 1024
    //  position = 0
    //  remaining = 1024
    @Override
    public void initFromBuffer(ByteBuffer buf)
    {
        System.out.println("RESPONSE init");

        //  copy table byte buffer
        m_requestTableBuffer = ByteBuffer.allocate(buf.capacity());

        buf.rewind();
        m_requestTableBuffer.put(buf);
        buf.rewind();
        m_requestTableBuffer.flip();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("Request Data Response (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceSiteId));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(m_destinationSiteId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append(", SP HANDLE: ");
        sb.append(m_spHandle);

        return sb.toString();
    }

}
