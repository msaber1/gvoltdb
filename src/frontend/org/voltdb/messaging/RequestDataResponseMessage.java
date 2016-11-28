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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable;
import org.voltdb.iv2.InitiatorMailbox;

public class RequestDataResponseMessage extends VoltMessage {

    public static final byte SUCCESS          = 1;
    public static final byte USER_ERROR       = 2;
    public static final byte UNEXPECTED_ERROR = 3;

    private InitiatorMailbox m_sender;

    private long m_executorHSId;
    private long m_destinationHSId;
    private long m_txnId;
    private long m_spHandle;

    ArrayList<Long> m_requestDestinations = new ArrayList<Long>();
    ArrayList<VoltTable> m_requestData = new ArrayList<VoltTable>();

    // Empty constructor for de-serialization
    RequestDataResponseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataResponseMessage(RequestDataMessage task, long destinationHSId) {
        m_executorHSId = task.getDestinationSiteId();
        m_txnId = task.getTxnId();
        m_spHandle = task.getSpHandle();
        m_destinationHSId = destinationHSId;
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataResponseMessage(long executorHSId, long destinationHSId, RequestDataResponseMessage msg) {
        m_executorHSId = executorHSId;
        m_txnId = msg.getTxnId();
        m_spHandle = msg.getSpHandle();
        m_destinationHSId = destinationHSId;
        m_subject = Subject.DEFAULT.getId();
    }

    // getters and setters
    // request destination is the destination of the message.
    // a set of destinations (for chained messaging) is accessed as a stack.
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

    // executor site = site that will be sending the data to destination site
    public long getExecutorSiteId() {
        return m_executorHSId;
    }

    public void setExecutorSiteId(long executorHSId) {
        m_executorHSId = executorHSId;
    }

    // destination site = site that requested the data
    public long getDestinationSiteId() {
        return m_destinationHSId;
    }

    public void setDestinationSiteId(long destHSId) {
        m_destinationHSId = destHSId;
    }

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

    public long getTxnId() {
        return m_txnId;
    }

    public long getSpHandle() {
        return m_spHandle;
    }


    @Override
    public void flattenToBuffer(ByteBuffer buf)
    {

//System.out.println("RESPONSE: " + buf.position() + ", " + buf.capacity());
        buf.put(VoltDbMessageFactory.REQUEST_DATA_RESPONSE_ID);
//System.out.println("RESPONSE: " + buf.position() + ", " + buf.capacity());
        //buf.putLong(m_executorHSId);
        //buf.putLong(m_destinationHSId);
        //buf.putLong(m_txnId);
        //buf.putLong(m_spHandle);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());

    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {

        //m_executorHSId = buf.getLong();
        //m_destinationHSId = buf.getLong();
        //m_txnId = buf.getLong();
        //m_spHandle = buf.getLong();

        //assert(buf.capacity() == buf.position());

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Request Data Response (FROM ");
        sb.append(CoreUtils.hsIdToString(m_executorHSId));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(m_destinationHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append(", SP HANDLE: ");
        sb.append(m_spHandle);

        return sb.toString();
    }

}