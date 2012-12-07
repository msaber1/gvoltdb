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

package org.voltdb.iv2;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientInterfaceHandleManager;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

public class Iv2Trace
{
    private static final VoltLogger iv2log = new VoltLogger("IV2TRACE");
    private static final VoltLogger iv2queuelog = new VoltLogger("IV2QUEUETRACE");

    // Log messages are passed to a separate thread through this queue
    private static final LinkedBlockingQueue<TaskMsg> msgQueue =
            new LinkedBlockingQueue<Iv2Trace.TaskMsg>();
    private static final ExecutorService es =
            Executors.newFixedThreadPool(1, CoreUtils.getThreadFactory("IV2TRACE"));
    private static final Runnable log4jWorker = new Runnable() {
        @Override
        public void run()
        {
            while (true) {
                TaskMsg msg;
                try {
                    msg = msgQueue.take();
                    if (msg.action.equals(ACTION.TXN_OFFER) || msg.action.equals(ACTION.TASK_OFFER)) {
                        iv2queuelog.trace(msg.toString());
                    } else {
                        iv2log.trace(msg.toString());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    iv2log.trace("Terminating IV2 trace log thread");
                    break;
                }
            }
        }
    };
    private static final Runnable dbWorker = new Runnable() {
        private final Client m_client = ClientFactory.createClient();

        private void connectToServers()
        {
            String servers = System.getenv().get("IV2TRACE_DB");
            String[] serverArray = servers.split(",");
            for (int i = 0; i < serverArray.length; i++) {
                try {
                    m_client.createConnection(serverArray[i]);
                } catch (UnknownHostException e) {
                    iv2log.error("Failed to connect to IV2 trace database: " + e.getMessage());
                } catch (IOException e) {
                    iv2log.trace("Unable to connect to IV2 trace database yet: " + e.getMessage());
                    try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
                    i--; // retry this server
                }
            }
            iv2log.trace("Connected to IV2 trace database: " + servers);
        }

        @Override
        public void run() {
            connectToServers();

            while (true) {
                TaskMsg msg = null;
                try {
                    msg = msgQueue.take();

                    // Long.MIN will be treated as NULL in the DB,
                    // convert it to -1, maybe no valid txnId will be -1?
                    long txnId = -1;
                    if (msg.txnId != Long.MIN_VALUE) {
                        txnId = msg.txnId;
                    }
                    byte isMP = -1;
                    if (msg.isMP != null) {
                        isMP = msg.isMP ? 1 : (byte) 0;
                    }

                    m_client.callProcedure("AddMsg",
                                           txnId,
                                           msg.action.ordinal(),
                                           msg.type == null ? null : msg.type.ordinal(),
                                           msg.localHSId,
                                           msg.sourceHSId,
                                           msg.ciHandle,
                                           msg.coordHSId,
                                           msg.spHandle,
                                           msg.truncationHandle,
                                           isMP,
                                           msg.procName,
                                           msg.status);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    iv2log.trace("Terminating IV2 trace log thread");
                    break;
                } catch (IOException e) {
                    iv2log.trace("Connection to IV2 trace database lost, logging to log4j", e);
                    iv2log.trace(msg.toString());
                    es.execute(log4jWorker);
                    break;
                } catch (Exception e) {
                    iv2log.trace("Exception on IV2 trace database", e);
                    iv2log.trace(msg.toString());
                    es.execute(log4jWorker);
                    break;
                }
            }
        }
    };

    static
    {
        if (iv2log.isTraceEnabled() || iv2queuelog.isTraceEnabled()) {
            if (System.getenv().get("IV2TRACE_DB") == null) {
                es.execute(log4jWorker);
            } else {
                es.execute(dbWorker);
            }
        }
    }

    private enum ACTION {
        CREATE     ("createTxn"),
        FINISH     ("finishTxn"),
        RECEIVE    ("recvMsg"),
        TXN_OFFER  ("txnQOffer"),
        TASK_OFFER ("tskQOffer");

        private final String shortName;
        private ACTION(String shortName)
        {
            this.shortName = shortName;
        }

        @Override
        public String toString()
        {
            return shortName;
        }
    }

    private enum MSG_TYPE {
        Iv2InitiateTaskMessage           ("InitMsg"),
        InitiateResponseMessage          ("InitRsp"),
        FragmentTaskMessage              ("FragMsg"),
        FragmentResponseMessage          ("FragRsp"),
        MultiPartitionParticipantMessage ("SntlMsg"),
        BorrowTaskMessage                ("BrrwMsg");

        private final String shortName;
        private MSG_TYPE(String shortName)
        {
            this.shortName = shortName;
        }

        public static MSG_TYPE typeFromMsg(VoltMessage msg)
        {
            String simpleName = msg.getClass().getSimpleName();
            return MSG_TYPE.valueOf(simpleName);
        }

        @Override
        public String toString() {
            return shortName;
        }
    }

    private static class TaskMsg {
        public final ACTION action;
        public final MSG_TYPE type;

        public final long localHSId;
        public final long sourceHSId;
        public final long ciHandle;
        public final long coordHSId;
        public final long txnId;
        public final long spHandle;
        public final long truncationHandle;
        public final Boolean isMP;
        public final String procName;
        public final byte status;

        public TaskMsg(ACTION action,
                       MSG_TYPE type,
                       long localHSId,
                       long sourceHSId,
                       long ciHandle,
                       long coordHSId,
                       long txnId,
                       long spHandle,
                       long truncationHandle,
                       Boolean isMP,
                       String procName,
                       byte status)
        {
            this.action = action;
            this.type = type;
            this.localHSId = localHSId;
            this.sourceHSId = sourceHSId;
            this.ciHandle = ciHandle;
            this.coordHSId = coordHSId;
            this.txnId = txnId;
            this.spHandle = spHandle;
            this.truncationHandle = truncationHandle;
            this.isMP = isMP;
            this.procName = procName;
            this.status = status;
        }

        @Override
        public String toString()
        {
            String procType = "UNKNOWN";
            if (isMP != null) {
                if (isMP == true) {
                    procType = "MP";
                } else {
                    procType = "SP";
                }
            }

            return String.format("%s %s %s from %s ciHandle %s initHSId %s txnId %s " +
                                 "spHandle %s trunc %s type %s proc %s status %s",
                                 action.toString(),
                                 type == null ? "" : type.toString(),
                                 CoreUtils.hsIdToString(localHSId),
                                 CoreUtils.hsIdToString(sourceHSId),
                                 ClientInterfaceHandleManager.handleToString(ciHandle),
                                 CoreUtils.hsIdToString(coordHSId),
                                 txnIdToString(txnId),
                                 txnIdToString(spHandle),
                                 txnIdToString(truncationHandle),
                                 procType,
                                 procName,
                                 statusToString(status));
        }
    }

    public static void logTopology(long leaderHSId, List<Long> replicas, int partitionId)
    {
        if (iv2log.isTraceEnabled()) {
            String logmsg = "topology partition %d leader %s replicas (%s)";
            iv2log.trace(String.format(logmsg, partitionId, CoreUtils.hsIdToString(leaderHSId),
                    CoreUtils.hsIdCollectionToString(replicas)));
        }
    }

    public static void logCreateTransaction(Iv2InitiateTaskMessage msg)
    {
        if (iv2log.isTraceEnabled()) {
            TaskMsg logmsg = new TaskMsg(ACTION.CREATE,
                                         MSG_TYPE.typeFromMsg(msg),
                                         msg.getInitiatorHSId(),
                                         msg.m_sourceHSId,
                                         msg.getClientInterfaceHandle(),
                                         msg.getCoordinatorHSId(),
                                         msg.getTxnId(),
                                         msg.getSpHandle(),
                                         msg.getTruncationHandle(),
                                         msg.isSinglePartition() == false,
                                         msg.getStoredProcedureInvocation().getProcName(),
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }

    public static void logFinishTransaction(InitiateResponseMessage msg, long localHSId)
    {
        if (iv2log.isTraceEnabled()) {
            TaskMsg logmsg = new TaskMsg(ACTION.FINISH,
                                         MSG_TYPE.typeFromMsg(msg),
                                         localHSId,
                                         msg.m_sourceHSId,
                                         msg.getClientInterfaceHandle(),
                                         msg.getCoordinatorHSId(),
                                         msg.getTxnId(),
                                         msg.getSpHandle(),
                                         0,
                                         null,
                                         "",
                                         msg.getClientResponseData().getStatus());
            msgQueue.offer(logmsg);
        }
    }

    private static String txnIdToString(long txnId)
    {
        if (txnId == Long.MIN_VALUE) {
            return "UNUSED";
        }
        else {
            return TxnEgo.txnIdToString(txnId);
        }
    }

    private static String statusToString(byte status)
    {
        switch(status) {
            case ClientResponse.SUCCESS:
            // or FragmentResponseMessage.SUCCESS
                return "SUCCESS";
            case ClientResponse.USER_ABORT:
            case FragmentResponseMessage.USER_ERROR:
                return "USER_ABORT";
            case ClientResponse.GRACEFUL_FAILURE:
                return "GRACEFUL_FAILURE";
            case ClientResponse.UNEXPECTED_FAILURE:
            case FragmentResponseMessage.UNEXPECTED_ERROR:
                return "UNEXPECTED_FAILURE";
            case ClientResponse.CONNECTION_LOST:
                return "CONNECTION_LOST";
            case ClientResponse.SERVER_UNAVAILABLE:
                return "SERVER_UNAVAILABLE";
            case ClientResponse.CONNECTION_TIMEOUT:
                return "CONNECTION_TIMEOUT";
        }
        return "UNKNOWN_STATUS";
    }

    public static void logInitiatorRxMsg(VoltMessage msg, long localHSId)
    {
        if (iv2log.isTraceEnabled()) {
            if (msg instanceof InitiateResponseMessage) {
                InitiateResponseMessage iresp = (InitiateResponseMessage)msg;
                TaskMsg logmsg = new TaskMsg(ACTION.RECEIVE,
                                             MSG_TYPE.typeFromMsg(msg),
                                             localHSId,
                                             iresp.m_sourceHSId,
                                             iresp.getClientInterfaceHandle(),
                                             iresp.getCoordinatorHSId(),
                                             iresp.getTxnId(),
                                             iresp.getSpHandle(),
                                             Long.MIN_VALUE,
                                             null,
                                             "",
                                             iresp.getClientResponseData().getStatus());
                msgQueue.offer(logmsg);
            }
            else if (msg instanceof FragmentResponseMessage) {
                FragmentResponseMessage fresp = (FragmentResponseMessage)msg;
                TaskMsg logmsg = new TaskMsg(ACTION.RECEIVE,
                                             MSG_TYPE.typeFromMsg(msg),
                                             localHSId,
                                             fresp.m_sourceHSId,
                                             0,
                                             -1,
                                             fresp.getTxnId(),
                                             fresp.getSpHandle(),
                                             Long.MIN_VALUE,
                                             null,
                                             "",
                                             fresp.getStatusCode());
                msgQueue.offer(logmsg);
            }
        }
    }

    public static void logIv2InitiateTaskMessage(Iv2InitiateTaskMessage itask, long localHSId, long txnid,
            long spHandle)
    {
        if (iv2log.isTraceEnabled()) {
            if (itask.getTxnId() != Long.MIN_VALUE && itask.getTxnId() != txnid) {
                iv2log.error("Iv2InitiateTaskMessage TXN ID conflict.  Message: " + itask.getTxnId() +
                        ", locally held: " + txnid);
            }
            if (itask.getSpHandle() != Long.MIN_VALUE && itask.getSpHandle() != spHandle) {
                iv2log.error("Iv2InitiateTaskMessage SP HANDLE conflict.  Message: " + itask.getSpHandle() +
                        ", locally held: " + spHandle);
            }

            TaskMsg logmsg = new TaskMsg(ACTION.RECEIVE,
                                         MSG_TYPE.typeFromMsg(itask),
                                         localHSId,
                                         itask.m_sourceHSId,
                                         itask.getClientInterfaceHandle(),
                                         itask.getCoordinatorHSId(),
                                         itask.getTxnId(),
                                         itask.getSpHandle(),
                                         itask.getTruncationHandle(),
                                         itask.isSinglePartition() == false,
                                         itask.getStoredProcedureInvocation().getProcName(),
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }

    public static void logIv2MultipartSentinel(MultiPartitionParticipantMessage message, long localHSId,
            long txnId)
    {
        if (iv2log.isTraceEnabled()) {
            TaskMsg logmsg = new TaskMsg(ACTION.RECEIVE,
                                         MSG_TYPE.typeFromMsg(message),
                                         localHSId,
                                         message.m_sourceHSId,
                                         0,
                                         message.getCoordinatorHSId(),
                                         message.getTxnId(),
                                         message.getSpHandle(),
                                         message.getTruncationHandle(),
                                         message.isSinglePartition() == false,
                                         "",
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }

    public static void logFragmentTaskMessage(FragmentTaskMessage ftask, long localHSId)
    {
        if (iv2log.isTraceEnabled()) {
            TaskMsg logmsg = new TaskMsg(ACTION.RECEIVE,
                                         MSG_TYPE.typeFromMsg(ftask),
                                         localHSId,
                                         ftask.m_sourceHSId,
                                         0,
                                         ftask.getCoordinatorHSId(),
                                         ftask.getTxnId(),
                                         ftask.getSpHandle(),
                                         ftask.getTruncationHandle(),
                                         ftask.isSinglePartition() == false,
                                         "",
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }

    public static void logBorrowTaskMessage(BorrowTaskMessage msg, long localHSId, long spHandle)
    {
        if (iv2log.isTraceEnabled()) {
            FragmentTaskMessage ftask = msg.getFragmentTaskMessage();
            if (ftask.getSpHandle() != Long.MIN_VALUE && ftask.getSpHandle() != spHandle) {
                iv2log.error("FragmentTaskMessage SP HANDLE conflict.  Message: " + ftask.getSpHandle() +
                             ", locally held: " + spHandle);
            }

            TaskMsg logmsg = new TaskMsg(ACTION.RECEIVE,
                                         MSG_TYPE.typeFromMsg(msg),
                                         localHSId,
                                         ftask.m_sourceHSId,
                                         0,
                                         ftask.getCoordinatorHSId(),
                                         ftask.getTxnId(),
                                         ftask.getSpHandle(),
                                         ftask.getTruncationHandle(),
                                         ftask.isSinglePartition() == false,
                                         "",
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }

    public static void logTransactionTaskQueueOffer(TransactionTask task)
    {
        if (iv2queuelog.isTraceEnabled()) {
            TransactionState txn = task.m_txn;
            StoredProcedureInvocation invocation = txn.getInvocation();
            TaskMsg logmsg = new TaskMsg(ACTION.TXN_OFFER,
                                         null,
                                         -1,
                                         -1,
                                         0,
                                         txn.coordinatorSiteId,
                                         task.getTxnId(),
                                         task.getSpHandle(),
                                         Long.MIN_VALUE,
                                         txn.isSinglePartition() == false,
                                         invocation == null ? "" : invocation.getProcName(),
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }

    public static void logSiteTaskerQueueOffer(TransactionTask task)
    {
        if (iv2queuelog.isTraceEnabled()) {
            TransactionState txn = task.m_txn;
            StoredProcedureInvocation invocation = txn.getInvocation();
            TaskMsg logmsg = new TaskMsg(ACTION.TASK_OFFER,
                                         null,
                                         -1,
                                         -1,
                                         0,
                                         txn.coordinatorSiteId,
                                         task.getTxnId(),
                                         task.getSpHandle(),
                                         Long.MIN_VALUE,
                                         txn.isSinglePartition() == false,
                                         invocation == null ? "" : invocation.getProcName(),
                                         (byte)0);
            msgQueue.offer(logmsg);
        }
    }
}
