/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.rejoin;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.exceptions.SerializableException;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Thread that blocks on the receipt of Acks.
 */
public class StreamSnapshotAckReceiver implements Runnable {
    public static interface AckCallback {
        public void receiveAck(int blockIndex);
    }

    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final Mailbox m_mb;
    private final StreamSnapshotBase.MessageFactory m_msgFactory;
    private final Map<Long, AckCallback> m_callbacks;
    private final AtomicInteger m_expectedEOFs;

    volatile Exception m_lastException = null;

    final static long WATCHDOG_PERIOD_S = 5;

    class SiteTrackingData {
        long acksSince = 0;
        long totalAcks = 0;
    }

    final Map<String, SiteTrackingData> m_ackTracking = new HashMap<String, SiteTrackingData>();

    public StreamSnapshotAckReceiver(Mailbox mb)
    {
        this(mb, new StreamSnapshotBase.DefaultMessageFactory());
    }

    public StreamSnapshotAckReceiver(Mailbox mb, StreamSnapshotBase.MessageFactory msgFactory) {
        Preconditions.checkArgument(mb != null);
        m_mb = mb;
        m_msgFactory = msgFactory;
        m_callbacks = Collections.synchronizedMap(new HashMap<Long, AckCallback>());
        m_expectedEOFs = new AtomicInteger();

        // start a periodic task to look for timed out connections
        VoltDB.instance().scheduleWork(new Watchdog(), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
    }

    public void setCallback(long targetId, AckCallback callback) {
        m_expectedEOFs.incrementAndGet();
        m_callbacks.put(targetId, callback);
    }

    @Override
    public void run() {
        rejoinLog.trace("Starting ack receiver thread");

        try {
            while (true) {
                rejoinLog.trace("Blocking on receiving mailbox");
                VoltMessage msg = m_mb.recvBlocking(10 * 60 * 1000); // Wait for 10 minutes
                if (msg == null) {
                    rejoinLog.warn("No stream snapshot ack message was received in the past 10 minutes" +
                                   " or the thread was interrupted");
                    continue;
                }

                // TestMidRejoinDeath ignores acks to trigger the watchdog
                if (StreamSnapshotDataTarget.m_rejoinDeathTestMode && (m_msgFactory.getAckTargetId(msg) == 1)) {
                    continue;
                }

                SerializableException se = m_msgFactory.getException(msg);
                if (se != null) {
                    m_lastException = se;
                    rejoinLog.error("Received exception in ack receiver", se);
                    return;
                }

                String trackingKey =
                        CoreUtils.hsIdToString(msg.m_sourceHSId) +
                        "=>" +
                        CoreUtils.hsIdToString(m_msgFactory.getAckTargetId(msg));

                synchronized (m_ackTracking) {
                    SiteTrackingData trackingData = m_ackTracking.get(trackingKey);
                    if (trackingData != null) {
                        trackingData.acksSince++;
                        trackingData.totalAcks++;
                    }
                    else {
                        trackingData = new SiteTrackingData();
                        trackingData.acksSince = trackingData.totalAcks = 1;
                        m_ackTracking.put(trackingKey, trackingData);
                    }
                }

                AckCallback ackCallback = m_callbacks.get(m_msgFactory.getAckTargetId(msg));
                if (ackCallback == null) {
                    rejoinLog.error("Unknown target ID " + m_msgFactory.getAckTargetId(msg) +
                                    " in stream snapshot ack message");
                } else if (m_msgFactory.getAckBlockIndex(msg) != -1) {
                    ackCallback.receiveAck(m_msgFactory.getAckBlockIndex(msg));
                }

                if (m_msgFactory.isAckEOS(msg)) {
                    // EOS message indicates end of stream.
                    // The receiver is shared by multiple data targets, each of them will
                    // send an end of stream message, must wait until all end of stream
                    // messages are received before terminating the thread.
                    if (m_expectedEOFs.decrementAndGet() == 0) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            m_lastException = e;
            rejoinLog.error("Error reading a message from a recovery stream", e);
        } finally {
            rejoinLog.trace("Ack receiver thread exiting");
        }
    }

    /**
     * Task run every so often to look for writes that haven't been acked
     * in writeTimeout time.
     */
    class Watchdog implements Runnable {

        @Override
        public synchronized void run() {

            Set<String> zeroPairs = new HashSet<>();

            synchronized (m_ackTracking) {
                for (Entry<String, SiteTrackingData> e : m_ackTracking.entrySet()) {
                    if (e.getValue().acksSince == 0) {
                        zeroPairs.add(e.getKey() + ":" + String.valueOf(e.getValue().totalAcks));
                    }
                    else {
                        rejoinLog.info(String.format(
                                "Rejoin snapshot for %s received %d acks in the past %d seconds (% recieved in total so far).",
                                e.getKey(),
                                e.getValue().acksSince,
                                WATCHDOG_PERIOD_S,
                                e.getValue().totalAcks));
                        e.getValue().acksSince = 0;
                    }
                }
            }

            // print sites that didn't send data
            if (!zeroPairs.isEmpty()) {
                String list = StringUtils.join(zeroPairs, ", ");
                rejoinLog.info(String.format(
                        "Rejoin snapshot received no acks in the past %d seconds from %s.",
                        WATCHDOG_PERIOD_S,
                        list));
            }

            // schedule to run again
            VoltDB.instance().scheduleWork(new Watchdog(), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
        }
    }
}
