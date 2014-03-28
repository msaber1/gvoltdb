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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;

/**
 * Sends acks of snapshot blocks to the snapshot sender.
 */
public class StreamSnapshotAckSender implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final Mailbox m_mb;
    private final LinkedBlockingQueue<Pair<Long, RejoinDataAckMessage>> m_blockIndices =
        new LinkedBlockingQueue<Pair<Long, RejoinDataAckMessage>>();

    final static long WATCHDOG_PERIOD_S = 5;

    class SiteTrackingData {
        long acksSince = 0;
        long totalAcks = 0;
    }

    final Map<String, SiteTrackingData> m_ackTracking = new HashMap<String, SiteTrackingData>();

    public StreamSnapshotAckSender(Mailbox mb) {
        m_mb = mb;

        // start a periodic task to look for timed out connections
        VoltDB.instance().scheduleWork(new Watchdog(), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
    }

    public void close() {
        // null message terminates the thread
        m_blockIndices.offer(Pair.of(-1L, (RejoinDataAckMessage) null));
    }

    /**
     * Ack with a positive block index.
     * @param hsId The mailbox to send the ack to
     * @param blockIndex
     */
    public void ack(long hsId, boolean isEOS, long targetId, int blockIndex) {
        m_blockIndices.offer(Pair.of(hsId, new RejoinDataAckMessage(isEOS, targetId, blockIndex)));
    }

    @Override
    public void run() {
        while (true) {
            long hsId;
            RejoinDataAckMessage ackMsg;
            try {
                Pair<Long, RejoinDataAckMessage> work = m_blockIndices.take();
                hsId = work.getFirst();
                ackMsg = work.getSecond();
            } catch (InterruptedException e1) {
                break;
            }

            if (ackMsg == null) {
                rejoinLog.debug(m_blockIndices.size() + " acks remaining, " +
                        "terminating ack sender");
                // special value of -1 terminates the thread
                break;
            }
            else {
                String trackingKey =
                        CoreUtils.hsIdToString(ackMsg.m_sourceHSId) +
                        "=>" +
                        CoreUtils.hsIdToString(hsId);

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
            }

            m_mb.send(hsId, ackMsg);
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
                                "Rejoin snapshot for %s sent %d acks in the past %d seconds (%d sent in total so far).",
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
                        "Rejoin snapshot sent no acks in the past %d seconds from %s.",
                        WATCHDOG_PERIOD_S,
                        list));
            }

            // schedule to run again
            VoltDB.instance().scheduleWork(new Watchdog(), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
        }
    }
}
