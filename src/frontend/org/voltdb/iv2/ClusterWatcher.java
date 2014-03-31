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

package org.voltdb.iv2;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.TheHashinator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltZK;

/**
 * Single place to do partition, replica queries. Encapsulate zookeeper structure and cache information to minmize zk
 * hits.
 */
public class ClusterWatcher {
    private static final VoltLogger tmLog = new VoltLogger("HOST");

    private final ZooKeeper m_zk;
    private final int m_kfactor;

    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("ClusterWatcher");
    private boolean m_isKSafe;
    private final KSafetyStats m_stats;
    private final AtomicBoolean m_needsRefresh = new AtomicBoolean(true);

    class PartitionInformation {
        final int m_pid;
        public byte m_state = 0; //Initializing.
        public boolean m_partitionOnRing;
        //Partition to replicas
        List<Integer> m_replicaHost = new ArrayList<>();
        final boolean m_mpi;

        PartitionInformation(int pid, boolean mpi) {
            m_pid = pid;
            m_mpi = mpi;
        }
    }
    private Map<Integer, PartitionInformation> m_partitionInfo = new HashMap<>();

    public ClusterWatcher(HostMessenger hm, int kfactor, KSafetyStats stats) {
        m_zk = hm.getZK();
        m_kfactor = kfactor;
        m_stats = stats;
        m_stats.setClusterWatcher(this);
    }

    public void dumpToLog() {
        for (PartitionInformation pinfo : m_partitionInfo.values()) {
            StringBuilder sb = new StringBuilder();
            sb.append("ClusterWatcher: Info for Partition: ").append(pinfo.m_pid)
                    .append(" Partition Hosts: ").append(pinfo.m_replicaHost)
                    .append(" Partition On Ring: ").append(pinfo.m_partitionOnRing);

            tmLog.info(sb.toString());
        }
    }

    private class StateWatcher implements Watcher {
        @Override
        public void process(final WatchedEvent event) {
            tmLog.info("ClusterWatcher: Setting refresh to true as zk watcher fired.");
            m_needsRefresh.set(true);
        }
    }
    private final StateWatcher stateWatcher = new StateWatcher();

    public boolean needsReload() {
        return m_needsRefresh.get();
    }

    public boolean refresh() {
        try {
            return m_es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return reload();
                }
            }).get();
        } catch (InterruptedException | ExecutionException t) {
            tmLog.error("ClusterWatcher: Error in submitting task to cluster watcher.", t);
            return false;
        }
    }

    public boolean isClusterKSafeAfterIDie() {
        try {
            return m_es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    reload();
                    boolean retval = true;
                    for (PartitionInformation pinfo : m_partitionInfo.values()) {
                        if (!pinfo.m_partitionOnRing || pinfo.m_mpi) {
                            continue;
                        }
                        if (pinfo.m_replicaHost.size() <= 1) {
                            retval = false;
                            break;
                        }
                    }
                    if (retval) {
                        //Since someone is going to die set to refresh.
                        m_needsRefresh.set(true);
                    }
                    return retval;
                }
            }).get();
        } catch (InterruptedException | ExecutionException t) {
            tmLog.error("ClusterWatcher: Error in isClusterKSafeAfterIDie returning cached value.", t);
            m_needsRefresh.set(true);
            synchronized (ClusterWatcher.class) {
                return m_isKSafe;
            }
        }
    }

    // Reload partition and replica information.
    // also set watcher in th process so any changes will be then tagged for refresh
    private Boolean reload() {
        tmLog.info("ClusterWatcher: Reloading partition information.");
        List<String> partitionDirs = null;
        final Map<Integer, PartitionInformation> partitionInfo = new HashMap<>();
        final Set<Integer> hostsOnRing = new HashSet<>();
        try {
            partitionDirs = m_zk.getChildren(VoltZK.leaders_initiators, stateWatcher);
        } catch (KeeperException | InterruptedException e) {
            throw Throwables.propagate(e);
        }

        ImmutableSortedSet.Builder<KSafetyStats.StatsPoint> lackingReplication
                = ImmutableSortedSet.naturalOrder();

        //Don't fetch the values serially do it asynchronously
        Queue<ZKUtil.ByteArrayCallback> dataCallbacks = new ArrayDeque<>();
        Queue<ZKUtil.ChildrenCallback> childrenCallbacks = new ArrayDeque<>();
        for (String partitionDir : partitionDirs) {
            String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            try {
                ZKUtil.ByteArrayCallback callback = new ZKUtil.ByteArrayCallback();
                m_zk.getData(dir, false, callback, null);
                dataCallbacks.offer(callback);
                ZKUtil.ChildrenCallback childrenCallback = new ZKUtil.ChildrenCallback();
                m_zk.getChildren(dir, false, childrenCallback, stateWatcher);
                childrenCallbacks.offer(childrenCallback);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        //Assume that we are ksafe
       boolean isKSafe = true;
        final long statTs = System.currentTimeMillis();
        boolean partitionRemovalDetected = false;
        for (String partitionDir : partitionDirs) {
            int pid = ClusterWatcher.getPartitionFromElectionDir(partitionDir);
            PartitionInformation pinfo;
            if (pid == MpInitiator.MP_INIT_PID) {
                pinfo = new PartitionInformation(pid, true);
            } else {
                pinfo = new PartitionInformation(pid, false);
            }

            try {
                // The data of the partition dir indicates whether the partition has finished
                // initializing or not. If not, the replicas may still be in the process of
                // adding themselves to the dir. So don't check for k-safety if that's the case.
                byte[] partitionState = dataCallbacks.poll().getData();
                if (partitionState != null && partitionState.length == 1) {
                    pinfo.m_state = partitionState[0];
                }

                //Leave watch
                String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
                m_zk.getChildren(dir, stateWatcher);
                List<String> replicas = childrenCallbacks.poll().getChildren();
                pinfo.m_partitionOnRing = partitionOnHashRing(pid);
                if (replicas.isEmpty() && !pinfo.m_mpi) {
                    //These partitions can fail, just cleanup and remove the partition from the system
                    if (!pinfo.m_partitionOnRing && !pinfo.m_mpi) {
                        //Cleanup partition not on ring.
                        partitionRemovalDetected = true;
                        continue;
                    }
                    tmLog.fatal("ClusterWatcher: K-Safety violation: No replicas found for partition: " + pid);
                    isKSafe = false;
                }
                //Record host ids for all partitions that are on the ring
                //so they are considered for partition detection
                final List<Integer> replicaHost = new ArrayList<>();
                for (String replica : replicas) {
                    final String split[] = replica.split("/");
                    final long hsId = Long.valueOf(split[split.length - 1].split("_")[0]);
                    final int hostId = CoreUtils.getHostIdFromHSId(hsId);
                    replicaHost.add(hostId);
                    if (pinfo.m_partitionOnRing) {
                        hostsOnRing.add(hostId);
                    }
                }
                pinfo.m_replicaHost = replicaHost;
                partitionInfo.put(pid, pinfo);
                if (pinfo.m_state != LeaderElector.INITIALIZING && pinfo.m_partitionOnRing) {
                    lackingReplication.add(new KSafetyStats.StatsPoint(statTs, pid, m_kfactor + 1 - replicas.size()));
                }
            } catch (InterruptedException | KeeperException | NumberFormatException e) {
                throw Throwables.propagate(e);
            }
        }
        synchronized (ClusterWatcher.class) {
            m_partitionInfo = partitionInfo;
            m_isKSafe = isKSafe;
            m_stats.setSafetySet(lackingReplication.build());
            //Set to refresh next time if we had a delete pending.
            m_needsRefresh.set(partitionRemovalDetected);
        }
        return true;
    }

    public static boolean partitionOnHashRing(int pid) {
        if (TheHashinator.getConfiguredHashinatorType() == TheHashinator.HashinatorType.LEGACY) {
            return true;
        }
        return !TheHashinator.getRanges(pid).isEmpty();
    }

    public static boolean partitionNotOnHashRing(int pid) {
        if (TheHashinator.getConfiguredHashinatorType() == TheHashinator.HashinatorType.LEGACY) {
            return false;
        }
        return TheHashinator.getRanges(pid).isEmpty();
    }

    public static String electionDirForPartition(int partition) {
        return ZKUtil.path(VoltZK.leaders_initiators, "partition_" + partition);
    }

    public static int getPartitionFromElectionDir(String partitionDir) {
        return Integer.parseInt(partitionDir.substring("partition_".length()));
    }

}
