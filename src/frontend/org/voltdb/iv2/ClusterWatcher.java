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

import com.google.gwt.thirdparty.guava.common.base.Throwables;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * Single place to do partition, replica queries. Encapsulate zookeeper structure and cache information to minmize zk
 * hits.
 */
public class ClusterWatcher {
    private static final VoltLogger tmLog = new VoltLogger("TM");

    private final ZooKeeper m_zk;
    private Set<Integer> m_hostsOnRing;
    private final int m_kfactor;

    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("ClusterWatcher");
    private boolean m_isKSafe;
    private final KSafetyStats m_stats;
    private AtomicBoolean m_needsRefresh = new AtomicBoolean(true);

    class PartitionInformation {

        final int m_pid;
        public byte m_state = 0; //Initializing.
        public boolean m_partitionOnRing;
        public List<String> m_replicas;
        //Partition to replicas
        public Map<Integer, List<Integer>> m_replicaHost = new HashMap<Integer, List<Integer>>();
        final boolean m_mpi;

        PartitionInformation(int pid, boolean mpi) {
            m_pid = pid;
            m_mpi = mpi;
        }
    }
    private Map<Integer, PartitionInformation> m_partitionInfo = new HashMap<Integer, PartitionInformation>();

    public ClusterWatcher(HostMessenger hm, int kfactor, KSafetyStats stats) {
        m_zk = hm.getZK();
        m_isKSafe = true;
        m_kfactor = kfactor;
        m_stats = stats;
    }

    public void dump() {
        for (PartitionInformation pinfo : m_partitionInfo.values()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Info for Partition: ").append(pinfo.m_pid)
                    .append(" Replicas: ").append(pinfo.m_replicas)
                    .append(" Partition Hosts: ").append(pinfo.m_replicaHost)
                    .append(" Partition On Ring: ").append(pinfo.m_partitionOnRing);

            tmLog.info(sb.toString());
            System.out.println(sb.toString());
        }
    }

    private class StateWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent event) {
            try {
                m_needsRefresh.set(true);
            } catch (RejectedExecutionException e) {
                com.google_voltpatches.common.base.Throwables.propagate(e);
            }
        }
    }
    private final StateWatcher stateWatcher = new StateWatcher();

    public boolean refresh() {
        try {
            return m_es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return reload();
                }
            }).get();
        } catch (Throwable t) {
            com.google_voltpatches.common.base.Throwables.propagate(t);
            return false;
        }

    }

    public boolean isClusterKSafe() {
        try {
            return m_es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    if (m_needsRefresh.get()) {
                        reload();
                    }
                    return m_isKSafe;
                }
            }).get();
        } catch (Throwable t) {
            com.google_voltpatches.common.base.Throwables.propagate(t);
            return m_isKSafe;
        }

    }

    public boolean isClusterKSafeAfterIDie() {
        try {
            return m_es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
//                    if (m_needsRefresh.get()) {
                        reload();
//                    }
                    boolean retval = true;
                    for (PartitionInformation pinfo : m_partitionInfo.values()) {
                        if (!pinfo.m_partitionOnRing || pinfo.m_mpi) {
                            continue;
                        }
                        if (pinfo.m_replicaHost.size() <= (m_kfactor + 1)) {
                            retval = false;
                            break;
                        }
                    }
                    return retval;
                }
            }).get();
        } catch (Throwable t) {
            com.google_voltpatches.common.base.Throwables.propagate(t);
            return m_isKSafe;
        }
    }

    public Set<Integer> getHostsOnRing() {
        try {
            return m_es.submit(new Callable<Set<Integer>>() {
                @Override
                public Set<Integer> call() throws Exception {
                    if (m_needsRefresh.get()) {
                        reload();
                    }
                    return m_hostsOnRing;
                }
            }).get();
        } catch (Throwable t) {
            com.google_voltpatches.common.base.Throwables.propagate(t);
            return m_hostsOnRing;
        }
    }

    public List<Integer> getPartitions() {
        try {
            return m_es.submit(new Callable<List<Integer>>() {
                @Override
                public List<Integer> call() throws Exception {
                    if (m_needsRefresh.get()) {
                        reload();
                    }
                    return new ArrayList(m_partitionInfo.keySet());
                }
            }).get();
        } catch (Throwable t) {
            com.google_voltpatches.common.base.Throwables.propagate(t);
            return new ArrayList(m_partitionInfo.keySet());
        }
    }

    public Integer getPartitionsCount() {
        try {
            return m_es.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    if (m_needsRefresh.get()) {
                        reload();
                    }
                    return m_partitionInfo.size();
                }
            }).get();
        } catch (Throwable t) {
            com.google_voltpatches.common.base.Throwables.propagate(t);
            return m_partitionInfo.size();
        }
    }

    private void setupWatcher() {
        List<String> partitionDirs = null;

        try {
            partitionDirs = m_zk.getChildren(VoltZK.leaders_initiators, stateWatcher);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        final long statTs = System.currentTimeMillis();
        for (String partitionDir : partitionDirs) {
            String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            try {
                //Set watcher to refresh the cache.
                m_zk.exists(dir, stateWatcher);
            } catch (KeeperException ex) {
                Logger.getLogger(ClusterWatcher.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(ClusterWatcher.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private Boolean reload() {
        List<String> partitionDirs = null;
        final Map<Integer, PartitionInformation> partitionInfo = new HashMap<Integer, PartitionInformation>();
        final Set<Integer> hostsOnRing = new HashSet<Integer>();

        try {
            partitionDirs = m_zk.getChildren(VoltZK.leaders_initiators, null);
        } catch (Exception e) {
            Throwables.propagate(e);
        }

        ImmutableSortedSet.Builder<KSafetyStats.StatsPoint> lackingReplication
                = ImmutableSortedSet.naturalOrder();

        //Don't fetch the values serially do it asynchronously
        Queue<ZKUtil.ByteArrayCallback> dataCallbacks = new ArrayDeque<ZKUtil.ByteArrayCallback>();
        Queue<ZKUtil.ChildrenCallback> childrenCallbacks = new ArrayDeque<ZKUtil.ChildrenCallback>();
        for (String partitionDir : partitionDirs) {
            String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            try {
                ZKUtil.ByteArrayCallback callback = new ZKUtil.ByteArrayCallback();
                m_zk.getData(dir, false, callback, null);
                dataCallbacks.offer(callback);
                ZKUtil.ChildrenCallback childrenCallback = new ZKUtil.ChildrenCallback();
                m_zk.getChildren(dir, false, childrenCallback, null);
                childrenCallbacks.offer(childrenCallback);
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
        //Assume that we are ksafe
        m_isKSafe = true;
        final long statTs = System.currentTimeMillis();
        for (String partitionDir : partitionDirs) {
            int pid = ClusterWatcher.getPartitionFromElectionDir(partitionDir);
            PartitionInformation pinfo;
            if (pid == MpInitiator.MP_INIT_PID) {
                pinfo = new PartitionInformation(pid, true);
            } else {
                pinfo = new PartitionInformation(pid, false);
            }

            String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            try {
                // The data of the partition dir indicates whether the partition has finished
                // initializing or not. If not, the replicas may still be in the process of
                // adding themselves to the dir. So don't check for k-safety if that's the case.
                byte[] partitionState = dataCallbacks.poll().getData();
                if (partitionState != null && partitionState.length == 1) {
                    pinfo.m_state = partitionState[0];
                }

                List<String> replicas = childrenCallbacks.poll().getChildren();
                pinfo.m_replicas = replicas;
                pinfo.m_partitionOnRing = partitionOnHashRing(pid);
                if (replicas.isEmpty() && !pinfo.m_mpi) {
                    //These partitions can fail, just cleanup and remove the partition from the system
                    if (pinfo.m_partitionOnRing && !pinfo.m_mpi) {
                        //Add cleanup.
                        removeAndCleanupPartition(pid);
                        continue;
                    }
                    tmLog.fatal("K-Safety violation: No replicas found for partition: " + pid);
                    m_isKSafe = false;
                }
                //Record host ids for all partitions that are on the ring
                //so they are considered for partition detection
                for (String replica : replicas) {
                    final String split[] = replica.split("/");
                    final long hsId = Long.valueOf(split[split.length - 1].split("_")[0]);
                    final int hostId = CoreUtils.getHostIdFromHSId(hsId);
                    List<Integer> hlist = pinfo.m_replicaHost.get(pid);
                    if (hlist != null) {
                        hlist.add(hostId);
                    } else {
                        hlist = new ArrayList<Integer>();
                        hlist.add(hostId);
                        pinfo.m_replicaHost.put(pid, hlist);
                    }
                }
                partitionInfo.put(pid, pinfo);
                if (pinfo.m_partitionOnRing) {
                    hostsOnRing.add(pinfo.m_pid);
                    lackingReplication.add(new KSafetyStats.StatsPoint(statTs, pid, m_kfactor + 1 - replicas.size()));
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to read replicas in ZK dir: " + dir, true, e);
            }
        }
        synchronized (m_partitionInfo) {
            m_partitionInfo = partitionInfo;
            m_hostsOnRing = hostsOnRing;
        }
        m_stats.setSafetySet(lackingReplication.build());
        m_needsRefresh.set(false);
        setupWatcher();
        dump();
        return true;
    }

    private void removeAndCleanupPartition(int pid) {
        tmLog.info("Removing and cleanup up partition info for partition " + pid);
        try {
            ZKUtil.asyncDeleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.iv2masters, String.valueOf(pid)));
            ZKUtil.asyncDeleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.iv2appointees, String.valueOf(pid)));
            ZKUtil.asyncDeleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.leaders_initiators, "partition_" + String.valueOf(pid)));
        } catch (Exception e) {
            tmLog.error("Error removing partition info", e);
        }
    }

    private static boolean partitionOnHashRing(int pid) {
        if (TheHashinator.getConfiguredHashinatorType() == TheHashinator.HashinatorType.LEGACY) return false;
        return !TheHashinator.getRanges(pid).isEmpty();
    }

    public static String electionDirForPartition(int partition) {
        return ZKUtil.path(VoltZK.leaders_initiators, "partition_" + partition);
    }

    public static int getPartitionFromElectionDir(String partitionDir) {
        return Integer.parseInt(partitionDir.substring("partition_".length()));
    }

}
