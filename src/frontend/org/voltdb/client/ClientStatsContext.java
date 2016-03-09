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

package org.voltdb.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * <p>An object to store and manipulate statistics information from
 * the VoltDB Java client. Each instance has a set of timestamped
 * baseline statistics and a set of timestamped current statistics.
 * Given these two sets of data, this object can return statistics
 * covering the period between the baseline data and current data.</p>
 *
 * <p>An instance is created using {@link Client#createStatsContext()}.
 * Mutliple instances can coexist, each covering a different time
 * period. See the Voter example in /examples for an example of using
 * one context for long term stats and another for short term updates.</p>
 */
public class ClientStatsContext {

    class FullStats {
        final Map<Long, Map<String, ClientStats>> baseStats;
        final Map<Long, ClientIOStats> ioStats;
        final Map<Integer, ClientAffinityStats> affinityStats;
        final long ts;

        FullStats() {
            baseStats = new TreeMap<Long, Map<String, ClientStats>>();
            ioStats = new TreeMap<Long, ClientIOStats>();
            affinityStats = new HashMap<Integer, ClientAffinityStats>();
            ts = System.currentTimeMillis();
        }

        FullStats(Map<Long, Map<String, ClientStats>> base,
                  Map<Long, ClientIOStats> io,
                  Map<Integer, ClientAffinityStats> affinity,
                  long ts) {
            baseStats = base;
            ioStats = io;
            affinityStats = affinity;
            this.ts = ts;
        }
    }

    static final long DEFAULT_TAG = 0;

    final Distributer m_distributor;
    FullStats m_current;
    HashMap<Object, FullStats> m_taggedBaselines = new HashMap<Object, FullStats>();

    ClientStatsContext(Distributer distributor, FullStats stats, HashMap<Object, FullStats> taggedBaselines) {
        m_distributor = distributor;
        m_taggedBaselines.putAll(taggedBaselines);
        m_current = stats;
    }

    ClientStatsContext(Distributer distributor,
                       Map<Long, Map<String, ClientStats>> current,
                       Map<Long, ClientIOStats> currentIO,
                       Map<Integer, ClientAffinityStats> currentAffinity)
    {
        m_distributor = distributor;
        FullStats baseline = new FullStats();
        m_taggedBaselines.put(DEFAULT_TAG, baseline);
        m_current = new FullStats(current, currentIO, currentAffinity, baseline.ts);
    }

    public ClientStatsContext tag(Object tagLabel) {
        m_taggedBaselines.put(tagLabel, m_current);
        return this;
    }

    /**
     * Fetch current statistics from the client internals. Don't
     * update the baseline. This will increase the range covered
     * by any {@link ClientStats} instances returned from this
     * context.
     *
     * @return A <code>this</code> pointer for chaining calls.
     */
    public ClientStatsContext fetch() {
        m_current = new FullStats(m_distributor.getStatsSnapshot(),
                                  m_distributor.getIOStatsSnapshot(),
                                  m_distributor.getAffinityStatsSnapshot(),
                                  System.currentTimeMillis());
        return this;
    }

    /**
     * Fetch current statistics from the client internals and set them to be the current baseline.
     * Subsequent calls to <code>getStats(..)</code> methods on this instance will return 0 values for
     * all statistics until either <code>fetch()</code> or <code>fetchAndResetBaseline()</code> are called.
     *
     * @return A new ClientStatsContext object that uses the newly fetched stats with the old baseline.
     */
    @Deprecated
    public ClientStatsContext fetchAndResetBaseline() {
        fetch();
        ClientStatsContext retval = new ClientStatsContext(m_distributor,
                                                           m_current,
                                                           m_taggedBaselines);
        tag(DEFAULT_TAG);
        return retval;
    }

    /**
     * Return a {@link ClientStats} that covers all procedures and
     * all connection ids. The {@link ClientStats} instance will
     * apply to the time period currently covered by the context.
     *
     * @return A {@link ClientStats} instance.
     */
    public ClientStats getStats() {
        return getStatsSinceTag(DEFAULT_TAG);
    }

    /**
     * Return a {@link ClientStats} that covers all procedures and
     * all connection ids. The {@link ClientStats} instance will
     * apply to the time period currently covered by the context.
     *
     * @return A {@link ClientStats} instance.
     */
    public ClientStats getStatsSinceTag(Object tagLabel) {
        Map<Long, ClientStats> stats = getStatsByConnectionSinceTag(tagLabel);
        if (stats == null) {
            return null;
        }

        return ClientStats.merge(stats.values());
    }


    /**
     * Return a map of {@link ClientStats} by procedure name. This will
     * roll up {@link ClientStats} instances by connection id. Each
     * {@link ClientStats} instance will apply to the time period
     * currently covered by the context.
     *
     * @return A map from procedure name to {@link ClientStats} instances.
     */
    public Map<String, ClientStats> getStatsByProc() {
        return getStatsByProcSinceTag(DEFAULT_TAG);
    }

    public Map<String, ClientStats> getStatsByProcSinceTag(Object tagLabel) {
        Map<Long, Map<String, ClientStats>> complete = getCompleteStatsSinceTag(tagLabel);
        if (complete == null) {
            return null;
        }

        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            for (Entry<String, ClientStats> e2 : e.getValue().entrySet()) {
                ClientStats current = e2.getValue();
                ClientStats aggregate = retval.get(current.getProcedureName());
                if (aggregate == null) {
                    retval.put(current.getProcedureName(), (ClientStats) current.clone());
                }
                else {
                    aggregate.add(current);
                }
            }
        }
        return retval;
    }

    /**
     * Return a map of {@link ClientStats} by connection id. This will
     * roll up {@link ClientStats} instances by procedure name for each
     * connection. Note that connection id is unique, while hostname and
     * port may not be. Hostname and port will be included in the
     * {@link ClientStats} instance data. Each {@link ClientStats}
     * instance will apply to the time period currently covered by the
     * context.
     *
     * @return A map from connection id to {@link ClientStats} instances.
     */
    public Map<Long, ClientStats> getStatsByConnection() {
        return getStatsByConnectionSinceTag(DEFAULT_TAG);
    }

    public Map<Long, ClientStats> getStatsByConnectionSinceTag(Object tagLabel) {
        FullStats baseline = m_taggedBaselines.get(tagLabel);
        if (baseline == null) {
            return null;
        }

        Map<Long, Map<String, ClientStats>> complete = getCompleteStatsSinceTag(tagLabel);
        Map<Long, ClientIOStats> completeIO = diffIO(m_current.ioStats, baseline.ioStats);
        Map<Long, ClientStats> retval = new TreeMap<Long, ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            ClientStats cs = ClientStats.merge(e.getValue().values());
            ClientIOStats cios = completeIO.get(e.getKey());
            if (cios != null) {
                cs.m_bytesReceived = cios.m_bytesReceived;
                cs.m_bytesSent = cios.m_bytesSent;
            }
            retval.put(e.getKey(), cs);
        }
        return retval;
    }

    /**
     * Return a map of maps by connection id. Each sub-map maps procedure
     * names to {@link ClientStats} instances. Note that connection id is
     * unique, while hostname and port may not be. Hostname and port will
     * be included in the {@link ClientStats} instance data. Each
     * {@link ClientStats} instance will apply to the time period currently
     * covered by the context. This is full set of data available from this
     * context instance.
     *
     * @return A map from connection id to {@link ClientStats} instances.
     */
    public Map<Long, Map<String, ClientStats>> getCompleteStats() {
        return getCompleteStatsSinceTag(DEFAULT_TAG);
    }

    public Map<Long, Map<String, ClientStats>> getCompleteStatsSinceTag(Object tagLabel) {
        Map<Long, Map<String, ClientStats>> retval =
                new TreeMap<Long, Map<String, ClientStats>>();

        FullStats baseline = m_taggedBaselines.get(tagLabel);
        if (baseline == null) {
            return null;
        }

        for (Entry<Long, Map<String, ClientStats>> e : m_current.baseStats.entrySet()) {
            if (baseline.baseStats.containsKey(e.getKey())) {
                retval.put(e.getKey(), diff(e.getValue(), baseline.baseStats.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), dup(e.getValue()));
            }
        }

        // reset the timestamp fields to reflect the difference
        for (Entry<Long, Map<String, ClientStats>> e : retval.entrySet()) {
            for (Entry<String, ClientStats> e2 : e.getValue().entrySet()) {
                ClientStats cs = e2.getValue();
                cs.m_startTS = baseline.ts;
                cs.m_endTS = m_current.ts;
                assert(cs.m_startTS != Long.MAX_VALUE);
                assert(cs.m_endTS != Long.MIN_VALUE);
            }
        }

        return retval;
    }

    /**
     * Get the client affinity stats.  Will only be populated if client affinity is enabled.
     *
     * @return A map from an internal partition id to a {@link ClientAffinityStats} instance.
     */
    public Map<Integer, ClientAffinityStats> getAffinityStats() {
        return getAffinityStatsSinceTag(DEFAULT_TAG);
    }

    public Map<Integer, ClientAffinityStats> getAffinityStatsSinceTag(Object tagLabel) {
        Map<Integer, ClientAffinityStats> retval = new TreeMap<Integer, ClientAffinityStats>();

        FullStats baseline = m_taggedBaselines.get(tagLabel);
        if (baseline == null) {
            return null;
        }

        for (Entry<Integer, ClientAffinityStats> e : m_current.affinityStats.entrySet()) {
            if (baseline.affinityStats.containsKey(e.getKey())) {
                retval.put(e.getKey(), ClientAffinityStats.diff(e.getValue(), baseline.affinityStats.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), (ClientAffinityStats) e.getValue().clone());
            }
        }
        return retval;
    }

    /**
     * Roll up the per-partition affinity stats and return the totals for each of the four
     * categories. Will only be populated if client affinity is enabled.
     *
     * @return A {@link ClientAffinityStats} instance covering all partitions.
     */
    public ClientAffinityStats getAggregateAffinityStats()
    {
        return getAggregateAffinityStatsSinceTag(DEFAULT_TAG);
    }

    public ClientAffinityStats getAggregateAffinityStatsSinceTag(Object tagLabel) {
        long afWrites = 0;
        long afReads = 0;
        long rrWrites = 0;
        long rrReads = 0;

        Map<Integer, ClientAffinityStats> affinityStats = getAffinityStatsSinceTag(tagLabel);
        if (affinityStats == null) {
            return null;
        }

        for (Entry<Integer, ClientAffinityStats> e : affinityStats.entrySet()) {
            afWrites += e.getValue().getAffinityWrites();
            afReads += e.getValue().getAffinityReads();
            rrWrites += e.getValue().getRrWrites();
            rrReads += e.getValue().getRrReads();
        }
        ClientAffinityStats retval = new ClientAffinityStats(Integer.MAX_VALUE, afWrites, rrWrites,
               afReads, rrReads);
        return retval;
    }

    /**
     * Return a {@link ClientStats} instance for a specific procedure
     * name. This will be rolled up across all connections. The
     * {@link ClientStats} instance will apply to the time period
     * currently covered by the context.
     *
     * @param procedureName Name of the procedure.
     * @return A {@link ClientStats} instance.
     */
    public ClientStats getStatsForProcedure(String procedureName) {
        return getStatsForProcedureSinceTag(DEFAULT_TAG, procedureName);
    }

    public ClientStats getStatsForProcedureSinceTag(Object tagLabel, String procedureName) {
        Map<Long, Map<String, ClientStats>> complete = getCompleteStatsSinceTag(tagLabel);
        List<ClientStats> statsForProc = new ArrayList<ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            ClientStats procStats = e.getValue().get(procedureName);
            if (procStats != null) {
                statsForProc.add(procStats);
            }
        }
        if (statsForProc.size() == 0) {
            return null;
        }
        return ClientStats.merge(statsForProc);
    }

    Map<Long, ClientIOStats> diffIO(Map<Long, ClientIOStats> newer, Map<Long, ClientIOStats> older) {
        Map<Long, ClientIOStats> retval = new TreeMap<Long, ClientIOStats>();
        if (newer == null) {
            return retval;
        }
        if (older == null) {
            return newer;
        }
        for (Entry<Long, ClientIOStats> e : newer.entrySet()) {
            if (older.containsKey(e.getKey())) {
                retval.put(e.getKey(), ClientIOStats.diff(e.getValue(), older.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), (ClientIOStats) e.getValue().clone());
            }
        }
        return retval;
    }

    Map<String, ClientStats> diff(Map<String, ClientStats> newer, Map<String, ClientStats> older) {
        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<String, ClientStats> e : newer.entrySet()) {
            if (older.containsKey(e.getKey())) {
                retval.put(e.getKey(), ClientStats.diff(e.getValue(), older.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), (ClientStats) e.getValue().clone());
            }
        }
        return retval;
    }

    Map<String, ClientStats> dup(Map<String, ClientStats> x) {
        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<String, ClientStats> e : x.entrySet()) {
            retval.put(e.getKey(), (ClientStats) e.getValue().clone());
        }
        return retval;
    }
}
