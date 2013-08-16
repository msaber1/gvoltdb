/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;

/**
 * Collects global cache use stats
 */
public class RunningProcedureStatsCollector extends StatsSource {

    private static final VoltLogger log = new VoltLogger("HOST");

    /**
     * Whether to return results in intervals since polling or since the beginning
     */
    private boolean m_interval = false;

    /**
     * Record procedure execution time ever N invocations
     */
    final int timeCollectionInterval = 1;

    /**
     * Number of times this procedure has been invoked.
     */
    private long m_invocations = 0;
    private long m_lastInvocations = 0;

    private long m_uniqueId = -1;
    private String m_procName = "";
    private float m_duration = -1;
    private short m_batchIndex = -1;
    private short m_indexVoltQueue = -1;
    private String m_planNodeName = "";
    private String m_lastTargetTable = "";
    private long m_lastTableSize = -1;
    private long m_tuplesFound = -1;

    /**
     * Time the procedure was last started
     */
    private long m_currentStartTime = -1;

    /**
     * Site ID
     */
    final long m_siteId;

    /**
     * Constructor
     *
     * @param siteId  site id
     */
    public RunningProcedureStatsCollector(long siteId) {
        super(false);
        m_siteId = siteId;
    }

    /**
     * Called when a procedure begins executing. Caches the time the procedure starts.
     */
    public final void beginProcedure(long uniqueId,
            String procName,
            float duration,
            short batchIndex,
            short indexVoltQueue,
            String planNodeName,
            String lastAccessedTable,
            long lastAccessedTableSize,
            long tuplesFound
            ) {
        m_uniqueId = uniqueId;
        m_duration = duration;
        m_procName = procName;
        m_batchIndex = batchIndex;
        m_indexVoltQueue = indexVoltQueue;
        m_planNodeName = planNodeName;
        m_lastTargetTable = lastAccessedTable;
        m_lastTableSize = lastAccessedTableSize;
        m_tuplesFound = tuplesFound;

        m_invocations++;
    }

    /**
     * Called after a procedure is finished executing. Compares the start and end time and calculates
     * the statistics.
     */
    public final void endProcedure(
            ) {
//        m_currentUniqueId = 0;
//        m_currentProcName = "";
    }

    /**
     * Update the rowValues array with the latest statistical information.
     * This method is overrides the super class version
     * which must also be called so that it can update its columns.
     * @param values Values of each column of the row of stats. Used as output.
     */
    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        super.updateStatsRow(rowKey, rowValues);

        rowValues[columnNameToIndex.get(VoltSystemProcedure.CNAME_SITE_ID)] = CoreUtils.getSiteIdFromHSId(m_siteId);
        rowValues[columnNameToIndex.get("UNIQUE_ID")] = m_uniqueId;
        rowValues[columnNameToIndex.get("PROCEDURE_NAME")] = m_procName;
        rowValues[columnNameToIndex.get("DURATION")] = m_duration;
        rowValues[columnNameToIndex.get("EXECUTESQLINDEX")] = m_indexVoltQueue;
        rowValues[columnNameToIndex.get("BATCHINDEX")] = m_batchIndex;
        rowValues[columnNameToIndex.get("FRAGMENT_TYPE")] = m_planNodeName;
        rowValues[columnNameToIndex.get("TABLE")] = m_lastTargetTable;
        rowValues[columnNameToIndex.get("TABLE_SIZE")] = m_lastTableSize;
        rowValues[columnNameToIndex.get("TUPLES_ACCESSED")] = m_tuplesFound;
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID));
        columns.add(new ColumnInfo("UNIQUE_ID",  VoltType.BIGINT));
        columns.add(new ColumnInfo("PROCEDURE_NAME",  VoltType.STRING));
        columns.add(new ColumnInfo("DURATION",  VoltType.BIGINT));
        columns.add(new ColumnInfo("EXECUTESQLINDEX",  VoltType.SMALLINT));
        columns.add(new ColumnInfo("BATCHINDEX",  VoltType.SMALLINT));
        columns.add(new ColumnInfo("FRAGMENT_TYPE",  VoltType.STRING));
        columns.add(new ColumnInfo("TABLE",  VoltType.STRING));
        columns.add(new ColumnInfo("TABLE_SIZE",  VoltType.BIGINT));
        columns.add(new ColumnInfo("TUPLES_ACCESSED",  VoltType.BIGINT));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_interval = interval;
        return new Iterator<Object>() {
            boolean givenNext = false;
            @Override
            public boolean hasNext() {
                if (!getInterval()) {
                    if (getInvocations() == 0) {
                        return false;
                    }
                }
                else if (getInvocations() - getLastInvocations() == 0) {
                    return false;
                }
                return !givenNext;
            }

            @Override
            public Object next() {
                if (!givenNext) {
                    givenNext = true;
                    return new Object();
                }
                return null;
            }

            @Override
            public void remove() {}

        };
    }

    /**
     * Accessor
     * @return the m_interval
     */
    public boolean getInterval() {
        return m_interval;
    }

    /**
     * Accessor
     * @return the m_invocations
     */
    public long getInvocations() {
        return m_invocations;
    }

    /**
     * Accessor
     * @return the m_lastInvocations
     */
    public long getLastInvocations() {
        return m_lastInvocations;
    }
}
