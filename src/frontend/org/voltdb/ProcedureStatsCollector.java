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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.IntHistogram;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.EstTime;
import org.voltdb.catalog.Procedure;

/**
 * Derivation of StatsSource to expose timing information of procedure invocations.
 *
 * For each stats being tracked it is stored 3 times. "all" is the counter since the beginning.
 * "last" is cached value for some window of time and used as the return value of stats with poll = true;
 * Just the value (no prefix) is the counter for the current window that will be stored in last when the window
 * rolls.
 */
class ProcedureStatsCollector extends SiteStatsSource {

    private static final VoltLogger log = new VoltLogger("HOST");

    /**
     * Record procedure execution time ever N invocations
     */
    final int timeCollectionInterval = 20;

    /**
     * Number of times this procedure has been invoked.
     */
    private long m_allInvocations = 0;
    private long m_lastInvocations = 0;
    private long m_invocations = 0;

    /**
     * Number of timed invocations
     */
    private long m_allTimedInvocations = 0;
    private long m_lastTimedInvocations = 0;
    private long m_timedInvocations;

    private final long POLL_WINDOW = 10000;
    private long m_lastWindowStart = System.currentTimeMillis();
    //24 hours measured in microseconds
    private final long EXECUTION_HISTOGRAM_HIGHEST_TRACKABLE = (60 * 60 * 1000 * 100);
    private final int  EXECUTION_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS = 1;

    /**
     * Execution time histogram in microseoconds tracking up to 1 second execution time accurately
     */
    private final Histogram m_allExecutionTimeHistogram =
            new Histogram( EXECUTION_HISTOGRAM_HIGHEST_TRACKABLE, EXECUTION_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);
    /**
     * Histogram values returned by poll, replaced periodically
     */
    private IntHistogram m_lastExecutionTimeHistogram =
            new IntHistogram( EXECUTION_HISTOGRAM_HIGHEST_TRACKABLE, EXECUTION_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);

    /**
     * Currently calculated values for window, not visible
     */
    private IntHistogram m_executionTimeHistogram =
            new IntHistogram( EXECUTION_HISTOGRAM_HIGHEST_TRACKABLE, EXECUTION_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);


    /**
     * Total amount of timed execution time
     */
    private long m_allTimedExecutionTime = 0;
    private long m_lastTimedExecutionTime = 0;
    private long m_timedExecutionTime;

    /**
     * Shortest amount of time this procedure has executed in
     */
    private long m_allMinExecutionTime = Long.MAX_VALUE;
    private long m_lastMinExecutionTime = Long.MAX_VALUE;
    private long m_minExecutionTime = Long.MAX_VALUE;

    /**
     * Longest amount of time this procedure has executed in
     */
    private long m_allMaxExecutionTime = Long.MIN_VALUE;
    private long m_lastMaxExecutionTime = Long.MIN_VALUE;
    private long m_maxExecutionTime = Long.MIN_VALUE;

    /**
     * Time the procedure was last started
     */
    private long m_currentStartTime = -1;

    /**
     * Count of the number of aborts (user initiated or DB initiated)
     */
    private long m_allAbortCount = 0;
    private long m_lastAbortCount = 0;
    private long m_abortCount;

    /**
     * Count of the number of errors that occured during procedure execution
     */
    private long m_allFailureCount = 0;
    private long m_lastFailureCount = 0;
    private long m_failureCount;

    private final long SIZE_HISTOGRAM_HIGHEST_TRACKABLE = VoltProtocolHandler.MAX_MESSAGE_SIZE;
    private final int  SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS = 1;

    /**
     * Smallest result size
     */
    private int m_allMinResultSize = Integer.MAX_VALUE;
    private int m_lastMinResultSize = Integer.MAX_VALUE;
    private int m_minResultSize = Integer.MAX_VALUE;

    /**
     * Largest result size
     */
    private int m_allMaxResultSize = Integer.MIN_VALUE;
    private int m_lastMaxResultSize = Integer.MIN_VALUE;
    private int m_maxResultSize = Integer.MIN_VALUE;

    /**
     * Total result size for calculating averages
     */
    private long m_allResultSize = 0;
    private int m_lastResultSize = 0;
    private int m_resultSize = 0;

    /**
     * Execution time histogram in microseoconds tracking up to 1 second execution time accurately
     */
    private Histogram m_allResultSizeHistogram =
            new Histogram( SIZE_HISTOGRAM_HIGHEST_TRACKABLE, SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);
    /**
     * Histogram values returned by poll, replaced periodically
     */
    private IntHistogram m_lastResultSizeHistogram =
            new IntHistogram( SIZE_HISTOGRAM_HIGHEST_TRACKABLE, SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);

    /**
     * Currently calculated values for window, not visible
     */
    private IntHistogram m_resultSizeHistogram =
            new IntHistogram( SIZE_HISTOGRAM_HIGHEST_TRACKABLE, SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);

    /**
     * Smallest parameter set size
     */
    private int m_allMinParameterSetSize = Integer.MAX_VALUE;
    private int m_lastMinParameterSetSize = Integer.MAX_VALUE;
    private int m_minParameterSetSize = Integer.MAX_VALUE;

    /**
     * Largest parameter set size
     */
    private int m_allMaxParameterSetSize = Integer.MIN_VALUE;
    private int m_lastMaxParameterSetSize = Integer.MIN_VALUE;
    private int m_maxParameterSetSize = Integer.MIN_VALUE;

    /**
     * Total parameter set size for calculating averages
     */
    private long m_allParameterSetSize = 0;
    private long m_lastParameterSetSize = 0;
    private long m_parameterSetSize;

    /**
     * Execution time histogram in microseoconds tracking up to 1 second execution time accurately
     */
    private Histogram m_allParameterSetSizeHistogram =
            new Histogram( SIZE_HISTOGRAM_HIGHEST_TRACKABLE, SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);
    /**
     * Histogram values returned by poll, replaced periodically
     */
    private IntHistogram m_lastParameterSetSizeHistogram =
            new IntHistogram( SIZE_HISTOGRAM_HIGHEST_TRACKABLE, SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);

    /**
     * Currently calculated values for window, not visible
     */
    private IntHistogram m_parameterSetSizeHistogram =
            new IntHistogram( SIZE_HISTOGRAM_HIGHEST_TRACKABLE, SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);

    /**
     * Whether to return results in intervals since polling or since the beginning
     */
    private boolean m_interval = false;

    private final Procedure m_catProc;
    private final int m_partitionId;

    /**
     * Constructor requires no args because it has access to the enclosing classes members.
     */
    public ProcedureStatsCollector(long siteId, int partitionId, Procedure catProc) {
        super(siteId, false);
        m_partitionId = partitionId;
        m_catProc = catProc;
    }

    /**
     * Called when a procedure begins executing. Caches the time the procedure starts.
     */
    public final void beginProcedure() {
        if (m_invocations % timeCollectionInterval == 0) {
            m_currentStartTime = System.nanoTime();
        }
    }

    /**
     * Called after a procedure is finished executing. Compares the start and end time and calculates
     * the statistics.
     */
    public final void endProcedure(
            boolean aborted,
            boolean failed,
            VoltTable[] results,
            ParameterSet parameterSet) {
        if (m_currentStartTime > 0) {
            rollWindow();
            // This is a sampled invocation.
            // Update timings and size statistics.
            final long endTime = System.nanoTime();
            final long delta = endTime - m_currentStartTime;
            if (delta < 0)
            {
                if (Math.abs(delta) > 1000000000)
                {
                    log.info("Procedure: " + m_catProc.getTypeName() +
                             " recorded a negative execution time larger than one second: " +
                             delta);
                }
            }
            else
            {
                m_timedExecutionTime += delta;
                final long deltaMicros = delta / 1000;
                if (deltaMicros <= m_allExecutionTimeHistogram.getHighestTrackableValue()) {
                    m_allExecutionTimeHistogram.recordValue(deltaMicros);
                    m_executionTimeHistogram.recordValue(deltaMicros);
                } else {
                    m_allExecutionTimeHistogram.recordValue(m_allExecutionTimeHistogram.getHighestTrackableValue());
                    m_executionTimeHistogram.recordValue(m_executionTimeHistogram.getHighestTrackableValue());
                }
                m_timedInvocations++;

                // sampled timings
                m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);

                // sampled size statistics
                int resultSize = 0;
                if (results != null) {
                    for (VoltTable result : results ) {
                        resultSize += result.getSerializedSize();
                    }
                }

                if (resultSize <= m_allResultSizeHistogram.getHighestTrackableValue()) {
                    m_allResultSizeHistogram.recordValue(resultSize);
                    m_resultSizeHistogram.recordValue(resultSize);
                } else {
                    m_allResultSizeHistogram.recordValue(m_allResultSizeHistogram.getHighestTrackableValue());
                    m_resultSizeHistogram.recordValue(m_resultSizeHistogram.getHighestTrackableValue());
                }

                m_resultSize += resultSize;
                m_minResultSize = Math.min(resultSize, m_minResultSize);
                m_maxResultSize = Math.max(resultSize, m_maxResultSize);

                int parameterSetSize = (
                        parameterSet != null ? parameterSet.getSerializedSize() : 0);
                if (parameterSetSize <= m_allParameterSetSizeHistogram.getHighestTrackableValue()) {
                    m_allParameterSetSizeHistogram.recordValue(parameterSetSize);
                    m_parameterSetSizeHistogram.recordValue(parameterSetSize);
                } else {
                    m_allParameterSetSizeHistogram.recordValue(
                            m_allParameterSetSizeHistogram.getHighestTrackableValue());
                    m_parameterSetSizeHistogram.recordValue(
                            m_parameterSetSizeHistogram.getHighestTrackableValue());
                }
                m_parameterSetSize += parameterSetSize;

                m_minParameterSetSize = Math.min(parameterSetSize, m_minParameterSetSize);
                m_maxParameterSetSize = Math.max(parameterSetSize, m_maxParameterSetSize);
            }
            m_currentStartTime = -1;
        }
        if (aborted) {
            m_abortCount++;
        }
        if (failed) {
            m_failureCount++;
        }
        m_invocations++;
    }

    /*
     * Every window seconds stash away the stats so all pollers see a consistent view
     */
    private void rollWindow() {
        final long now = EstTime.currentTimeMillis();
        //Second clause handles time going backwards
        if (now - m_lastWindowStart  > POLL_WINDOW || now < m_lastWindowStart) {
            m_lastWindowStart = now;

            /*
             * These 3 values are incremented even if no procedure has been profiled
             * since the last window. That is fine, but for accuracy zero
             * them out as well
             */
            m_lastInvocations = m_invocations;
            m_allInvocations += m_invocations;
            m_invocations = 0;

            m_lastAbortCount = m_abortCount;
            m_allAbortCount += m_abortCount;
            m_abortCount = 0;

            m_lastFailureCount = m_failureCount;
            m_allFailureCount += m_allFailureCount;
            m_failureCount = 0;

            /*
             * If nothing has happened in this window and the last one there is nothing to do.
             * They are all zeroes so this is a no op
             */
            if (m_timedInvocations == 0 &&
                    m_lastExecutionTimeHistogram.getHistogramData().getTotalCount() == 0) return;

            m_lastExecutionTimeHistogram = m_executionTimeHistogram;
            m_executionTimeHistogram =
                    new IntHistogram(
                            EXECUTION_HISTOGRAM_HIGHEST_TRACKABLE,
                            EXECUTION_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);
            m_lastResultSizeHistogram = m_resultSizeHistogram;
            m_resultSizeHistogram =
                    new IntHistogram(
                            SIZE_HISTOGRAM_HIGHEST_TRACKABLE,
                            SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);
            m_lastParameterSetSizeHistogram = m_parameterSetSizeHistogram;
            m_parameterSetSizeHistogram =
                    new IntHistogram(
                            SIZE_HISTOGRAM_HIGHEST_TRACKABLE,
                            SIZE_HISTOGRAM_SIGNIFICANT_VALUE_DIGITS);

            m_lastTimedInvocations = m_timedInvocations;
            m_allTimedInvocations += m_timedInvocations;
            m_timedInvocations = 0;

            m_lastTimedExecutionTime = m_timedExecutionTime;
            m_allTimedExecutionTime += m_timedExecutionTime;
            m_timedExecutionTime = 0;

            m_lastMinExecutionTime = m_minExecutionTime;
            m_allMinExecutionTime = Math.min(m_allMinExecutionTime, m_minExecutionTime);
            m_minExecutionTime = Long.MAX_VALUE;

            m_lastMaxExecutionTime = m_maxExecutionTime;
            m_allMaxExecutionTime = Math.max(m_allMaxExecutionTime, m_maxExecutionTime);
            m_maxExecutionTime = Long.MIN_VALUE;



            m_lastMinResultSize = m_minResultSize;
            m_allMinResultSize = Math.min(m_allMinResultSize, m_minResultSize);
            m_minResultSize = Integer.MAX_VALUE;

            m_lastMaxResultSize = m_maxResultSize;
            m_allMaxResultSize = Math.max(m_allMaxResultSize, m_maxResultSize);
            m_maxResultSize = Integer.MIN_VALUE;

            m_lastResultSize = m_resultSize;
            m_allMaxResultSize += m_resultSize;
            m_resultSize = 0;

            m_lastMinParameterSetSize = m_minParameterSetSize;
            m_allMinParameterSetSize = Math.min(m_allMinParameterSetSize, m_minParameterSetSize);
            m_minParameterSetSize = Integer.MAX_VALUE;

            m_lastMaxParameterSetSize = m_maxParameterSetSize;
            m_allMaxParameterSetSize = Math.max(m_allMaxParameterSetSize, m_maxParameterSetSize);
            m_maxParameterSetSize = Integer.MIN_VALUE;

            m_lastParameterSetSize = m_parameterSetSize;
            m_allParameterSetSize += m_parameterSetSize;
            m_parameterSetSize = 0;
        }
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
        rollWindow();
        rowValues[columnNameToIndex.get("PARTITION_ID")] = m_partitionId;
        rowValues[columnNameToIndex.get("PROCEDURE")] = m_catProc.getClassname();
        long invocations = m_allInvocations;
        long totalTimedExecutionTime = m_allTimedExecutionTime + m_timedExecutionTime;
        long timedInvocations = m_allTimedInvocations + m_timedInvocations;
        long minExecutionTime = Math.min(m_allMinExecutionTime, m_minExecutionTime);
        long maxExecutionTime = Math.max(m_allMaxExecutionTime, m_maxExecutionTime);
        long abortCount = m_allAbortCount + m_abortCount;
        long failureCount = m_allFailureCount + m_failureCount;
        int minResultSize = Math.min(m_allMinResultSize, m_minResultSize);
        int maxResultSize = Math.max(m_allMaxResultSize, m_maxResultSize);
        long totalResultSize = m_allResultSize + m_resultSize;
        int minParameterSetSize = Math.min(m_allMinParameterSetSize, m_minParameterSetSize);
        int maxParameterSetSize = Math.max(m_allMaxParameterSetSize, m_maxParameterSetSize);
        long totalParameterSetSize = m_allParameterSetSize + m_parameterSetSize;
        long executionTime99 = 0;
        int resultSize99 = 0;
        int paramSize99 = 0;

        if (m_interval) {
            invocations = m_lastInvocations;

            totalTimedExecutionTime = m_lastTimedExecutionTime;

            timedInvocations = m_lastTimedInvocations;

            abortCount = m_lastAbortCount;

            failureCount = m_lastFailureCount;

            minExecutionTime = m_lastMinExecutionTime;
            maxExecutionTime = m_lastMaxExecutionTime;

            minResultSize = m_lastMinResultSize;
            maxResultSize = m_lastMaxResultSize;

            minParameterSetSize = m_lastMinParameterSetSize;
            maxParameterSetSize = m_lastMaxParameterSetSize;

            totalResultSize = m_lastResultSize;

            totalParameterSetSize = m_lastParameterSetSize;

            if (timedInvocations > 0) {
                executionTime99 = m_lastExecutionTimeHistogram.getHistogramData().getValueAtPercentile(.99) * 1000;
                resultSize99 = ((int)m_lastResultSizeHistogram.getHistogramData().getValueAtPercentile(.99));
                paramSize99 = ((int)m_lastParameterSetSizeHistogram.getHistogramData().getValueAtPercentile(.99));
            }
        } else {
            if (timedInvocations > 0) {
                executionTime99 = m_allExecutionTimeHistogram.getHistogramData().getValueAtPercentile(.99) * 1000;
                resultSize99 = ((int)m_allResultSizeHistogram.getHistogramData().getValueAtPercentile(.99));
                paramSize99 = ((int)m_allParameterSetSizeHistogram.getHistogramData().getValueAtPercentile(.99));
            }
        }

        rowValues[columnNameToIndex.get("INVOCATIONS")] = invocations;
        rowValues[columnNameToIndex.get("TIMED_INVOCATIONS")] = timedInvocations;
        rowValues[columnNameToIndex.get("MIN_EXECUTION_TIME")] = minExecutionTime;
        rowValues[columnNameToIndex.get("MAX_EXECUTION_TIME")] = maxExecutionTime;
        if (timedInvocations != 0) {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] =
                 (totalTimedExecutionTime / timedInvocations);
            rowValues[columnNameToIndex.get("AVG_RESULT_SIZE")] =
                    (int)(totalResultSize / timedInvocations);
            rowValues[columnNameToIndex.get("AVG_PARAMETER_SET_SIZE")] =
                    (int)(totalParameterSetSize / timedInvocations);
        } else {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] = 0L;
            rowValues[columnNameToIndex.get("AVG_RESULT_SIZE")] = 0;
            rowValues[columnNameToIndex.get("AVG_PARAMETER_SET_SIZE")] = 0;
        }
        rowValues[columnNameToIndex.get("ABORTS")] = abortCount;
        rowValues[columnNameToIndex.get("FAILURES")] = failureCount;
        rowValues[columnNameToIndex.get("MIN_RESULT_SIZE")] = minResultSize;
        rowValues[columnNameToIndex.get("MAX_RESULT_SIZE")] = maxResultSize;
        rowValues[columnNameToIndex.get("MIN_PARAMETER_SET_SIZE")] = minParameterSetSize;
        rowValues[columnNameToIndex.get("MAX_PARAMETER_SET_SIZE")] = maxParameterSetSize;
        rowValues[columnNameToIndex.get("EXECUTION_TIME_99")] = executionTime99;
        rowValues[columnNameToIndex.get("PARAMETER_SET_SIZE_99")] = resultSize99;
        rowValues[columnNameToIndex.get("RESULT_SIZE_99")] = paramSize99;
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("PARTITION_ID", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("ABORTS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("FAILURES", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("EXECUTION_TIME_99", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("PARAMETER_SET_SIZE_99", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("RESULT_SIZE_99", VoltType.INTEGER));
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
                else if (getLastInvocations() == 0) {
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

    @Override
    public String toString() {
        return m_catProc.getTypeName();
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
        return m_allInvocations + m_invocations;
    }

    /**
     * Accessor
     * @return the m_lastInvocations
     */
    public long getLastInvocations() {
        return m_lastInvocations;
    }
}