/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
//
// Get Counter Value provided a counter_id
//
package voltcounter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

/**
 *
 * @author akhanzode
 */
@ProcInfo(
        partitionInfo = "counter_rollups.rollup_id:0",
        singlePartition = true)
public class GetCounterStdDev extends VoltProcedure {
    // Inserts a counter

    /**
     *
     */
    public final SQLStmt selectAvgStmt = new SQLStmt("SELECT AVG(rollup_value) from counter_rollups where rollup_id = ? ORDER BY rollup_value DESC;");
    /**
     *
     */
    public final SQLStmt selectStmt = new SQLStmt("SELECT rollup_value from counter_rollups where rollup_id = ? ORDER BY rollup_value DESC;");

    /**
     *
     * @param srollup_id
     * @return
     */
    public VoltTable run(String srollup_id) {

        // get rollup values
        voltQueueSQL(selectAvgStmt, srollup_id);
        voltQueueSQL(selectStmt, srollup_id);
        VoltTable retresult = new VoltTable(
                new VoltTable.ColumnInfo("std-dev", VoltType.BIGINT));


        VoltTable results[] = voltExecuteSQL();
        if (results[0].getRowCount() != 1) {
            retresult.addRow(new Object[]{0});
            return retresult;
        }
        results[0].advanceRow();
        double avg = results[0].getLong(0);
        if (results[1].getRowCount() <= 0) {
            retresult.addRow(new Object[]{0});
            return retresult;
        }
        VoltTable result = results[1];
        double sqtotal = 0.0;
        for (int i = 0; i < result.getRowCount(); i++) {
            if (result.advanceRow()) {
                double val = (double) result.getLong(0);
                double sqval = Math.pow((val - avg), 2);
                sqtotal += sqval;
            }
        }
        double stddev = Math.sqrt(sqtotal / result.getRowCount());

        retresult.addRow(new Object[]{stddev});
        return retresult;
    }
}
