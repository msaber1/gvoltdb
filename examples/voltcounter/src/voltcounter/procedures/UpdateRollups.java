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

@ProcInfo (
    partitionInfo = "counter_rollups.rollup_id:0",
    singlePartition = true
)
public class UpdateRollups extends VoltProcedure
{
    // Inserts a counter
    public final SQLStmt selectRollupStmt = new SQLStmt("SELECT TOP 1 rollup_time from counter_rollups where rollup_id = ? ORDER BY rollup_time DESC;");
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO counter_rollups "
            + "(rollup_id, rollup_value, rollup_time) "
            + "VALUES "
            + "(?, ?, ?);");

    public long run(String srollup_id, long rollup_ttl, long counter_value, long lastupdatetime) {

        // get rollup values
        voltQueueSQL(selectRollupStmt, srollup_id);
        
        VoltTable tbl[] = voltExecuteSQL();
        int rcnt = tbl[0].getRowCount();
        if (rcnt == 0) {
            voltQueueSQL(insertStmt, srollup_id, counter_value, this.getTransactionTime()); 
            voltExecuteSQL();
            voltQueueSQL(selectRollupStmt, srollup_id);
            tbl = voltExecuteSQL();
        }
        tbl[0].advanceRow();
        long lastrolluptime = tbl[0].getTimestampAsLong(0);
        long tdiff = lastupdatetime - lastrolluptime;
        if (tdiff != 0) {
            tdiff = (tdiff/1000)/1000;
        }
        if (tdiff  > (rollup_ttl)) {
            voltQueueSQL(insertStmt, srollup_id, counter_value, this.getTransactionTime());                        
            voltExecuteSQL();
            return 0L;
        }
        return 1L;
    }
}
