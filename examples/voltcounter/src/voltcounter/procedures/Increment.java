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
// Increment a counter value this will also increment any parent values.
// Additionally based on rollup_seconds given rollups are updated for given counter and all ancestors.
//
package voltcounter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo(
        partitionInfo = "counters.counter_class_id:0",
        singlePartition = true)
public class Increment extends VoltProcedure {

    /**
     *
     */
    public final SQLStmt selectStmt = new SQLStmt("SELECT c.parent_id "
            + "FROM counter_map c "
            + "WHERE c.counter_id = ? "
            + "ORDER BY c.parent_id;");
    /**
     *
     */
    public final SQLStmt incrStmt = new SQLStmt("UPDATE counters "
            + "SET counter_value = counter_value+1, last_update_time = ? "
            + "WHERE counter_id = ?;");

    /**
     *
     */
    public final SQLStmt selectRollupStmt = new SQLStmt("SELECT TOP 1 rollup_time "
            + "FROM counter_rollups "
            + "WHERE rollup_id = ? "
            + "ORDER BY rollup_time DESC;");
    public final SQLStmt selectCounter = new SQLStmt("SELECT counter_class_id, "
            + "rollup_seconds, counter_value, last_update_time, parent_id "
            + "FROM counters "
            + "WHERE counter_id = ? ");
    /**
     *
     */
    public final SQLStmt insertRollupStmt = new SQLStmt("INSERT INTO counter_rollups "
            + "(rollup_id, rollup_value, rollup_time, counter_class_id, counter_id) "
            + "VALUES "
            + "(?, ?, ?, ?, ?);");

    /**
     *
     * @param counter_class
     * @param counter_id
     * @return number of counters incremented.
     */
    public long run(long counter_class_id, long counter_id) {

        long incCount = 0;
        voltQueueSQL(incrStmt, this.getTransactionTime(), counter_id);
        voltExecuteSQL();
        incCount++;
        updateRollup(counter_class_id, counter_id);

        voltQueueSQL(selectStmt, counter_id );
        VoltTable ret[] = voltExecuteSQL();
        for (int i = 0; i < ret.length; i++) {
            VoltTable val = ret[i];
            for (int j = 0; j < val.getRowCount(); j++) {
                VoltTableRow row = val.fetchRow(j);
                long found_parent = row.getLong(0);
                voltQueueSQL(incrStmt, this.getTransactionTime(), found_parent);
                incCount++;
            }
            voltExecuteSQL();
            for (int j = 0; j < val.getRowCount(); j++) {
                VoltTableRow row = val.fetchRow(j);
                long found_parent = row.getLong(0);
                updateRollup(counter_class_id, found_parent);
            }
        }
        return incCount;
    }

    /**
     * Update a specific counter rollup
     * @param counter_class_id
     * @param counter_id
     * @return
     */
    public long updateRollup(long counter_class_id, long counter_id) {
        voltQueueSQL(selectCounter, counter_id);
        VoltTable result[] = voltExecuteSQL();
        if (result == null || result.length != 1) {
            return 1L;
        }
        result[0].advanceRow();
        counter_class_id = result[0].getLong(0);
        long rollup_ttl = result[0].getLong(1);
        long counter_value = result[0].getLong(2);
        long lastupdatetime = result[0].getTimestampAsLong(3);
        long parent = result[0].getLong(4);

        String srollup_id = counter_class_id + "-" + counter_id;

        voltQueueSQL(selectRollupStmt, srollup_id);

        result = voltExecuteSQL();
        int rcnt = result[0].getRowCount();
        if (rcnt == 0) {
            voltQueueSQL(insertRollupStmt, srollup_id, counter_value, this.getTransactionTime(), counter_class_id, counter_id);
            voltExecuteSQL();
            voltQueueSQL(selectRollupStmt, srollup_id);
            result = voltExecuteSQL();
        }
        result[0].advanceRow();
        long lastrolluptime = result[0].getTimestampAsLong(0);
        long tdiff = lastupdatetime - lastrolluptime;
        if (tdiff != 0) {
            tdiff = (tdiff / 1000) / 1000;
        }
        if (tdiff >= (rollup_ttl)) {
            voltQueueSQL(insertRollupStmt, srollup_id, counter_value, this.getTransactionTime(), counter_class_id, counter_id);
            voltExecuteSQL();
        }
        return 0L;
    }

}
