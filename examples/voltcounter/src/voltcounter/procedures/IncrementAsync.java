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
import org.voltdb.VoltType;

@ProcInfo(
        partitionInfo = "counters.counter_class_id:0",
        singlePartition = true)
public class IncrementAsync extends VoltProcedure {

    /**
     *
     */
    public final SQLStmt incrStmt = new SQLStmt("UPDATE counters "
            + "SET counter_value = counter_value+1, last_update_time = ? "
            + "WHERE counter_id = ? AND counter_class_id = ?;");

    /**
     *
     */
    public final SQLStmt selectRollupStmt = new SQLStmt("SELECT TOP 1 rollup_time "
            + "FROM v_counter_rollups "
            + "WHERE rollup_id = ? AND counter_class_id = ?"
            + "ORDER BY rollup_time DESC;");
    public final SQLStmt selectCounter = new SQLStmt("SELECT counter_class_id, "
            + "rollup_seconds, counter_value, last_update_time, parent_id "
            + "FROM counters "
            + "WHERE counter_id = ? AND counter_class_id = ?");
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
    public VoltTable run(long counter_class_id, long counter_id) {

        voltQueueSQL(incrStmt, this.getTransactionTime(), counter_id, counter_class_id);
        voltExecuteSQL();
        long parent = updateRollup(counter_class_id, counter_id);
        if ( (parent == counter_id) || (parent == -1) ) {
            return null;
        }
        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("counter_class_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("counter_id", VoltType.BIGINT));
        result.addRow(new Object[]{counter_class_id, parent});
        return result;
    }

    /**
     * Update a specific counter rollup
     * @param counter_class_id
     * @param counter_id
     * @return
     */
    public long updateRollup(long counter_class_id, long counter_id) {
        voltQueueSQL(selectCounter, counter_id, counter_class_id);
        VoltTable result[] = voltExecuteSQL();
        if (result == null || result.length != 1) {
            return -1;
        }
        result[0].advanceRow();
        counter_class_id = result[0].getLong(0);
        long rollup_ttl = result[0].getLong(1);
        long counter_value = result[0].getLong(2);
        long lastupdatetime = result[0].getTimestampAsLong(3);
        long parent = result[0].getLong(4);
        if (rollup_ttl <= 0) {
            return parent;
        }

        String srollup_id = counter_class_id + "-" + counter_id;

        voltQueueSQL(selectRollupStmt, srollup_id, counter_class_id);

        result = voltExecuteSQL();
        int rcnt = result[0].getRowCount();
        if (rcnt == 0) {
            voltQueueSQL(insertRollupStmt, srollup_id, counter_value, this.getTransactionTime(), counter_class_id, counter_id);
            voltExecuteSQL();
            voltQueueSQL(selectRollupStmt, srollup_id, counter_class_id);
            result = voltExecuteSQL();
        }
        result[0].advanceRow();
        long lastrolluptime = result[0].getTimestampAsLong(0);
        long tdiff = lastupdatetime - lastrolluptime;
        if (tdiff != 0) {
            tdiff = tdiff / 1000000;
        }
        if (tdiff >= (rollup_ttl)) {
            voltQueueSQL(insertRollupStmt, srollup_id, counter_value, this.getTransactionTime(), counter_class_id, counter_id);
            voltExecuteSQL();
        }
        return parent;
    }

}
