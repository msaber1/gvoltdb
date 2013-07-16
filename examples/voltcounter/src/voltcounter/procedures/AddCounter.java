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
// Add New Counter
//
package voltcounter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import static org.voltdb.VoltProcedure.EXPECT_SCALAR_MATCH;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo(
        partitionInfo = "counters.counter_class_id:0",
        singlePartition = true)
public class AddCounter extends VoltProcedure {

    /**
     * Inserts a counter
     */
    public final SQLStmt insertCounter = new SQLStmt("INSERT INTO counters "
            + "(counter_class_id, counter_id, description, rollup_seconds, last_update_time, parent_id) "
            + "VALUES "
            + "(?, ?, ?, ?, ?, ?);");
    public final SQLStmt findParent = new SQLStmt("SELECT counter_class_id,parent_id,counter_id FROM counter_map "
            + "WHERE counter_id = ? AND counter_class_id = ?"
            + "ORDER BY counter_class_id, counter_id, parent_id");

    public final SQLStmt insertCounterMap = new SQLStmt("INSERT INTO counter_map "
            + "(counter_class_id, counter_id, parent_id, map_id) "
            + "VALUES "
            + "(?, ?, ?, ?);");

    /**
     * Add a new counter and return counter_id if add was successful.
     *
     * @param counter_class
     * @param counter_id
     * @param counter_description
     * @param rollup_seconds
     * @param parent
     * @return counter_id
     */
    public long run(long counter_class_id, long counter_id, String counter_description, int rollup_seconds, long parent) {

        // add the counter itself
        voltQueueSQL(insertCounter, EXPECT_SCALAR_MATCH(1), counter_class_id,
                counter_id, counter_description, rollup_seconds, this.getTransactionTime(), parent);

        VoltTable[] result = voltExecuteSQL();
        // Root nodes have -1 parent
        if (result != null && result.length == 1 && parent != -1) {
            voltQueueSQL(findParent, parent, counter_class_id );
            result = voltExecuteSQL();
            // For each ancestors add me-ancestor mapping
            for (int i = 0; i < result.length; i++) {
                VoltTable val = result[i];
                for (int j = 0; j < val.getRowCount(); j++) {
                    VoltTableRow row = val.fetchRow(j);
                    long found_parent = row.getLong(1);
                    String map_id = found_parent + "-" + counter_id;
                    voltQueueSQL(insertCounterMap, counter_class_id, counter_id, found_parent, map_id);
                }
            }
            // Add me-parent mapping
            String map_id = parent + "-" + counter_id;
            voltQueueSQL(insertCounterMap, counter_class_id, counter_id, parent, map_id);
            voltExecuteSQL(true);
            return counter_id;
        }
        return -1;
    }
}
