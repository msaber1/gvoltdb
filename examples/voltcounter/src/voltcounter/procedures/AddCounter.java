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

@ProcInfo(
        partitionInfo = "counters.counter_class_id:0",
        singlePartition = true)
public class AddCounter extends VoltProcedure {

    // Inserts a counter
    public final SQLStmt insertCounter = new SQLStmt("INSERT INTO counters "
            + "(counter_class_id, counter_id, description, rollup_seconds, last_update_time) "
            + "VALUES "
            + "(?, ?, ?, ?, ?);");
    public final SQLStmt insertMapping = new SQLStmt("INSERT INTO counter_maps "
            + "(counter_class_id, counter_id) "
            + "VALUES "
            + "(?, ?);");

    /**
     * Add a new counter and return counter_id if add was successful.
     *
     * @param counter_class
     * @param counter_id
     * @param counter_description
     * @return counter_id or -1 if counter already exists.
     */
    public long run(long counter_class, long counter_id, String counter_description, int rollup_seconds) {

        // add the counter
        voltQueueSQL(insertCounter, EXPECT_SCALAR_MATCH(1), counter_class,
                counter_id, counter_description, rollup_seconds, this.getTransactionTime());
        voltQueueSQL(insertMapping, counter_class, counter_id);

        VoltTable[] result = voltExecuteSQL();
        if (result != null && result.length == 1) {
            return counter_id;
        }
        return counter_id;
    }
}
