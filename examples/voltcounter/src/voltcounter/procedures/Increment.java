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
// Increment a counter value and return
//

package voltcounter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo (
    partitionInfo = "counters.counter_class_id:0",
    singlePartition = true
)
public class Increment extends VoltProcedure
{
    public final SQLStmt selectStmt = new SQLStmt("SELECT c.counter_id "
            + "FROM counters c where c.counter_class_id = ? "
            + "ORDER BY c.counter_id;");
    public final SQLStmt incrStmt = new SQLStmt("UPDATE counters "
            + "SET counter_value = counter_value+1, last_update_time = ? "
            + "WHERE counter_id = ?;");

    public long run(long counter_class) {

        voltQueueSQL(selectStmt, counter_class);
        VoltTable ret[] = voltExecuteSQL();
        for (int i = 0; i < ret.length;i++) {
            VoltTable val = ret[i];
            for (int j = 0; j < val.getRowCount(); j++) {
                VoltTableRow row = val.fetchRow(j);                
                long newval = row.getLong(0);
                voltQueueSQL(incrStmt, this.getTransactionTime(), newval);
                voltExecuteSQL();
            }
        }
        return 0;
    }
}
