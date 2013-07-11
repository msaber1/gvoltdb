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
package voltcounter.procedures;

import org.voltdb.*;

@ProcInfo
(
  singlePartition = false
)
public class CleanCounters extends VoltProcedure
{
    // Delete everything
    /**
     *
     */
    public final SQLStmt cleanStmt = new SQLStmt("DELETE FROM counters;");
    /**
     *
     */
    public final SQLStmt cleanStmtClass = new SQLStmt("DELETE FROM counter_class;");
    /**
     *
     */
    public final SQLStmt cleanRollups = new SQLStmt("DELETE FROM counter_rollups;");
    /**
     *
     */
    public final SQLStmt cleanMaps = new SQLStmt("DELETE FROM counter_map;");

    /**
     *
     * @return
     */
    public VoltTable[] run()
    {
        voltQueueSQL(cleanRollups);
        voltQueueSQL(cleanMaps);
        voltQueueSQL(cleanStmt);
        voltQueueSQL(cleanStmtClass);
        return voltExecuteSQL(true);
    }
}
