/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.matviewprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    singlePartition = false
)
public class TruncateMatViewDataMP extends VoltProcedure {
    public final SQLStmt truncatebase8 = new SQLStmt("TRUNCATE TABLE CUSTOMERS;");
    public final SQLStmt truncatebase9 = new SQLStmt("TRUNCATE TABLE ORDERS;");
    public final SQLStmt truncatebase10 = new SQLStmt("TRUNCATE TABLE ORDERITEMS;");
    public final SQLStmt truncatebase11 = new SQLStmt("TRUNCATE TABLE PRODUCTS;");

    public VoltTable[] run() {
        VoltTable[] result;
        voltQueueSQL(truncatebase8); // ("TRUNCATE TABLE CUSTOMERS;");
        voltQueueSQL(truncatebase9); // ("TRUNCATE TABLE ORDERS;");
        voltQueueSQL(truncatebase10); // ("TRUNCATE TABLE ORDERITEMS;");
        voltQueueSQL(truncatebase11); // ("TRUNCATE TABLE PRODUCTS;");
        result = voltExecuteSQL();
        return result;
    }
}
