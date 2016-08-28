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
import org.voltdb.VoltType;

@ProcInfo (
    singlePartition = false
)
public class TruncateTables extends VoltProcedure {

    public final SQLStmt captureview1 = new SQLStmt("SELECT * FROM ORDER_COUNT_GLOBAL;");

    public final SQLStmt clearcache1 = new SQLStmt("TRUNCATE TABLE WAS_CUSTOMERS;");
    public final SQLStmt clearcache2 = new SQLStmt("TRUNCATE TABLE WAS_ORDERS;");
    public final SQLStmt clearcache3 = new SQLStmt("TRUNCATE TABLE WAS_ORDERITEMS;");
    public final SQLStmt clearcache4 = new SQLStmt("TRUNCATE TABLE WAS_PRODUCTS;");

    public final SQLStmt cachebase1 = new SQLStmt("INSERT INTO WAS_CUSTOMERS SELECT * FROM CUSTOMERS ORDER BY CUSTOMER_ID;");
    public final SQLStmt cachebase2 = new SQLStmt("INSERT INTO WAS_ORDERS SELECT * FROM ORDERS ORDER BY ORDER_ID;");
    public final SQLStmt cachebase3 = new SQLStmt("INSERT INTO WAS_ORDERITEMS SELECT * FROM ORDERITEMS ORDER BY ORDER_ID, PID;");
    public final SQLStmt cachebase4 = new SQLStmt("INSERT INTO WAS_PRODUCTS SELECT * FROM PRODUCTS ORDER BY PID;");

    public final SQLStmt truncatebase1 = new SQLStmt("TRUNCATE TABLE CUSTOMERS;");
    public final SQLStmt truncatebase2 = new SQLStmt("TRUNCATE TABLE ORDERS;");
    public final SQLStmt truncatebase3 = new SQLStmt("TRUNCATE TABLE ORDERITEMS;");
    public final SQLStmt truncatebase4 = new SQLStmt("TRUNCATE TABLE PRODUCTS;");

    public final SQLStmt validatebase1 = new SQLStmt("SELECT COUNT(*) FROM CUSTOMERS;");
    public final SQLStmt validatebase2 = new SQLStmt("SELECT COUNT(*) FROM ORDERS;");
    public final SQLStmt validatebase3 = new SQLStmt("SELECT COUNT(*) FROM ORDERITEMS;");
    public final SQLStmt validatebase4 = new SQLStmt("SELECT COUNT(*) FROM PRODUCTS;");

    public final SQLStmt validateview1 = new SQLStmt("SELECT * FROM ORDER_COUNT_GLOBAL;");

    public final SQLStmt renewbase1 = new SQLStmt("INSERT INTO CUSTOMERS SELECT * FROM WAS_CUSTOMERS ORDER BY CUSTOMER_ID;");
    public final SQLStmt renewbase2 = new SQLStmt("INSERT INTO ORDERS SELECT * FROM WAS_ORDERS ORDER BY ORDER_ID;");
    public final SQLStmt renewbase3 = new SQLStmt("INSERT INTO ORDERITEMS SELECT * FROM WAS_ORDERITEMS ORDER BY ORDER_ID, PID;");
    public final SQLStmt renewbase4 = new SQLStmt("INSERT INTO PRODUCTS SELECT * FROM WAS_PRODUCTS ORDER BY PID;");

    public VoltTable run(int rollback,
            int truncateTable1,
            int truncateTable2,
            int truncateTable3,
            int truncateTable4) {
        boolean atLeastOneTruncation = false;

        voltQueueSQL(captureview1);
        VoltTable beforeView1 = voltExecuteSQL()[0];

        if (truncateTable1 != 0) {
            atLeastOneTruncation = true;
            voltQueueSQL(clearcache1);
            voltQueueSQL(cachebase1);
            voltQueueSQL(truncatebase1); // ("TRUNCATE TABLE CUSTOMERS;");
        }
        if (truncateTable2 != 0) {
            atLeastOneTruncation = true;
            voltQueueSQL(clearcache2);
            voltQueueSQL(cachebase2);
            voltQueueSQL(truncatebase2); // ("TRUNCATE TABLE ORDERS;");
        }
        if (truncateTable3 != 0) {
            atLeastOneTruncation = true;
            voltQueueSQL(clearcache3);
            voltQueueSQL(cachebase3);
            voltQueueSQL(truncatebase3); // ("TRUNCATE TABLE ORDERITEMS;");
        }
        if (truncateTable4 != 0) {
            atLeastOneTruncation = true;
            voltQueueSQL(clearcache4);
            voltQueueSQL(cachebase4);
            voltQueueSQL(truncatebase4); // ("TRUNCATE TABLE PRODUCTS;");
        }

        if (! atLeastOneTruncation) {
            if (rollback != 0) {
                throw new VoltAbortException("Rolling back as requested.");
            }
            // There's nothing to do. The queue is empty.
            return wrapResult(""); // success
        }

        queueTruncationChecks(
                truncateTable1,
                truncateTable2,
                truncateTable3,
                truncateTable4);

        VoltTable[] results = voltExecuteSQL();

        VoltTable deleted = results[results.length-1];
        if (deleted.asScalarLong() != 0) {
            System.out.println(
                    "DEBUG Truncate failed to delete all view rows, leaving: " +
                            deleted.asScalarLong());
        }

        renewBases(
                truncateTable1,
                truncateTable2,
                truncateTable3,
                truncateTable4);

        voltQueueSQL(captureview1);
        VoltTable afterView1 = voltExecuteSQL()[0];

        String diff = compareTables(beforeView1, afterView1);

        if (rollback != 0) {
            throw new VoltAbortException("Rolling back as requested.");
        }

        VoltTable result = wrapResult(diff);
        return result;
    }

    private VoltTable wrapResult(String diff) {
        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("DIFF", VoltType.STRING));
        result.addRow(diff);
        return result;
    }

    private void queueTruncationChecks(
                int truncateTable1,
                int truncateTable2,
                int truncateTable3,
                int truncateTable4) {
        if (truncateTable1 != 0) {
            voltQueueSQL(validatebase1); // ("SELECT COUNT(*) FROM CUSTOMERS;");
        }
        if (truncateTable2 != 0) {
            voltQueueSQL(validatebase2); // ("SELECT COUNT(*) FROM ORDERS;");
        }
        if (truncateTable3 != 0) {
            voltQueueSQL(validatebase3); // ("SELECT COUNT(*) FROM ORDERITEMS;");
        }
        if (truncateTable4 != 0) {
            voltQueueSQL(validatebase4); // ("SELECT COUNT(*) FROM PRODUCTS;");
        }

        voltQueueSQL(validateview1);
    }

    private VoltTable renewBases(
            int truncateTable1,
            int truncateTable2,
            int truncateTable3,
            int truncateTable4) {
        if (truncateTable1 != 0) {
            voltQueueSQL(renewbase1); // ("INSERT INTO CUSTOMERS;");
        }
        if (truncateTable2 != 0) {
            voltQueueSQL(renewbase2); // ("INSERT INTO ORDERS;");
        }
        if (truncateTable3 != 0) {
            voltQueueSQL(renewbase3); // ("INSERT INTO ORDERITEMS;");
        }
        if (truncateTable4 != 0) {
            voltQueueSQL(renewbase4); // ("INSERT INTO PRODUCTS;");
        }

        voltQueueSQL(validateview1);
        VoltTable[] results = voltExecuteSQL();
        return results[results.length-1];
    }

    private String compareTables(VoltTable beforeView1, VoltTable afterView1) {
        // TODO Auto-generated method stub
        return "";
    }

}
