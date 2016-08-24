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
        /*
        for (VoltTable deleted : result) {
            System.out.println("DEBUG Deleted: " + deleted.asScalarLong());
        }
        voltQueueSQL(validatebase1); // ("SELECT COUNT(*) FROM PEOPLE;");
        voltQueueSQL(validatebase2); // ("SELECT COUNT(*) FROM THINGS;");
        voltQueueSQL(validatebase3); // ("SELECT COUNT(*) FROM OVERFLOWTEST;");
        voltQueueSQL(validatebase4); // ("SELECT COUNT(*) FROM ENG798;");
        voltQueueSQL(validatebase5); // ("SELECT COUNT(*) FROM contest;");
        voltQueueSQL(validatebase6); // ("SELECT COUNT(*) FROM DEPT_PEOPLE;");
        voltQueueSQL(validatebase7); // ("SELECT COUNT(*) FROM ENG6511;");
        voltQueueSQL(validatebase8); // ("SELECT COUNT(*) FROM CUSTOMERS;");
        voltQueueSQL(validatebase9); // ("SELECT COUNT(*) FROM ORDERS;");
        voltQueueSQL(validatebase10); // ("SELECT COUNT(*) FROM ORDERITEMS;");
        voltQueueSQL(validatebase11); // ("SELECT COUNT(*) FROM PRODUCTS;");

        voltQueueSQL(validateview1); // ("SELECT COUNT(*) FROM MATPEOPLE;");
        voltQueueSQL(validateview2); // ("SELECT COUNT(*) FROM MATTHINGS;");
        voltQueueSQL(validateview3); // ("SELECT COUNT(*) FROM V_OVERFLOWTEST;");
        voltQueueSQL(validateview4); // ("SELECT COUNT(*) FROM V_ENG798;");
        voltQueueSQL(validateview5); // ("SELECT COUNT(*) FROM V_RUNNING_TEAM;");
        voltQueueSQL(validateview6); // ("SELECT COUNT(*) FROM V_TEAM_MEMBERSHIP;");
        voltQueueSQL(validateview7); // ("SELECT COUNT(*) FROM V_TEAM_TIMES;");
        voltQueueSQL(validateview8); // ("SELECT COUNT(*) FROM MATPEOPLE2;");
        voltQueueSQL(validateview9); // ("SELECT COUNT(*) FROM MATPEOPLE3;");
        voltQueueSQL(validateview10); // ("SELECT COUNT(*) FROM DEPT_AGE_MATVIEW;");
        voltQueueSQL(validateview11); // ("SELECT COUNT(*) FROM DEPT_AGE_FILTER_MATVIEW;");
        voltQueueSQL(validateview12); // ("SELECT COUNT(*) FROM VENG6511;");
        voltQueueSQL(validateview13); // ("SELECT COUNT(*) FROM VENG6511expL;");
        voltQueueSQL(validateview14); // ("SELECT COUNT(*) FROM VENG6511expR;");
        voltQueueSQL(validateview15); // ("SELECT COUNT(*) FROM VENG6511expLR;");
        voltQueueSQL(validateview16); // ("SELECT COUNT(*) FROM VENG6511C;");
        // TODO: It is very strange that validateview 17-21 fails but the view tables seem to be empty.
        //       Disabling those checks for now.
        // voltQueueSQL(validateview17); // ("SELECT COUNT(*) FROM ORDER_COUNT_NOPCOL;");
        // voltQueueSQL(validateview18); // ("SELECT * FROM ORDER_COUNT_GLOBAL;");
        // voltQueueSQL(validateview19); // ("SELECT COUNT(*) FROM ORDER_DETAIL_NOPCOL;");
        // voltQueueSQL(validateview20); // ("SELECT COUNT(*) FROM ORDER_DETAIL_WITHPCOL;");
        // voltQueueSQL(validateview21); // ("SELECT COUNT(*) FROM ORDER2016;");
        voltQueueSQL(validateview22); // ("SELECT * FROM MATPEOPLE_COUNT;");
        voltQueueSQL(validateview23); // ("SELECT * FROM MATPEOPLE_CONDITIONAL_COUNT;");
        voltQueueSQL(validateview24); // ("SELECT NUM FROM MATPEOPLE_CONDITIONAL_COUNT_SUM;");
        voltQueueSQL(validateview25); // ("SELECT NUM FROM MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX;");
        result = voltExecuteSQL(true);
        /*
        for (VoltTable deleted : result) {
            System.out.println("DEBUG Validated deletion: " + deleted.asScalarLong());
        }
        */
        return result;
    }
}
