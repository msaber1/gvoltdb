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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AddPerson;
import org.voltdb_testprocs.regressionsuites.matviewprocs.TruncateMatViewDataMP;
import org.voltdb_testprocs.regressionsuites.matviewprocs.TruncateTables;

import com.google_voltpatches.common.collect.Lists;

public class TestJoinedMatViewSuite extends RegressionSuite {
    private static final int[] yesAndNo = new int[]{1, 0};
    private static final int[] never = new int[]{0};

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        TruncateMatViewDataMP.class,
        TruncateTables.class
    };

    // For comparing tables with FLOAT columns
    private static final double EPSILON = 0.000001;

    public TestJoinedMatViewSuite(String name) {
        super(name);
    }

    private void truncateBeforeTest(Client client) {
        // TODO Auto-generated method stub
        VoltTable[] results = null;
        try {
            results = client.callProcedure("TruncateMatViewDataMP").getResults();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
            fail("Unexpected:" + e);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Unexpected:" + e);
        } catch (ProcCallException e) {
            e.printStackTrace();
            fail("Unexpected:" + e);
        }
        int nStatement = 0;
        for (VoltTable countTable : results) {
            try {
                long count = countTable.asScalarLong();
                assertEquals("COUNT statement " + nStatement + "/" +
                results.length + " should have found no undeleted rows.", 0, count);
            }
            catch (Exception exc) {
                System.out.println("validation query " + nStatement + " got a bad result: " + exc);
                throw exc;
            }
            ++nStatement;
        }
    }

    private void insertRow(Client client, Object... parameters) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        results = client.callProcedure(parameters[0].toString() + ".insert", Arrays.copyOfRange(parameters, 1, parameters.length)).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
    }

    private void updateRow(Client client, Object[] oldRow, Object[] newRow) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        String tableName1 = oldRow[0].toString();
        String tableName2 = newRow[0].toString();
        assertEquals("Trying to update table " + tableName1 + " with " + tableName2 + " data.", tableName1, tableName2);
        results = client.callProcedure("UPDATE" + tableName1, newRow[2], newRow[3],
                                                              oldRow[1], oldRow[2], oldRow[3]).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
    }

    private void verifyViewOnJoinQueryResult(Client client) throws IOException, ProcCallException
    {
        VoltTable vresult = null;
        VoltTable tresult = null;
        String prefix = "Assertion failed comparing the view content and the AdHoc query result ";

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_COUNT_NOPCOL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_COUNT_NOPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_COUNT_NOPCOL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_COUNT_GLOBAL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_COUNT_GLOBAL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_COUNT_GLOBAL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_DETAIL_NOPCOL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_DETAIL_NOPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_DETAIL_NOPCOL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_DETAIL_WITHPCOL ORDER BY 1, 2;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_DETAIL_WITHPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_DETAIL_WITHPCOL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER2016 ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER2016").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER2016: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM QTYPERPRODUCT ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_QTYPERPRODUCT").getResults()[0];
        assertTablesAreEqual(prefix + "QTYPERPRODUCT: ", tresult, vresult, EPSILON);
    }

    public void testViewOnJoinQuery() throws IOException, ProcCallException
    {
        Client client = getClient();
        ArrayList<Object[]> dataList1 = Lists.newArrayList(
            new Object[][] {
                {"CUSTOMERS", 1, "Tom", "VoltDB"},
                {"CUSTOMERS", 2, "Jerry", "Bedford"},
                {"CUSTOMERS", 3, "Rachael", "USA"},
                {"CUSTOMERS", 4, "Ross", "Massachusetts"},
                {"CUSTOMERS", 5, "Stephen", "Houston TX"},
                {"ORDERS", 1, 2, "2016-04-23 13:24:57.671000"},
                {"ORDERS", 2, 7, "2015-04-12 10:24:10.671400"},
                {"ORDERS", 3, 5, "2016-01-20 09:24:15.943000"},
                {"ORDERS", 4, 1, "2015-10-30 19:24:00.644000"},
                {"PRODUCTS", 1, "H MART", 20.97},
                {"PRODUCTS", 2, "COSTCO WHOLESALE", 62.66},
                {"PRODUCTS", 3, "CENTRAL ROCK GYM", 22.00},
                {"PRODUCTS", 4, "ATT*BILL PAYMENT", 48.90},
                {"PRODUCTS", 5, "APL* ITUNES", 16.23},
                {"PRODUCTS", 6, "GOOGLE *YouTube", 10.81},
                {"PRODUCTS", 7, "UNIV OF HOUSTON SYSTEM", 218.35},
                {"PRODUCTS", 8, "THE UPS STORE 2287", 36.31},
                {"PRODUCTS", 9, "NNU*XFINITYWIFI", 7.95},
                {"PRODUCTS", 10, "IKEA STOUGHTON", 61.03},
                {"PRODUCTS", 11, "WM SUPERCENTER #5752", 9.74},
                {"PRODUCTS", 12, "STOP & SHOP 0831", 12.28},
                {"PRODUCTS", 13, "VERANDA NOODLE HOUSE", 29.81},
                {"PRODUCTS", 14, "AMC 34TH ST 14 #2120", 38.98},
                {"PRODUCTS", 15, "STARBUCKS STORE 19384", 5.51},
                {"ORDERITEMS", 1, 2, 1},
                {"ORDERITEMS", 1, 7, 1},
                {"ORDERITEMS", 2, 5, 2},
                {"ORDERITEMS", 3, 1, 3},
                {"ORDERITEMS", 3, 15, 1},
                {"ORDERITEMS", 3, 20, 1},
                {"ORDERITEMS", 3, 4, 2},
                {"ORDERITEMS", 3, 26, 5},
                {"ORDERITEMS", 4, 30, 1},
                {"ORDERITEMS", 5, 8, 1},
            }
        );
        ArrayList<Object[]> dataList2 = Lists.newArrayList(
            new Object[][] {
                {"CUSTOMERS", 6, "Mike", "WPI"},
                {"CUSTOMERS", 7, "Max", "New York"},
                {"CUSTOMERS", 8, "Ethan", "Beijing China"},
                {"CUSTOMERS", 9, "Selina", "France"},
                {"CUSTOMERS", 10, "Harry Potter", "Hogwarts"},
                {"ORDERS", 5, 3, "2015-04-23 00:24:45.768000"},
                {"ORDERS", 6, 2, "2016-07-05 16:24:31.384000"},
                {"ORDERS", 7, 4, "2015-03-09 21:24:15.768000"},
                {"ORDERS", 8, 2, "2015-09-01 16:24:42.279300"},
                {"PRODUCTS", 16, "SAN SOO KAP SAN SHUSHI", 10.69},
                {"PRODUCTS", 17, "PLASTC INC.", 155.00},
                {"PRODUCTS", 18, "MANDARIN MALDEN", 34.70},
                {"PRODUCTS", 19, "MCDONALDS F16461", 7.25},
                {"PRODUCTS", 20, "UBER US JUL20 M2E3D", 31.33},
                {"PRODUCTS", 21, "TOUS LES JOURS", 13.25},
                {"PRODUCTS", 22, "GINGER JAPANESE RESTAU", 69.20},
                {"PRODUCTS", 23, "WOO JEON II", 9.58},
                {"PRODUCTS", 24, "INFLIGHT WI-FI - LTV", 7.99},
                {"PRODUCTS", 25, "EXPEDIA INC", 116.70},
                {"PRODUCTS", 26, "THE ICE CREAM STORE", 5.23},
                {"PRODUCTS", 27, "WEGMANS BURLINGTON #59", 22.13},
                {"PRODUCTS", 28, "ACADEMY EXPRESS", 46.80},
                {"PRODUCTS", 29, "TUCKS CANDY FACTORY INC", 7.00},
                {"PRODUCTS", 30, "SICHUAN GOURMET", 37.12},
                {"ORDERITEMS", 5, 12, 6},
                {"ORDERITEMS", 5, 1, 0},
                {"ORDERITEMS", 5, 27, 1},
                {"ORDERITEMS", 6, 0, 1},
                {"ORDERITEMS", 6, 21, 1},
                {"ORDERITEMS", 7, 8, 1},
                {"ORDERITEMS", 7, 19, 1},
                {"ORDERITEMS", 7, 30, 4},
                {"ORDERITEMS", 7, 1, 1},
                {"ORDERITEMS", 8, 25, 2}
            }
        );
        assertEquals(dataList1.size(), dataList2.size());

        // -- 1 -- Test updating the data in the source tables.
        // There are two lists of data, we first insert the data in the first list
        // into the corresponding source tables, then update each row with the data
        // from the second data list.
        System.out.println("Now testing updating the join query view source table.");
        for (int i=0; i<dataList1.size(); i++) {
            insertRow(client, dataList1.get(i));
            verifyViewOnJoinQueryResult(client);
        }
        for (int i=0; i<dataList2.size(); i++) {
            updateRow(client, dataList1.get(i), dataList2.get(i));
            verifyViewOnJoinQueryResult(client);
        }

        // -- 2 -- Test inserting the data into the source tables.
        // We do a shuffle here and in the delete test. But I do believe we still
        // have the full coverage of all the cases because we are inserting and deleting
        // all the rows. The cases updating values of all kinds of aggregations will be
        // tested in one row or another.
        truncateBeforeTest(client);
        // Merge two sub-lists for the following tests.
        dataList1.addAll(dataList2);
        // For more deterministic debugging, consider this instead of shuffle:
        // Collections.reverse(dataList1);
        Collections.shuffle(dataList1);
        System.out.println("Now testing inserting data to the join query view source table.");
        for (int i=0; i<dataList1.size(); i++) {
            insertRow(client, dataList1.get(i));
            verifyViewOnJoinQueryResult(client);
        }

        // Test truncating one or more tables,
        // then explicitly restoring their content.
        System.out.println("Now testing truncating the join query view source table.");
        // Temporarily substitute never for yesAndNo on the next line if you
        // want to bypass testing of rollback after truncate.
        for (int forceRollback : /*default:*/ yesAndNo) { //alt:*/ never) {
            for (int truncateTable1 : yesAndNo) {
                // Each use of 'never' reduces by half the tried
                // combinations of truncate operations.
                for (int truncateTable2 : /*default:*/ yesAndNo) { //alt:*/ never) {
                    // Substitute yesAndNo below for test overkill
                    for (int truncateTable3 : /**/ never) { //*/ yesAndNo) {
                        for (int truncateTable4 : /**/ never) { //*/ yesAndNo) {
                            // truncateSourceTable verifies the short-term effects
                            // of truncation and restoration within the transaction.
                            truncateSourceTables(client, forceRollback,
                                    truncateTable1,
                                    truncateTable2,
                                    truncateTable3,
                                    truncateTable4);
                            // Verify the correctness outside the transaction.
                            verifyViewOnJoinQueryResult(client);
                        }
                    }
                }
            }
        }

    }

    private void truncateSourceTables(Client client, int rollback,
            int truncateTable1, int truncateTable2, int truncateTable3,
            int truncateTable4) {
        try {
            try {
                VoltTable vt = client.callProcedure("TruncateTables", rollback,
                        truncateTable1,
                        truncateTable2,
                        truncateTable3,
                        truncateTable4).getResults()[0];
                assertEquals("TruncateTables was expected to roll back", 0, rollback);
                String result = " UNEXPECTED EMPTY RETURN FROM TruncateTables ";
                if (vt.advanceRow()) {
                    result = vt.getString(0);
                }
                if ( ! "".equals(result)) {
                    fail("TruncateTables detected an unexpected difference: " + result);
                }
            }
            catch (ProcCallException vae) {
                if ( ! vae.getMessage().contains("Rolling back as requested")) {
                    throw vae;
                }
                assertEquals("TruncateTables was not requested to roll back", 1, rollback);
            }
        }
        catch (Exception other) {
            fail("The call to TruncateTables unexpectedly threw: " + other);
        }
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJoinedMatViewSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        URL url = AddPerson.class.getResource("matviewsuite-ddl.sql");
        String schemaPath = url.getPath();
        project.addSchema(schemaPath);

        project.addProcedures(PROCEDURES);

        LocalCluster config = new LocalCluster("matview-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
