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

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

// Once ReadOnlySlow support MP queries, can extend from AdHocQueryTester again
public class TestReadOnlySlowQueries extends ReadOnlySlowQueryTester {

    Client m_client;
    private final static boolean m_debug = false;
    public static final boolean retry_on_mismatch = true;

    @AfterClass
    public static void tearDownClass()
    {
        try {
            VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        }
        catch (IOException e) {};
    }


    @Test
    public void testSP() throws Exception {
        System.out.println("Starting testSP");
        VoltDB.Configuration config = setUpSPDB();
        ServerThread localServer = new ServerThread(config);

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            m_client = ClientFactory.createClient();
            m_client.createConnection("localhost", config.m_port);

            VoltTable modCount;

            //Hashes to partition 0
            int hashableA;
            //Hashes to partition 1
            int hashableB;
            //Hashes to partition 0
            int hashableC;
            //Hashes to partition 1
            int hashableD;
            if (TheHashinator.getConfiguredHashinatorType() == HashinatorType.LEGACY) {
                hashableA = 4;
                hashableB = 1;
                hashableC = 2;
                hashableD = 3;
            } else {
                hashableA = 8;
                hashableB = 2;
                hashableC = 1;
                hashableD = 4;
            }

            //If things break you can use this to find what hashes where and fix the constants
//            for (int ii = 0; ii < 10; ii++) {
//                System.out.println("Partition " + TheHashinator.getPartitionForParameter(VoltType.INTEGER.getValue(), ii) + " param " + ii);
//            }

            // Unlike TestAdHocPlans, TestAdHocQueries runs the queries against actual (minimal) data.
            // Load that, here.
            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED1 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED1 VALUES (%d, %d);", hashableB, hashableB)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED2 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED2 VALUES (%d, %d);", hashableC, hashableC)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED3 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED3 VALUES (%d, %d);", hashableD, hashableD)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED1 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED1 VALUES (%d, %d);", hashableB, hashableB)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED2 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED2 VALUES (%d, %d);", hashableC, hashableC)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            // verify that inserts to a table partitioned on an integer get handled correctly - results not used later
            for (int i = -7; i <= 7; i++) {
                modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED4 VALUES (%d, %d);", i, i)).getResults()[0];
                assertEquals(1, modCount.getRowCount());
                assertEquals(1, modCount.asScalarLong());
            }

            runAllAdHocSPtests(hashableA, hashableB, hashableC, hashableD);
        }
        finally {
            if (m_client != null) m_client.close();
            m_client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
            System.out.println("Ending testSP");
        }
    }

    /**
     * @param query
     * @param hashable - used to pick a single partition for running the query
     * @param spPartialSoFar - counts from prior SP queries to compensate for unpredictable hashing
     * @param expected - expected value of MP query (and of SP query, adjusting by spPartialSoFar, and only if validatingSPresult).
     * @param validatingSPresult - disables validation for non-deterministic SP results (so we don't have to second-guess the hashinator)
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    @Override
    public int runQueryTest(String query, int hashable, int spPartialSoFar, int expected, int validatingSPresult)
            throws IOException, NoConnectionsException, ProcCallException {

        // Compare to @AdHoc results
        //System.out.println("@AdHoc:");
        VoltTable result = m_client.callProcedure("@AdHoc", query).getResults()[0];
        assertEquals(expected, result.getRowCount());

        //System.out.println("@ReadOnlySlow:");
        VoltTable result2 = m_client.callProcedure("@ReadOnlySlow", query).getResults()[0];

        VoltTable lrrResult = LRRHelper.getTableFromFileTable(result2);
        //System.out.println(lrrResult.toString());
        assertEquals("Result sizes don't match: expected " + expected + ", read "+ lrrResult.getRowCount(),expected, lrrResult.getRowCount());

        result.equals(lrrResult);

        int spResult = lrrResult.getRowCount();

        return spResult;
    }

    public static String m_catalogJar = "adhoc.jar";
    public static String m_pathToCatalog = Configuration.getPathToCatalogForTest(m_catalogJar);
    public static String m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");


    @Test
    public void testSimple() throws Exception {
        System.out.println("Starting testSimple");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();

            VoltTable result;
            VoltTable files;

            VoltTable modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            try {
                files = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH;").getResults()[0];
                fail("Failed to throw Planning Error for multi-partition transaction");
            } catch (Exception e) {}

            // try a huge bigint literal
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5);").getResults()[0];
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            System.out.println(result.toString());
            assertEquals(1, result.getRowCount());
            files = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            assertEquals(1, files.getRowCount());
            System.out.println(files.toString());
            result = LRRHelper.getTableFromFileTable(files);
            System.out.println(result.toString());
            assertEquals(1, result.getRowCount());
            try {
                result = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26.123012';").getResults()[0];
                fail("Failed to throw Planning Error for multi-partition transaction");
            } catch (Exception e) {}


        }
        finally {
            env.tearDown();
            System.out.println("Ending testSimple");
        }
    }


    /**
     * Test environment with configured schema and server.
     */
    public static class TestEnv {

        final VoltProjectBuilder m_builder;
        LocalCluster m_cluster;
        Client m_client = null;

        TestEnv(String pathToCatalog, String pathToDeployment,
                     int siteCount, int hostCount, int kFactor) {

            // hack for no k-safety in community version
            if (!MiscUtils.isPro()) {
                kFactor = 0;
            }

            m_builder = new VoltProjectBuilder();
            //Increase query tmeout as long literal queries taking long time.
            m_builder.setQueryTimeout(60000);
            try {
                m_builder.addLiteralSchema("create table BLAH (" +
                                           "IVAL bigint default 0 not null, " +
                                           "TVAL timestamp default null," +
                                           "DVAL decimal default null," +
                                           "PRIMARY KEY(IVAL));\n" +
                                           "PARTITION TABLE BLAH ON COLUMN IVAL;\n" +
                                           "\n" +
                                           "CREATE TABLE AAA (A1 VARCHAR(2), A2 VARCHAR(2), A3 VARCHAR(2));\n" +
                                           "CREATE TABLE BBB (B1 VARCHAR(2), B2 VARCHAR(2), B3 VARCHAR(2) NOT NULL UNIQUE);\n" +
                                           "CREATE TABLE CCC (C1 VARCHAR(2), C2 VARCHAR(2), C3 VARCHAR(2));\n" +
                                           "\n" +
                                           "CREATE TABLE CHAR_TEST (COL1 VARCHAR(254));\n" +
                                           "CREATE TABLE INT_TEST (COL1 INTEGER);\n" +
                                           "CREATE TABLE SMALL_TEST (COL1 SMALLINT);\n" +
                                           "CREATE TABLE REAL_TEST (REF VARCHAR(1),COL1 REAL);\n" +
                                           "CREATE TABLE REAL3_TEST (COL1 REAL,COL2 REAL,COL3 REAL);\n" +
                                           "CREATE TABLE DOUB_TEST (REF VARCHAR(1),COL1 FLOAT);\n" +
                                           "CREATE TABLE DOUB3_TEST (COL1 FLOAT,COL2 FLOAT\n" +
                                           "   PRECISION,COL3 FLOAT);\n" +
                                           "\n" +
                                           "-- Users may provide an explicit precision for FLOAT_TEST.COL1\n" +
                                           "\n" +
                                           "CREATE TABLE FLOAT_TEST (REF VARCHAR(1),COL1 FLOAT);\n" +
                                           "\n" +
                                           "CREATE TABLE INDEXLIMIT(COL1 VARCHAR(2), COL2 VARCHAR(2),\n" +
                                           "   COL3 VARCHAR(2), COL4 VARCHAR(2), COL5 VARCHAR(2),\n" +
                                           "   COL6 VARCHAR(2), COL7 VARCHAR(2));\n" +
                                           "\n" +
                                           "CREATE TABLE WIDETABLE (WIDE VARCHAR(118));\n" +
                                           "CREATE TABLE WIDETAB (WIDE1 VARCHAR(38), WIDE2 VARCHAR(38), WIDE3 VARCHAR(38));\n" +
                                           "\n" +
                                           "CREATE TABLE TEST_TRUNC (TEST_STRING VARCHAR (6));\n" +
                                           "\n" +
                                           "CREATE TABLE WARNING(TESTCHAR VARCHAR(6), TESTINT INTEGER);\n" +
                                           "\n" +
                                           "CREATE TABLE TV (dec3 DECIMAL(3), dec1514 DECIMAL(15,14),\n" +
                                           "                 dec150 DECIMAL(15,0), dec1515 DECIMAL(15,15));\n" +
                                           "\n" +
                                           "CREATE TABLE TU (smint SMALLINT, dec1514 DECIMAL(15,14),\n" +
                                           "                 integr INTEGER, dec1515 DECIMAL(15,15));\n" +
                                           "\n" +
                                           "CREATE TABLE STAFF\n" +
                                           "  (EMPNUM   VARCHAR(3) NOT NULL UNIQUE,\n" +
                                           "   EMPNAME  VARCHAR(20),\n" +
                                           "   GRADE    DECIMAL(4),\n" +
                                           "   CITY     VARCHAR(15));\n" +
                                           "\n" +
                                           "CREATE TABLE PROJ\n" +
                                           "  (PNUM     VARCHAR(3) NOT NULL UNIQUE,\n" +
                                           "   PNAME    VARCHAR(20),\n" +
                                           "   PTYPE    VARCHAR(6),\n" +
                                           "   BUDGET   DECIMAL(9),\n" +
                                           "   CITY     VARCHAR(15));\n" +
                                           "\n" +
                                           "CREATE TABLE WORKS\n" +
                                           "  (EMPNUM   VARCHAR(3) NOT NULL,\n" +
                                           "   PNUM     VARCHAR(3) NOT NULL,\n" +
                                           "   HOURS    DECIMAL(5),\n" +
                                           "   UNIQUE(EMPNUM,PNUM));\n" +
                                           "\n" +
                                           "CREATE TABLE INTS\n" +
                                           "  (INT1      SMALLINT NOT NULL,\n" +
                                           "   INT2      SMALLINT NOT NULL);\n" +
                                           "CREATE TABLE VOTES\n" +
                                           "  (PHONE_NUMBER BIGINT NOT NULL,\n" +
                                           "   STATE     VARCHAR(2) NOT NULL,\n" +
                                           "   CONTESTANT_NUMBER  INTEGER NOT NULL);\n" +
                                           "\n" +
                                           "CREATE PROCEDURE TestProcedure AS INSERT INTO AAA VALUES(?,?,?);\n" +
                                           "CREATE PROCEDURE Insert AS INSERT into BLAH values (?, ?, ?);\n" +
                                           "CREATE PROCEDURE InsertWithDate AS \n" +
                                           "  INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);\n" +
                                           "\n" +
                                           "CREATE TABLE TS_CONSTRAINT_EXCEPTION\n" +
                                           "  (TS TIMESTAMP UNIQUE NOT NULL,\n" +
                                           "   COL1 VARCHAR(2048)); \n" +
                                           "");

                // add more partitioned and replicated tables, PARTED[1-3] and REPED[1-2]
                AdHocQueryTester.setUpSchema(m_builder, pathToCatalog, pathToDeployment);
            }
            catch (Exception e) {
                e.printStackTrace();
                fail("Failed to set up schema");
            }

            m_cluster = new LocalCluster(pathToCatalog, siteCount, hostCount, kFactor,
                                         BackendTarget.NATIVE_EE_JNI,
                                         LocalCluster.FailureState.ALL_RUNNING,
                                         m_debug);
            m_cluster.setHasLocalServer(true);
            boolean success = m_cluster.compile(m_builder);
            assert(success);

            try {
                MiscUtils.copyFile(m_builder.getPathToDeployment(), pathToDeployment);
            }
            catch (Exception e) {
                fail(String.format("Failed to copy \"%s\" to \"%s\"", m_builder.getPathToDeployment(), pathToDeployment));
            }
        }

        void setUp() {
            m_cluster.startUp();

            try {
                // do the test
                m_client = ClientFactory.createClient();
                m_client.createConnection("localhost", m_cluster.port(0));
            }
            catch (UnknownHostException e) {
                e.printStackTrace();
                fail(String.format("Failed to connect to localhost:%d", m_cluster.port(0)));
            }
            catch (IOException e) {
                e.printStackTrace();
                fail(String.format("Failed to connect to localhost:%d", m_cluster.port(0)));
            }
        }

        void tearDown() {
            if (m_client != null) {
                try {
                    m_client.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Failed to close client");
                }
            }
            m_client = null;

            if (m_cluster != null) {
                try {
                    m_cluster.shutDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Failed to shut down cluster");
                }
            }
            m_cluster = null;

            // no clue how helpful this is
            System.gc();
        }

        boolean isValgrind() {
            if (m_cluster != null)
                return m_cluster.isValgrind();
            return true;
        }

        boolean isMemcheckDefined() {
            return (m_cluster != null) ? m_cluster.isMemcheckDefined() : true;
        }
    }

}
