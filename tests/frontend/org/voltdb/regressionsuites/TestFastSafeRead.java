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
import java.nio.file.Files;
import java.nio.file.Paths;

import org.voltdb.BackendTarget;
import org.voltdb.Consistency.ReadLevel;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
/*
 * Functional tests of the statements compiled in the test suite
 * org.voltdb.planner.TestComplexGroupBySuite.
 */

public class TestFastSafeRead extends RegressionSuite {

    public void testXin() throws IOException, ProcCallException {
        System.out.println("test xin...");
        Client client = getClient(60 * 60 * 1000);

        VoltTable vt;
        String sql;

//        sql = "INSERT INTO t1 VALUES (1, 10);";
//        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
//        sql = "INSERT INTO t1 VALUES (2, 20);";
//        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
//        sql = " select b from t1 where a = 1; ";
//        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
//        System.out.println(vt);
//
//        sql = "INSERT INTO t1 VALUES (3, 30);";
//        vt = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = "INSERT INTO t1 VALUES (1, 10);";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt);

        sql = "INSERT INTO t1 VALUES (3, 30);";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt);

        sql = "select b from t1 where a = 3 order by b;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt);

      sql = "INSERT INTO t1 VALUES (2, 10);";
      vt = client.callProcedure("@AdHoc", sql).getResults()[0];
      System.out.println(vt);

        sql = "select b from t1 where a = 2 order by b;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt);
    }

    public void notestMP() throws IOException, ProcCallException {
        System.out.println("test xin...");
        Client client = getClient(60 * 60 * 1000);

        VoltTable vt;
        String sql;

        sql = "INSERT INTO r1 VALUES (1, 10);";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        sql = "INSERT INTO r1 VALUES (2, 20);";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = " select b from r1 where a = 1; ";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt);
    }

    static String readFile(String path) throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }

    public TestFastSafeRead(String name) {
        super(name);
    }
    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestFastSafeRead.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE t1("
                + "a INTEGER not null,"
                + "b integer);" +
                "create index t1_idx on t1 (a);" +

                "partition table t1 on column a;" +


                "CREATE TABLE r1("
                + "a INTEGER not null,"
                + "b integer);" +
                "create index r1_idx on r1 (a);" +

                ""
                ;
        project.setDefaultConsistencyReadLevel(ReadLevel.SAFE_1);
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        boolean success;

        config = new LocalCluster("fastreads-onesite.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
