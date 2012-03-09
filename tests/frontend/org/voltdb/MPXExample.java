/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

/**
 * Client code to drive the MPXRunner code before it's integrated into VoltDB.
 *
 */
public class MPXExample {

    public static class TwoRoundWrite extends VoltProcedure {

        public final SQLStmt insert = new SQLStmt("insert into NEW_ORDER values (?,?,?);");
        public final SQLStmt select = new SQLStmt("select max(NO_O_ID) as NOID from NEW_ORDER;");

        public VoltTable run(long value1, long value2, long value3, byte isFinal) {
            // batch 1
            voltQueueSQL(insert, value1, value1, value1);
            voltQueueSQL(select);
            voltQueueSQL(insert, value2, value2, value2);
            voltQueueSQL(select);
            voltExecuteSQL();

            // batch 2
            voltQueueSQL(insert, value3, value3, value3);
            voltQueueSQL(select);
            VoltTable[] results = voltExecuteSQL(isFinal != 0);
            return results[1];
        }
    }

    private static final VoltLogger log = new VoltLogger("HOST");

    static CatalogContext loadCatalog(String pathToCatalog, String pathToDeployment) throws Exception {
        final int MAX_CATALOG_SIZE = 40 * 1024 * 1024; // 40mb
        InputStream fin = null;
        try {
            URL url = new URL(pathToCatalog);
            fin = url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(pathToCatalog);
        }
        byte[] buffer = new byte[MAX_CATALOG_SIZE];
        int readBytes = 0;
        int totalBytes = 0;
        try {
            while (readBytes >= 0) {
                totalBytes += readBytes;
                readBytes = fin.read(buffer, totalBytes, buffer.length - totalBytes - 1);
            }
        } finally {
            fin.close();
        }
        byte[] catalogBytes = Arrays.copyOf(buffer, totalBytes);

        String serializedCatalog = CatalogUtil.loadCatalogFromJar(catalogBytes, log);
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        byte deploymentBytes[] = org.voltcore.utils.MiscUtils.urlToBytes(pathToDeployment);
        DeploymentType deployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));

        CatalogUtil.compileDeploymentAndGetCRC(catalog, deployment, true, false);

        CatalogContext context = new CatalogContext(0, catalog, catalogBytes, 0, 0, -1);

        return context;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // compile a catalog
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(TwoRoundWrite.class);
        builder.addStmtProcedure("getneworders", "select * from NEW_ORDER;");
        builder.addStmtProcedure("insertneworder", "insert into NEW_ORDER values (?,?,?);");
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("mpx.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("mpx.xml"));

        // start a server
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("mpx.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("mpx.xml");
        //config.m_backend = BackendTarget.NATIVE_EE_IPC;
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        // load the catalog here
        CatalogContext context = loadCatalog(Configuration.getPathToCatalogForTest("mpx.jar"),
                                             Configuration.getPathToCatalogForTest("mpx.xml"));
        assert(context != null);

        // create the special MPXClient
        ClientConfig clientConfig = new ClientConfig();
        Client coreClient = ClientFactory.createClient(clientConfig);
        MPXClient client = new MPXClient(context, coreClient, 1);
        client.createConnection("localhost");

        // reused variables
        ClientResponse cr;

        // call some single-part procedures
        cr = client.callProcedure("NEW_ORDER.insert", 1, 1, 1);
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        cr = client.callProcedure("NEW_ORDER.insert", 2, 2, 2);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        // call some multi-part procedures
        cr = client.callProcedure("insertneworder", 3, 3, 3);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        System.out.println(cr.getResults()[0].toJSONString());

        cr = client.callProcedure("getneworders");
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        System.out.println(cr.getResults()[0].toJSONString());

        // run without final hint
        cr = client.callProcedure("MPXExample$TwoRoundWrite", 4, 5, 6, 0);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            System.out.println(cr.getStatusString());
            System.out.println(cr.getStatus());
            assert(false);
        }

        System.out.println(cr.getResults()[0].toJSONString());

        // run with final hint
        cr = client.callProcedure("MPXExample$TwoRoundWrite", 7, 8, 9, 1);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        System.out.println(cr.getResults()[0].toJSONString());

        // shut everything down
        client.close();
        localServer.shutdown();
    }
}