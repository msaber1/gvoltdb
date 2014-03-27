/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestStopNode extends RegressionSuite
{
    public TestStopNode(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("");
        return builder;
    }

    class StopCallBack implements ProcedureCallback {
        final String m_expected;
        final long m_hid;

        public StopCallBack(String expected, long hid) {
            m_expected = expected;
            m_hid = hid;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                VoltTable tab = clientResponse.getResults()[0];
                while (tab.advanceRow()) {
                    String status = tab.getString("RESULT");
                    long hid = tab.getLong("HOST_ID");
                    if (hid == m_hid) {
                        assertEquals(m_expected, status);
                        System.out.println("Host " + m_hid + " Matched Expected @StopNode Reslt of: " + m_expected);
                    }
                }
            }
        }

    }
    public void testStopNode() throws Exception {
        Client client = getFullyConnectedClient();
        Thread.sleep(1000);

        boolean lostConnect = false;
        try {
            client.callProcedure(new StopCallBack("SUCCESS", 0), "@StopNode", 0);
            client.drain();
            client.callProcedure("@SystemInformation", "overview");
            client.callProcedure(new StopCallBack("SUCCESS", 1), "@StopNode", 1);
            client.drain();
            client.callProcedure("@SystemInformation", "overview");
            client.callProcedure(new StopCallBack("SUCCESS", 2), "@StopNode", 2);
            client.drain();
            client.callProcedure("@SystemInformation", "overview");
        } catch (Exception ex) {
            lostConnect = true;
        }
        assertFalse(lostConnect);
        try {
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack("FAILED", 0), "@StopNode", 3);
            client.drain();
            //stop a already stopped node should resurn an empty table.
            VoltTable tabl[] = client.callProcedure("@StopNode", 1).getResults();
            System.out.println(tabl[0].toFormattedString());
            //assertEquals(0, tabl.length);
            client.drain();
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack("FAILED", 0), "@StopNode", 4);
            client.drain();
            VoltTable tab = client.callProcedure("@SystemInformation", "overview").getResults()[0];
            client.drain();
        } catch (Exception pce) {
            pce.printStackTrace();
            lostConnect = pce.getMessage().contains("was lost before a response was received");
        }
        assertFalse(lostConnect);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestStopNode.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        //Lets tolerate 3 node failures.
        LocalCluster config = new LocalCluster("decimal-default.jar", 4, 5, 3, BackendTarget.NATIVE_EE_JNI);
        config.setHasLocalServer(false);
        config.setExpectedToCrash(true);
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);
        return builder;
    }
}
