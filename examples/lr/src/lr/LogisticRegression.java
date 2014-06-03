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

package lr;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

import lr.procedures.*;

public class LogisticRegression
{
    public static void main(String[] args) throws Exception {
        double[] weights = new double[14];
        double stepsize = 0.001;

        // init client
        Client client = null;
        ClientConfig config = null;
        try {
            client = ClientFactory.createClient();
            client.createConnection("localhost");
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        client.createConnection("localhost");

        VoltTable keys = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];

        try {
            for (int iter = 0; iter < 10000; iter++) {
                for (int k = 0; k < keys.getRowCount(); k++) {
                    long key = keys.fetchRow(k).getLong(1);
                    VoltTable gt = client.callProcedure("Solve", key, weights, stepsize).getResults()[0];
                    // TODO: now sync, change to async
                    for (int i = 0; i < weights.length; i++) {
                        // TODO: inefficient now
                        weights[i] -= gt.fetchRow(i).getDouble(0);
                    }
                }
                //System.out.println("after the " + iter + " iteration:");
                for (int i =0; i < weights.length; i++)
                    System.out.print(weights[i] + "\t");
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            //System.out.println("after 1000 iteration");
            for (int i = 0; i<weights.length; i++)
                //System.out.println(weights[i]);
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    };

    static class LRCallback implements ProcedureCallback {
        public LRCallback(double[] weights) {
            this.weights = weights;
        }
        @Override
        public void clientCallback(ClientResponse response) {
            if(response.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(response.getStatusString());
                return;
            }

            VoltTable result = response.getResults()[0];
            result.resetRowPosition();
            int i = 0;
            while (result.advanceRow()) {
                weights[i] -= result.getDouble(0);
            }
            /*for (int i = 0; i < weights.length; i++) {
                // TODO: inefficient now
                weights[i] -= gt.fetchRow(i).getDouble(0);
            }*/
        }

        double[] weights;
    }
}
