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
        int dim = 3;
        double[] weights = new double[dim];
        double stepsize = 0.0001;
        double lambda = 0.01;

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
            for (int iter = 0; iter < 150; iter++) {
                double[] grad = new double[dim];
                for (int k = 0; k < keys.getRowCount(); k++) {
                    long key = keys.fetchRow(k).getLong(1);
                    VoltTable gt = client.callProcedure("Solve", key, weights, stepsize).getResults()[0];
                    // TODO: now sync, change to async
                    for (int i = 0; i < weights.length; i++) {
                        // TODO: inefficient now
                        //weights[i] -= gt.fetchRow(i).getDouble(0);
                        grad[i] += gt.fetchRow(i).getDouble(0);
                    }
                    //System.out.print(gt);
                    //System.out.println();
                }
                //System.out.println("after the " + iter + " iteration:");
                for (int i =0; i < weights.length; i++){
                    weights[i] -= grad[i] + 0.5 * lambda * weights[i];
                    System.out.print(weights[i] + "\t");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    };
}
