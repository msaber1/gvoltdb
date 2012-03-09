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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;

/**
 * This class acts like a VoltDB client, but it intercepts MP
 * transactions and runs them using a local non-transactional
 * ProcedureRunner (a subclass called MPXRunner).
 *
 * The MPXRunner breaks MP txns into a bunch of single partition
 * transactions.
 *
 * This class is not really test or product code, but rather
 * something cooked up to test MPXRunner. It should get removed
 * when MPXRunner gets integrated into the internals.
 *
 */
public class MPXClient implements Client {

    final Client m_coreClient;
    final CatalogContext m_context;
    final TransactionIdManager m_txnIdManager = new TransactionIdManager(666, 0);

    final Map<String, MPXRunner> mpProcs = new HashMap<String, MPXRunner>();

    class SPC implements SiteProcedureConnection {

        @Override
        public void registerPlanFragment(long pfId, ProcedureRunner proc) { assert(false); }
        @Override
        public long getCorrespondingSiteId() { return 0; }
        @Override
        public int getCorrespondingPartitionId() { return 0; }
        @Override
        public int getCorrespondingHostId() { return 0; }
        @Override
        public void loadTable(long txnId, String clusterName,
                String databaseName, String tableName, VoltTable data)
                throws VoltAbortException { assert(false); }
        @Override
        public VoltTable[] executeQueryPlanFragmentsAndGetResults(
                long[] planFragmentIds, int numFragmentIds,
                ParameterSet[] parameterSets, int numParameterSets, long txnId,
                boolean readOnly) throws EEException { assert(false); return null; }
        @Override
        public VoltTable executePlanFragment(long fragmentId, int inputDepId,
                ParameterSet params) { assert(false); return null; }
        @Override
        public long getReplicatedDMLDivisor() { assert(false); return 0; }
        @Override
        public void simulateExecutePlanFragments(long txnId, boolean readOnly) { assert(false); }
        @Override
        public Map<Integer, List<VoltTable>> recursableRun(
                TransactionState currentTxnState) { assert(false); return null; }

    }

    public MPXClient(CatalogContext context, Client coreClient, int partitionCount) throws Exception {
        m_context = context;
        m_coreClient = coreClient;

        Database db = m_context.database;
        for (Procedure proc : db.getProcedures()) {
            if (proc.getSinglepartition()) // skip singles
                continue;
            if (proc.getTypeName().startsWith("@")) // skip sysprocs (for now)
                continue;

            VoltProcedure procedure = null;
            MPXRunner runner = null;

            if (proc.getHasjava()) {

                String clsName = proc.getClassname();
                @SuppressWarnings("unchecked")
                Class<? extends VoltProcedure> procCls = (Class<? extends VoltProcedure>) m_context.classForProcedure(clsName);

                procedure = procCls.newInstance();
            }
            else {
                procedure = new ProcedureRunner.StmtProcedure();
            }

            assert(procedure != null);
            runner = new MPXRunner(procedure, new SPC(), proc, this, partitionCount);
            mpProcs.put(proc.getTypeName(), runner);
        }
    }

    @Override
    public void createConnection(String host) throws UnknownHostException, IOException {
        m_coreClient.createConnection(host);
    }

    @Override
    public void createConnection(String host, int port) throws UnknownHostException, IOException {
        m_coreClient.createConnection(host, port);
    }

    @Override
    public ClientResponse callProcedure(String procName, Object... parameters) throws IOException, NoConnectionsException, ProcCallException {
        SyncCallback cb = new SyncCallback();
        boolean success = callProcedure(cb, procName, parameters);
        assert(success);
        try {
            cb.waitForResponse();
        }
        catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
        return cb.getResponse();
    }

    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters) throws IOException, NoConnectionsException {
        MPXRunner runner = mpProcs.get(procName);
        if (runner == null) {
            return m_coreClient.callProcedure(callback, procName, parameters);
        }

        // assume MP
        long txnId = m_txnIdManager.getNextUniqueTransactionId();
        ClientResponseImpl response = runner.call(txnId, parameters);
        if (callback != null) {
            try {
                callback.clientCallback(response);
            }
            catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean callProcedure(ProcedureCallback callback,
            int expectedSerializedSize, String procName, Object... parameters)
            throws IOException, NoConnectionsException {
        assert(false);
        return false;
    }

    @Override
    @Deprecated
    public int calculateInvocationSerializedSize(String procName, Object... parameters) {
        return m_coreClient.calculateInvocationSerializedSize(procName, parameters);
    }

    @Override
    public ClientResponse updateApplicationCatalog(File catalogPath,
            File deploymentPath) throws IOException, NoConnectionsException,
            ProcCallException {
        assert(false);
        return null;
    }

    @Override
    public boolean updateApplicationCatalog(ProcedureCallback callback,
            File catalogPath, File deploymentPath) throws IOException,
            NoConnectionsException {
        assert(false);
        return false;
    }

    @Override
    public void drain() throws NoConnectionsException, InterruptedException {
        m_coreClient.drain();
    }

    @Override
    public void close() throws InterruptedException {
        m_coreClient.close();
    }

    @Override
    public void backpressureBarrier() throws InterruptedException {
        m_coreClient.backpressureBarrier();
    }

    @Override
    public VoltTable getIOStats() {
        return m_coreClient.getIOStats();
    }

    @Override
    public VoltTable getIOStatsInterval() {
        return m_coreClient.getIOStatsInterval();
    }

    @Override
    public VoltTable getProcedureStats() {
        return m_coreClient.getProcedureStats();
    }

    @Override
    public VoltTable getProcedureStatsInterval() {
        return m_coreClient.getProcedureStatsInterval();
    }

    @Override
    public VoltTable getClientRTTLatencies() {
        return m_coreClient.getClientRTTLatencies();
    }

    @Override
    public VoltTable getClusterRTTLatencies() {
        return m_coreClient.getClusterRTTLatencies();
    }

    @Override
    public Object[] getInstanceId() {
        return m_coreClient.getInstanceId();
    }

    @Override
    public String getBuildString() {
        return m_coreClient.getBuildString();
    }

    @Override
    public void configureBlocking(boolean blocking) {
        m_coreClient.configureBlocking(blocking);
    }

    @Override
    public boolean blocking() {
        return m_coreClient.blocking();
    }

}
