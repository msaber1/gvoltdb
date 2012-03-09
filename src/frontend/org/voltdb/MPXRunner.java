/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.pmsg.DTXN;
import org.voltdb.utils.CatalogUtil;

import com.google.protobuf.ByteString;

/**
 * A ProcedureRunner subclass designed to turn MP txns into a set of
 * SP transactions. First version works externally using a standard
 * VoltDB client.
 *
 */
public class MPXRunner extends ProcedureRunner {

    // use constants for the three kinds of procs called
    static final String FRAGMENT_READ_PROC_NAME =
            "@" + FragmentReadProc.class.getSimpleName();
    static final String FRAGMENT_WRITE_PROC_NAME =
            "@" + FragmentWriteProc.class.getSimpleName();
    static final String FRAGMENT_NON_TRANSACTIONAL_PROC_NAME =
            "@" + FragmentNonTransactionalProc.class.getSimpleName();

    static final byte TRUE_BYTE = 1;
    static final byte FALSE_BYTE = 0;

    // regular voltdb client
    // ... should be replaced with mailboxes upon integration with voltdb
    final Client m_client;

    final int m_partitionCount;

    // assume 0 is a local partition for now
    // ... will need to be figured out at init time upon integration
    final int m_localPartitionId = 0;

    // for the current transaction, have we sent a commit or rollback
    // for a specific partition?
    final boolean[] m_sentCommitOrRollback;

    /**
     * Handy callback that lets the logic block until all the callbacks have
     * returned without calling drain()
     *
     * Will be replaced with mailboxy-magic upon integration
     */
    class CallbackForMPRound implements ProcedureCallback {
        final int m_index;
        final ClientResponse[] m_responses;
        final CountDownLatch m_latch;

        CallbackForMPRound(int index, ClientResponse[] responses, CountDownLatch latch) {
            m_index = index;
            m_responses = responses;
            m_latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_responses[m_index] = clientResponse;
            m_latch.countDown();
        }
    };

    /**
     * This is used for fire-and-forget commit and rollback messages.
     * It should probably do something more "real" when we switch
     * to mailboxes.
     */
    class CasualCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse)
                throws Exception {
            assert(clientResponse.getStatus() == ClientResponse.SUCCESS);
        }
    };

    MPXRunner(VoltProcedure procedure,
              SiteProcedureConnection site,
              Procedure catProc,
              Client client,
              int partitionCount) {
        super(procedure, site, catProc, null);
        m_client = client;
        m_partitionCount = partitionCount;
        m_sentCommitOrRollback = new boolean[m_partitionCount];
    }

    /**
     * The main point of overriding call(..) is to ensure that
     * the transaction will send commit or rollback messages
     */
    @Override
    ClientResponseImpl call(long txnId, Object... paramList) {
        ClientResponseImpl cr = null;

        // reset the transaction finalization state
        for (int i = 0; i < m_sentCommitOrRollback.length; i++) {
            m_sentCommitOrRollback[i] = false;
        }

        try {
            cr = super.call(txnId, paramList);
        }
        catch (Exception e) {
            // need to reset txnid because call will reset it before exiting
            m_txnId = txnId;
            sendAnyNeededRollbacks();

            // TODO log this error
            e.printStackTrace();
        }

        try {
            // need to reset txnid because call will reset it before exiting
            m_txnId = txnId;
            // note, if rollbacks sent, then this will do nothing
            sendAnyUnsentCommits();
        }
        finally {
            m_txnId = -1;
        }

        return cr;
    }

    /**
     * For all partitions that haven't seen a commit message, send one.
     * This might be called several times, but should do no real work
     * on subsequent calls.
     */
    void sendAnyUnsentCommits() {
        for (int i = 0; i < m_sentCommitOrRollback.length; i++) {
            if (m_sentCommitOrRollback[i] == false) {
                try {
                    callFragmentProcedure(new CasualCallback(), false, i, true, false, new long[0], null, null);
                }
                catch (Exception e) {
                    // this is bad... not sure what to do about it
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * For all partitions that haven't seen a commit or rollback message,
     * send a rollback message.
     */
    void sendAnyNeededRollbacks() {
        for (int i = 0; i < m_sentCommitOrRollback.length; i++) {
            if (m_sentCommitOrRollback[i] == false) {
                try {
                    callFragmentProcedure(new CasualCallback(), false, i, false, true, new long[0], null, null);
                }
                catch (Exception e) {
                    // this is bad... not sure what to do about it
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Execute a list of SQL statements from procedure code.
     */
    @Override
    protected VoltTable[] executeQueriesInABatch(List<QueuedSQL> batch, boolean isFinalSQL) {
        final int batchSize = batch.size();

        VoltTable[] results = null;

        if (batchSize == 0)
            return new VoltTable[] {};

        try {
            /*
             * Determine if reads and writes are mixed. Can't mix reads and writes
             * because the order of execution is wrong when replicated tables are involved
             * due to ENG-1232
             */
            boolean hasRead = false;
            boolean hasWrite = false;
            boolean allSingleFragment = true;
            for (int i = 0; i < batch.size(); ++i) {
                final SQLStmt stmt = batch.get(i).stmt;
                if (stmt.catStmt.getReadonly()) {
                    hasRead = true;
                } else {
                    hasWrite = true;
                }
                if (stmt.numFragGUIDs > 1) {
                    allSingleFragment = false;
                }
            }
            /*
             * If they are all reads or all writes then we can use the batching slow path
             * Otherwise the order of execution will be interleaved incorrectly so we have to do
             * each statement individually.
             */
            if (hasRead && hasWrite) {
                results = executeQueriesInIndividualBatches(batch, isFinalSQL);
            }
            else {
                if (allSingleFragment) {
                    results = singleFragmentSlowPath(batch, isFinalSQL);
                }
                else {
                    results = multiFragmentSlowPath(batch, isFinalSQL);
                }
            }

            // check expectations
            int i = 0; for (QueuedSQL qs : batch) {
                Expectation.check(m_procedureName, qs.stmt.getText(),
                        i, qs.expectation, results[i]);
                i++;
            }

            return results;
        }
        finally {
            // clear the queued sql list for the next call
            batch.clear();
        }
    }

    /**
     * Run a set of SQL statements that can be executed at one site.
     * In the case of MP txns, these are usually replicated table reads.
     */
    protected VoltTable[] singleFragmentSlowPath(List<QueuedSQL> batch, boolean finalTask) {
        final int batchSize = batch.size();

        long[] fragIds = new long[batchSize];

        for (int i = 0; i < batchSize; ++i) {
            QueuedSQL queuedSQL = batch.get(i);

            // add the bottom frag to the list
            int numFrags = queuedSQL.stmt.catStmt.getFragments().size();
            assert(numFrags == 1);

            // get the first/only fragment
            PlanFragment fragment = null;
            for (PlanFragment frag : queuedSQL.stmt.catStmt.getFragments()) {
                assert(frag != null);
                assert(frag.getHasdependencies() == false);
                fragment = frag;
            }
            assert(fragment != null);

            // update the array of fragment ids
            fragIds[i] = CatalogUtil.getUniqueIdForFragment(fragment);
        }

        byte[] paramSetBytes = getParamSetBytes(batch);

        // note: assume this work is transactional because it's
        // not an aggregation fragment (or it would be in another method).
        return callLocalFragmentBatch(finalTask, false, fragIds,
                paramSetBytes, null);
    }

    /**
     * Run a set of SQL statements that each have two fragments
     * Work will be divided into a collect from all phase, and
     * an aggregation phase.
     */
    protected VoltTable[] multiFragmentSlowPath(List<QueuedSQL> batch, boolean finalTask) {
        final int batchSize = batch.size();

        long[] collectFragIds = new long[batchSize];
        long[] aggFragIds = new long[batchSize];

        // do the set of aggregate fragments not read from/write to table
        // true until set to false by counter-example
        boolean nonTransactionalAgg = true;

        for (int i = 0; i < batchSize; ++i) {
            QueuedSQL queuedSQL = batch.get(i);

            // add the bottom frag to the list
            int numFrags = queuedSQL.stmt.catStmt.getFragments().size();
            assert(numFrags == 2);

            for (PlanFragment frag : queuedSQL.stmt.catStmt.getFragments()) {
                assert(frag != null);

                // frags with no deps are usually collector frags that go to all partitions
                if (frag.getHasdependencies() == false) {
                    collectFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                }
                // frags with deps are usually aggregator frags (always in this path
                else {
                    assert(frag.getHasdependencies());
                    aggFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                    if (frag.getNontransactional() == false)
                        nonTransactionalAgg = false;
                }
            }
        }

        // serialized protobufs parameters as bytes
        byte[] paramSetBytes = getParamSetBytes(batch);

        // latch to allow blocking on all responses
        CountDownLatch latchForRound = new CountDownLatch(m_partitionCount);
        // array to catch responses
        ClientResponse[] responses = new ClientResponse[m_partitionCount];

        for (int i = 0; i < m_partitionCount; i++) {
            CallbackForMPRound callback = new CallbackForMPRound(i, responses, latchForRound);
            // if final task, all transactions except the one doing the agg can finish
            // note that finalTask will get reverted for most writes in callFragmentProcedure
            boolean finishTxn = finalTask && (i != m_localPartitionId);
            try {
                callFragmentProcedure(callback, false, i, finishTxn, false, collectFragIds, paramSetBytes, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // block until all responses received
        try {
            latchForRound.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // check for failures
        for (int p = 0; p < m_partitionCount; p++) {
            if (responses[p].getStatus() != ClientResponse.SUCCESS) {
                throw new RuntimeException(responses[p].getStatusString());
            }
        }

        // convert the responses from the collect fragments
        // into a dependency set for the aggregation fragments
        DTXN.DependencySet.Builder depSetBuilder = DTXN.DependencySet.newBuilder();
        for (int i = 0; i < batchSize; i++) {
            DTXN.DependencySet.DependencyGroup.Builder depGroupBuilder =
                    DTXN.DependencySet.DependencyGroup.newBuilder();
            depGroupBuilder.setDependencyId(i);

            for (int p = 0; p < m_partitionCount; p++) {
                VoltTable[] results = responses[p].getResults();
                VoltTable dep = results[i];
                assert(dep != null);
                FastSerializer fs = new FastSerializer();
                try {
                    fs.writeObject(dep);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                ByteString bs = ByteString.copyFrom(fs.getBytes());
                depGroupBuilder.addDependencies(bs);
            }

            depSetBuilder.addDependencies(depGroupBuilder);
        }

        // convert the protobufs builder to a byte array
        DTXN.DependencySet depSet = depSetBuilder.build();
        byte[] depBytes = depSet.toByteArray();

        // run the aggregation work
        return callLocalFragmentBatch(finalTask, nonTransactionalAgg,
                aggFragIds, paramSetBytes, depBytes);
    }

    /**
     * Wrap the call to the procedure. This calls the correct version of
     * the fragment-executing procedure
     */
    void callFragmentProcedure(ProcedureCallback callback,
                               boolean nonTransactional,
                               int partitionId,
                               boolean finishTransaction,
                               boolean rollbackProgress,
                               long[] fragIds,
                               byte[] paramData,
                               byte[] depData)
    throws NoConnectionsException, IOException {

        boolean readOnly = m_catProc.getReadonly();
        String procName = readOnly ? FRAGMENT_READ_PROC_NAME : FRAGMENT_WRITE_PROC_NAME;
        if (nonTransactional) procName = FRAGMENT_NON_TRANSACTIONAL_PROC_NAME;

        // ensure that commits for write transactions never contain work
        if (!readOnly && finishTransaction) {
            assert(nonTransactional || (fragIds.length == 0));
        }

        m_client.callProcedure(callback,
                               procName,
                               partitionId,
                               getTransactionId(),
                               finishTransaction ? TRUE_BYTE : FALSE_BYTE,
                               rollbackProgress ? TRUE_BYTE : FALSE_BYTE,
                               fragIds,
                               paramData,
                               depData);

        // if this is a commit (or full rollback), note that we've sent it
        // to this partition
        if (finishTransaction) {
            assert(m_sentCommitOrRollback[partitionId] == false);
            m_sentCommitOrRollback[partitionId] = true;
        }
    }

    /**
     * Run a batch on an execution site on a local partition
     * (Note that this code doesn't know how to use a local
     *  partition yet, so it uses id 0)
     */
    VoltTable[] callLocalFragmentBatch(boolean isFinal, boolean nonTransactional,
            long[] fragIds, byte[] paramSetBytes, byte[] depBytes) {
        ClientResponse cr = null;

        // deal with finality and writes
        if (m_catProc.getReadonly() == false) {
            // if this is non-transactional and final,
            // then all transactional work must be done, so we can commit
            isFinal = isFinal && nonTransactional;
        }

        // synchronous call
        SyncCallback cb = null;
        try {
            cb = new SyncCallback();
            callFragmentProcedure(cb, nonTransactional, 0, isFinal,
                    false, fragIds, paramSetBytes, depBytes);
        }
        catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }

        // if we just did final work, also send commit notices
        if (isFinal) {
            sendAnyUnsentCommits();
        }

        // get the response from the actual local fragment batch call
        try {
            cb.waitForResponse();
            cr = cb.getResponse();
        }
        catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
        assert(cr != null);

        if (cr.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException(cr.getStatusString());
        }

        return cr.getResults();
    }

    /**
     * Given a set of batched SQL, get a binary representation of
     * all of the parameter sets to pass to VoltFragmentProcedure
     */
    byte[] getParamSetBytes(List<QueuedSQL> batch) {
        DTXN.ParameterSet.Builder paramSetBuilder = DTXN.ParameterSet.newBuilder();

        for (QueuedSQL queuedSQL : batch) {
            // Build the set of params for the frags
            FastSerializer fs = new FastSerializer();
            try {
                fs.writeObject(queuedSQL.params);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing parameters for SQL statement: " +
                                           queuedSQL.stmt.getText() + " with params: " +
                                           queuedSQL.params.toJSONString(), e);
            }
            ByteBuffer params = fs.getBuffer();
            assert(params != null);
            ByteString bs = ByteString.copyFrom(params);

            paramSetBuilder.addParameterSets(bs);
        }

        DTXN.ParameterSet paramSet = paramSetBuilder.build();
        byte[] paramSetBytes = paramSet.toByteArray();

        return paramSetBytes;
    }
}
