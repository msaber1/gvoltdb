package voltkv;

import java.io.IOException;

import java.util.*;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.utils.Pair;
import org.voltdb.client.SyncCallback;

import java.util.concurrent.*;

public class KVStore {
    private ScheduledExecutorService m_es;
    private ExecutorService m_blockingService;

    private static final int m_initialBackoff = 40;

    private static int getNextBackoff(int current) {
        if (current == 0) {
            return m_initialBackoff;
        }
        return (int) ((current * 1.5) + (current * .5 * Math.random()));
    }

    public static class KeyLockIntent {
        public KeyLockIntent(String key, boolean getValue) {
            if (key == null) {
                throw new IllegalArgumentException("Key is null");
            }
            m_key = key;
            if (getValue) {
                m_getValue = (byte) 1;
            } else {
                m_getValue = 0;
            }
        }

        private final String m_key;
        private final byte m_getValue;
    }

    public static class KeyUpdateIntent {
        public KeyUpdateIntent(String key, byte newValue[]) {
            this(key, newValue, true);
        }

        private KeyUpdateIntent(String key, byte newValue[], boolean setValue) {
            if (key == null) {
                throw new IllegalArgumentException("Key is null");
            }
            m_key = key;
            m_newValue = newValue;
            m_setValue = setValue;
        }

        private final String m_key;
        private final byte m_newValue[];
        private final boolean m_setValue;
    }

    /*
     * Needs to be idempotent, if the commit fails the txn will be retried and
     * the munger will be tasked with remunging the keys
     */
    public interface KeyMunger {
        List<KeyUpdateIntent> mungeKeys(Map<String, byte[]> keys);
    }

    public static class Response {
        private Response(byte payload[], long rtt) {
            this.payload = payload;
            this.rtt = rtt;
        }

        public final byte payload[];
        public final long rtt;
    }

    public interface Callback {
        public void response(Response r);
    }

    private Client m_client;

    public KVStore() {
    }

    public void init(Client client) {
        m_client = client;
        m_es = Executors.newScheduledThreadPool(1);
        m_blockingService = Executors.newCachedThreadPool();
    }

    public static class SCallback implements Callback {
        private final Semaphore m_lock;
        private Response m_response;

        public SCallback() {
            m_response = null;
            m_lock = new Semaphore(1);
            m_lock.acquireUninterruptibly();
        }

        @Override
        public void response(Response r) {
            m_response = r;
            m_lock.release();
        }

        public Response getResponse() throws InterruptedException {
            m_lock.acquire();
            m_lock.release();
            return m_response;
        }
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_blockingService.shutdown();
        m_es.awaitTermination(1, TimeUnit.DAYS);
        m_blockingService.awaitTermination(1, TimeUnit.DAYS);
    }

    private void put(final String key, final byte value[], final Callback cb,
            final int lastBackoff, final long startTime) throws IOException {
        m_client.callProcedure(new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.err.println(clientResponse.getStatusString());
                }
                byte appStatus = clientResponse.getAppStatus();
                if (appStatus == Byte.MIN_VALUE) {
                    Response r = new Response(null, System.currentTimeMillis()
                            - startTime);
                    cb.response(r);
                } else if (appStatus == Constants.ROW_LOCKED) {
                    final int backoff = getNextBackoff(lastBackoff);
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                put(key, value, cb, backoff, startTime);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                    }, backoff, TimeUnit.MILLISECONDS);
                } else if (appStatus == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY) {
                    VoltTable lockState = clientResponse.getResults()[0];
                    Runnable continuation = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                put(key, value, cb, lastBackoff, startTime);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                    };
                    recoverTransaction(continuation, key, lockState);
                }
            }
        }, "Put", key, value);
    }

    public void put(final String key, final byte value[], final Callback cb)
            throws IOException {
        put(key, value, cb, 0, System.currentTimeMillis());
    }

    private void get(final String key, final Callback cb,
            final int lastBackoff, final long startTime) throws IOException {
        m_client.callProcedure(new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.err.println(clientResponse.getStatusString());
                }
                byte appStatus = clientResponse.getAppStatus();
                if (appStatus == Byte.MIN_VALUE) {
                    Response r = new Response(null, System.currentTimeMillis()
                            - startTime);
                    cb.response(r);
                } else if (appStatus == Constants.ROW_LOCKED) {
                    final int backoff = getNextBackoff(lastBackoff);
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                get(key, cb, backoff, startTime);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                    }, backoff, TimeUnit.MILLISECONDS);
                } else if (appStatus == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY) {
                    VoltTable lockState = clientResponse.getResults()[0];
                    Runnable continuation = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                get(key, cb, lastBackoff, startTime);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                    };
                    recoverTransaction(continuation, key, lockState);
                }
            }
        }, "Get", key, (byte) 0);
    }

    public void get(String key, Callback cb) throws IOException {
        get(key, cb, 0, System.currentTimeMillis());
    }

    private void remove(final String key, final Callback cb,
            final int lastBackoff, final long startTime) throws IOException {
        m_client.callProcedure(new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.err.println(clientResponse.getStatusString());
                }
                byte appStatus = clientResponse.getAppStatus();
                if (appStatus == Byte.MIN_VALUE) {
                    Response r = new Response(null, System.currentTimeMillis()
                            - startTime);
                    cb.response(r);
                } else if (appStatus == Constants.ROW_LOCKED) {
                    final int backoff = getNextBackoff(lastBackoff);
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                remove(key, cb, backoff, startTime);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                    }, backoff, TimeUnit.MILLISECONDS);
                } else if (appStatus == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY) {
                    VoltTable lockState = clientResponse.getResults()[0];
                    Runnable continuation = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                remove(key, cb, lastBackoff, startTime);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                    };
                    recoverTransaction(continuation, key, lockState);
                }
            }
        }, "Remove", key, (byte) 0);
    }

    public void remove(String key, Callback cb) throws IOException {
        remove(key, cb, 0, System.currentTimeMillis());
    }

    public void doTransaction(final List<KeyLockIntent> keys,
            final KeyMunger munger, final Callback cb) throws IOException {
        doTransaction(keys, munger, cb, 0, System.currentTimeMillis());
    }

    /*
     * The only way to break up the txn method into multiple functions
     * without passing all the parameters to each function was to bind them in
     * this class
     */
    private class TxnState implements Runnable {
        /*
         * Parameters to a transaction
         */
        private final List<KeyLockIntent> keys;
        private final KeyMunger munger;
        private final Callback cb;
        private final int lastBackoff;
        private final long startTime;


        /*
         * This section is for data used during the course of a transaction
         * that is accessed from multiple methods defining each
         * stage in the txn lifecycle
         */

        /*
         * Sort the keys so that locks are always acquired in the same order to
         * discourage failed partial lock set acquisition when locks sets are common
         */
        TreeMap<String, Byte> sortedKeys = new TreeMap<String, Byte>();

        /*
         * First key in the sorted key set
         */
        private String rootKey;

        /*
         * Construct a map of values retrieved for each
         * key to pass to the munger.
         */
        TreeMap<String, byte[]> retrievedValues = new TreeMap<String, byte[]>();

        private long expirationTime = Long.MIN_VALUE;

        //Assigned after the root lock is created
        private Long lockTxnId;

        private TxnState(final List<KeyLockIntent> keys,
            final KeyMunger munger, final Callback cb, final int lastBackoff,
            final long startTime) {
            this.keys = keys;
            this.munger = munger;
            this.cb = cb;
            this.lastBackoff = lastBackoff;
            this.startTime = startTime;
        }

        /*
         * Lock the root key, creating the root lock and generating a lock id in the process.
         * The lock id is returned on success and on failure the transaction is rescheduled
         * and null is returned. The caller does not need to take corrective action on failure.
         */
        private Long createRootLock(String rootKey, Byte retrieveRootKeyValue) throws Exception {
            /*
             * Call a procedure to lock the root key
             */
            final ClientResponse r = m_client.callProcedure("Lock", rootKey,
                    null, null, expirationTime, retrieveRootKeyValue);

            if (r.getStatus() != ClientResponse.SUCCESS) {
                new Throwable(r.getStatusString()).printStackTrace();
                System.exit(-1);
            }

            byte appStatus = r.getAppStatus();
            if (appStatus == Constants.EXPIRE_TIME_REACHED
                    || appStatus == Constants.ROW_LOCKED) {
                // ouch?
                final int backoff = getNextBackoff(lastBackoff);
                m_es.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            doTransaction(keys, munger, cb, backoff,
                                    startTime);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }, backoff, TimeUnit.MILLISECONDS);
                return null;
            } else if (appStatus == Constants.KEY_DOES_NOT_EXIST) {
                /*
                 * Exercise left to the reader
                 */
                return null;
            } else if (appStatus == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY) {
                // oy vey
                VoltTable lockState = r.getResults()[0];
                Runnable continuation = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            doTransaction(keys, munger, cb,
                                    lastBackoff, startTime);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                };
                recoverTransaction(continuation, rootKey, lockState);
            }

            if (retrieveRootKeyValue == 1) {
                VoltTable vt = r.getResults()[0];
                vt.advanceRow();
                retrievedValues.put(rootKey, vt.getVarbinary(1));
            }

            return Long.valueOf(r.getAppStatusString());
        }

        /*
         * Acquire secondary row locks, and if that fails,
         * do any necessary cleanup (including the root lock).
         * The transaction is rescheduled if necessary and the caller doesn't need
         * to do anything.
         */
        private boolean acquireSecondaryLocks() throws Exception {
            // Need to queue a replay to unlock these, and only after
            // all are processed can we continue
            Map<String, ClientResponse> keysNeedingReplay = new TreeMap<String, ClientResponse>();
            // If this set is not empty then also apply exponential
            // backoff
            Map<String, ClientResponse> lockedKeys = new TreeMap<String, ClientResponse>();
            // Don't need to do anyting with these, but if this is not
            // empty, apply exponential backoff
            Map<String, ClientResponse> expireTimeReached = new TreeMap<String, ClientResponse>();
            //Successfully locked rows. On failure these only have to be unlocked
            Map<String, ClientResponse> success = new TreeMap<String, ClientResponse>();

            /*
             * Now iterate the rest of the keys and attempt to lock them
             */
            Iterator<Map.Entry<String, Byte>> iter = sortedKeys
                    .entrySet().iterator();

            /*
             * First key is the root lock, skip it
             */
            iter.next();

            /*
             * Invoke all the procedures at once to minimize the window in which
             * locks are being acquired. Collect the callbacks in order to check
             * on the result after.
             */
            TreeMap<String, SyncCallback> lockCallbacks = new TreeMap<String, SyncCallback>();
            while (iter.hasNext()) {
                Map.Entry<String, Byte> nextKey = iter.next();
                SyncCallback cb = new SyncCallback();
                m_client.callProcedure(cb, "Lock", nextKey.getKey(),
                        lockTxnId, rootKey, expirationTime,
                        nextKey.getValue());
                lockCallbacks.put(nextKey.getKey(), cb);
            }

            /*
             * Check each response and put it into a map for appropriate
             * handling later
             */
            for (Map.Entry<String, SyncCallback> entry : lockCallbacks
                    .entrySet()) {
                SyncCallback scb = entry.getValue();
                String attemptedKey = entry.getKey();
                scb.waitForResponse();
                ClientResponse r = scb.getResponse();
                if (r.getStatus() != ClientResponse.SUCCESS) {
                    new Throwable(r.getStatusString()).printStackTrace();
                    System.exit(-1);
                }

                byte appStatus = r.getAppStatus();
                if (appStatus == Constants.ROW_LOCKED) {
                    lockedKeys.put(attemptedKey, r);
                } else if (appStatus == Constants.KEY_DOES_NOT_EXIST) {
                    /*
                     * Exercise for the reader
                     */
                } else if (appStatus == Constants.EXPIRE_TIME_REACHED) {
                    expireTimeReached.put(attemptedKey, r);
                } else if (appStatus == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY) {
                    keysNeedingReplay.put(attemptedKey, r);
                } else if (appStatus == Byte.MIN_VALUE) {
                    /*
                     * hooray it worked!
                     */
                    success.put(attemptedKey, r);
                    VoltTable tables[] = r.getResults();
                    if (tables.length > 0) {
                        VoltTable vt = tables[0];
                        vt.advanceRow();
                        retrievedValues.put(attemptedKey, vt.getVarbinary(1));
                    }
                }
            }

            /*
             * Rather then expose these maps outside of this method I put the cleanup here
             * so that the caller doesn't need them to invoke cleanupFailureSecondaryLockAcquisitions
             */
            if (!keysNeedingReplay.isEmpty() || !lockedKeys.isEmpty() || !expireTimeReached.isEmpty()) {
                cleanupFailedSecondaryLockAcquisitions( keysNeedingReplay, lockedKeys, expireTimeReached, success);
                return false;
            }
            return true;
        }

        private void cleanupFailedSecondaryLockAcquisitions(
                Map<String, ClientResponse> keysNeedingReplay,
                Map<String, ClientResponse> lockedKeys,
                Map<String, ClientResponse> expireTimeReached,
                Map<String, ClientResponse> success) throws Exception {
            /*
             * Life sucks and then you die, do cleanup. Create a list of runnables
             * that will block until all the necessary cleanup has finished.
             * Once all of them have been executed the txn will be retried.
             */
            LinkedList<Runnable> runnablesThatWillWaitOnStuffToBeDone = new LinkedList<Runnable>();

            /*
             * If there were locked keys then there is contention in the system
             * and backoff is probably a good idea. If the expire time was reached
             * then something must be running slowly/overloaded right now so back off is probably a
             * good idea.
             */
            boolean needBackoff = (!lockedKeys.isEmpty() || !expireTimeReached
                    .isEmpty());

            /*
             * First unlock the stuff that was successfully locked,
             * that is pretty straightforward
             */
            //Cheesy hack to make the root key get unlocked by the loop
            success.put(rootKey, null);
            for (String key : success.keySet()) {
                final SyncCallback scb = new SyncCallback();
                m_client.callProcedure(scb, "Unlock", key, null,
                        (byte) 0, lockTxnId, expirationTime);
                runnablesThatWillWaitOnStuffToBeDone
                .add(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            scb.waitForResponse();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                });
            }

            /*
             * Now schedule a replay/unlock process for every
             * key that was expired but still unlocked
             */
            for (Map.Entry<String, ClientResponse> entry : keysNeedingReplay
                    .entrySet()) {
                final CountDownLatch cdl = new CountDownLatch(1);
                final Runnable waitOnLatch = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cdl.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                };

                final String keyNeedingReplay = entry.getKey();
                final ClientResponse responseWithLockState = entry
                        .getValue();
                final VoltTable lockState = responseWithLockState.getResults()[0];
                recoverTransaction(
                        new Runnable() {
                            @Override
                            public void run() {
                                cdl.countDown();
                            }
                        }, keyNeedingReplay, lockState);
                runnablesThatWillWaitOnStuffToBeDone.add(waitOnLatch);
            }

            /*
             * Once all these runnables have finished all the cleanup actions
             * will have completed. At that point it is reasonable to retry the transaction
             */
            for (Runnable runnable : runnablesThatWillWaitOnStuffToBeDone) {
                runnable.run();
            }

            if (needBackoff) {
                /*
                 * Now schedule a retry with backoff
                 */
                final int backoff = getNextBackoff(lastBackoff);
                m_es.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            doTransaction(keys, munger, cb,
                                    backoff, startTime);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }, backoff, TimeUnit.MILLISECONDS);
            } else {
                /*
                 * Resubmit the task so that it is restarted with a clean stack.
                 * For debugging it might be desirable to not do that,
                 * but there would be quite a bit of memory retained.
                 */
                doTransaction(keys, munger, cb,
                        lastBackoff, startTime);
            }
        }

        /*
         * Run the client provided code snippet on the values of the locked
         * rows and retrieve the updates that need to be generated.
         */
        private TreeMap<String,  KeyUpdateIntent> mungeKeys() {
            /*
             * OK, all locks acquired, data retrieved, pass them to the munger
             * to get the resulting updates
             */
            List<KeyUpdateIntent> updateKeys = munger
                    .mungeKeys(retrievedValues);
            if (updateKeys == null) {
                updateKeys = new LinkedList<KeyUpdateIntent>();
            }

            /*
             * Convert the list to a sorted map and validate
             * that the generated updates are for rows that were locked
             */
            TreeMap<String, KeyUpdateIntent> keysToUnlock = new TreeMap<String, KeyUpdateIntent>();
            for (KeyUpdateIntent kui : updateKeys) {
                keysToUnlock.put(kui.m_key, kui);
                if (!sortedKeys.containsKey(kui.m_key)) {
                    new Throwable("Should never happen").printStackTrace();
                    System.exit(-1);
                }
            }

            /*
             * Any rows that didn't have updates generated still need
             * to be unlocked, add them to the map as well.
             */
            Iterator<Map.Entry<String, Byte>>iter = sortedKeys.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, Byte> entry = iter.next();
                if (!keysToUnlock.containsKey(entry.getKey())) {
                    keysToUnlock.put(entry.getKey(),
                            new KeyUpdateIntent(entry.getKey(),
                                    null, false));
                }
            }
            return keysToUnlock;
        }

        /*
         * Journal the commit to the root key so that any process and come along and complete the commit
         * if this one fails. If journaling fails no corrective action is taken,
         * and the caller has to invoke abortOnFailedCommit
         */
        private boolean journalCommit(TreeMap<String, KeyUpdateIntent> keysToUnlock) throws Exception {
            KeyUpdateIntent rootUpdateIntent = keysToUnlock
                    .firstEntry().getValue();

            /*
             * The client shouldn't introduce a new key into the list of updates
             * that obviates the root key... in fact the entire thing should be validated
             * against the list of locked rows. I added that so this check shouldn't
             * be necessary, but it is a nice assertion.
             */
            if (!rootUpdateIntent.m_key.equals(rootKey)) {
                new Throwable("Should never happen").printStackTrace();
                System.exit(-1);
            }

            /*
             * Going to do the commit to the root key, remove it
             * from the list of stuff to unlock afterwards
             * because the commit automatically unlocks the root key
             */
            keysToUnlock.remove(rootUpdateIntent.m_key);

            /*
             * Construct a journal of the updates and keys to unlock
             */
            VoltTable journal = new VoltTable(new ColumnInfo(
                    "TARGET_KEY", VoltType.STRING), new ColumnInfo(
                            "SET_PAYLOAD", VoltType.TINYINT),
                            new ColumnInfo("PAYLOAD", VoltType.VARBINARY));
            for (KeyUpdateIntent kui : keysToUnlock.values()) {
                journal.addRow(kui.m_key,
                        kui.m_setValue ? (byte) 1 : (byte) 0,
                                kui.m_newValue);
            }

            ClientResponse r = m_client.callProcedure("Commit",
                    rootUpdateIntent.m_key,
                    rootUpdateIntent.m_newValue,
                    rootUpdateIntent.m_setValue ? (byte) 1
                            : (byte) 0, lockTxnId, expirationTime,
                            journal);
            if (r.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(r.getStatusString());
                System.exit(-1);
            }

            /*
             * Byte.MIN_VALUE is the default, it means success
             */
            if (r.getAppStatus() == Byte.MIN_VALUE) {
                ArrayList<SyncCallback> unlockCallbacks = new ArrayList<SyncCallback>();
                /*
                 * **** yes we are committed
                 */
                for (KeyUpdateIntent kui : keysToUnlock.values()) {
                    SyncCallback cb = new SyncCallback();
                    unlockCallbacks.add(cb);
                    m_client.callProcedure( cb,
                            "Unlock", kui.m_key, kui.m_newValue,
                            kui.m_setValue ? (byte) 1 : (byte) 0,
                                    lockTxnId, expirationTime);
                }
                for (SyncCallback cb : unlockCallbacks) {
                    cb.waitForResponse();
                }
                m_client.callProcedure( new NullCallback(), "ClearJournal", rootKey, lockTxnId);
                Response response = new Response(null, System.currentTimeMillis() - startTime);
                cb.response(response);
                return true;
            } else if (r.getAppStatus() == Constants.EXPIRE_TIME_REACHED) {
                /*
                 * The txn needs to be aborted by the caller
                 */
                return false;
            } else {
                new Throwable("Shouldn't happen").printStackTrace();
                System.exit(-1);
                //Interesting that Java doesn't pick up on System.exit...
                return false;
            }
        }

        private void abortOnFailedCommit(TreeMap<String, KeyUpdateIntent> keysToUnlock) throws Exception {
            /*
             * HA-HA sucks to be you, unlock everything else to
             * roll back
             */
            List<SyncCallback> callbacksToWaitFor = new LinkedList<SyncCallback>();
            for (KeyUpdateIntent kui : keysToUnlock.values()) {
                SyncCallback scb = new SyncCallback();
                m_client.callProcedure(scb, "Unlock",
                        kui.m_key, null, (byte) 0, lockTxnId,
                        expirationTime);
                callbacksToWaitFor.add(scb);
            }
            for (SyncCallback scb : callbacksToWaitFor) {
                scb.waitForResponse();
            }

            /*
             * Now schedule a retry with backoff
             */
            final int backoff = getNextBackoff(lastBackoff);
            m_es.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        doTransaction(keys, munger, cb,
                                backoff, startTime);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }, backoff, TimeUnit.MILLISECONDS);
        }

        @Override
        public void run() {
            try {
                /*
                 * Populate the sorted key set
                 */
                for (KeyLockIntent k : keys) {
                    sortedKeys.put(k.m_key, k.m_getValue);
                }

                /*
                 * Set the expire time to a long time in the future so
                 * that the entire shebang can be completed without worrying about expiration.
                 */
                expirationTime = System.currentTimeMillis() + (60 * 1000);

                /*
                 * Designate the first key in the lock set to be
                 * the root lock. The root lock is special in that it contains the
                 * state that determines whether the txn succeded in the event that the process
                 * initiating the txn fails.
                 * All other locked rows will point back to the root lock which has to be checked
                 * in the event of expiration. The commit will be journaled at the root lock's partition
                 * by the process initiating the txn and the txn is only committed onces the updates have been
                 * journaled at the root lock.
                 */
                rootKey = sortedKeys.firstEntry().getKey();

                /*
                 * Byte flag indicating whether the value of the root key should be retrieved.
                 * It might be large so save the bandwidth if possible
                 */
                final Byte retrieveRootKeyValue = sortedKeys.firstEntry()
                        .getValue();

                /*
                 * Try to create the root lock
                 */
                lockTxnId = createRootLock(rootKey, retrieveRootKeyValue);
                if (lockTxnId == null) {
                    /*
                     * Failed to acquire the root lock. Corrective action
                     * is taken inside createRootLock so there is
                     * nothing to do here but return
                     */
                    return;
                }

                /*
                 * Now try to acquire locks on all the other keys involved
                 * in the txn. If they can't all be acquired corrective
                 * action is taken in acquireSecondaryLocks
                 */
                if (!acquireSecondaryLocks()) {
                    return;
                }


                /*
                 * Run the client code on the values retrieved
                 * from the locked rows
                 */
                TreeMap<String, KeyUpdateIntent> keysToUnlock = mungeKeys();

                /*
                 * Attempt to journal the updates and the txn commit
                 * to the root lock
                 */
                if (!journalCommit(keysToUnlock)) {
                    /*
                     * Journaling failed (only valid failure is an expired lock)
                     * Unlock everything and schedule the txn to be retried with backoff
                     */
                    abortOnFailedCommit(keysToUnlock);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            } finally {
                /*
                 * The runnables instantiated from within this
                 * class contain a pointer to the enclosing instance.
                 * That will pack rat all the retrieved values if there is a lot of contention
                 * and transactions are rescheduled several times. Clear them just in case.
                 */
                retrievedValues = null;
                sortedKeys = null;
            }
        }
    }

    private void doTransaction(final List<KeyLockIntent> keys,
            final KeyMunger munger, final Callback cb, final int lastBackoff,
            final long startTime) throws IOException {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("No keys supplied for txn");
        }
        m_blockingService.submit(new TxnState(keys, munger, cb, lastBackoff, startTime));
    }

    void recoverTransaction(final Runnable continuation,
            final String contendedKey, final VoltTable lockState)
                    throws IOException {
        if (!lockState.advanceRow()) {
            new Throwable("Failed to recover transaction for "
                    + contendedKey + " because there was no lock state").printStackTrace();
            System.exit(-1);
        }
        final long lockTxnId = lockState.getLong(0);
        final long lockExpirationTime = lockState.getLong(1);
        final String lockRootKey = lockState.getString(2);

        m_client.callProcedure(new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    new Throwable(clientResponse.getStatusString()).printStackTrace();
                    System.exit(-1);
                }
                Map<String, Pair<Boolean, byte[]>> journal = new TreeMap<String, Pair<Boolean, byte[]>>();
                VoltTable journalEntries = clientResponse.getResults()[0];
                while (journalEntries.advanceRow()) {
                    final String targetKey = journalEntries.getString(0);
                    final boolean setPayload = journalEntries.getLong(1) == 1 ? true
                            : false;
                    final byte payload[] = journalEntries.getVarbinary(2);
                    journal.put(targetKey, Pair.of(setPayload, payload));
                }

                if (!journal.isEmpty() && !journal.containsKey(contendedKey)) {
                    new Throwable("Should never happen").printStackTrace();
                    System.exit(-1);
                }

                if (journal.isEmpty()) {
                    m_client.callProcedure(
                            new ProcedureCallback() {

                                @Override
                                public void clientCallback(
                                        ClientResponse clientResponse)
                                                throws Exception {
                                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                        new Throwable(clientResponse
                                                .getStatusString()).printStackTrace();
                                        System.exit(-1);
                                    }
                                    continuation.run();
                                }

                            }, "Unlock", contendedKey, null, (byte) 0,
                            lockTxnId,
                            lockExpirationTime);
                    return;
                }

                final List<SyncCallback> callbacks = Collections
                        .synchronizedList(new LinkedList<SyncCallback>());
                for (Map.Entry<String, Pair<Boolean, byte[]>> entry : journal
                        .entrySet()) {
                    SyncCallback cb = new SyncCallback();
                    callbacks.add(cb);
                    m_client.callProcedure(cb, "Unlock", entry.getKey(), entry
                            .getValue().getSecond(), entry.getValue()
                            .getFirst() ? (byte) 1 : (byte) 0, lockTxnId,
                                    lockExpirationTime);
                }
                m_blockingService.submit(new Runnable() {
                    @Override
                    public void run() {
                        for (SyncCallback cb : callbacks) {
                            try {
                                cb.waitForResponse();
                                ClientResponse cr = cb.getResponse();
                                if (cr.getStatus() != ClientResponse.SUCCESS) {
                                    new Throwable(cr.getStatusString()).printStackTrace();
                                    System.exit(-1);
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        }
                        continuation.run();
                    }
                });
            }

        }, "GetJournal", lockRootKey, lockTxnId);
    }
}
