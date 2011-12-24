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
     * Longest method in the world
     */
    private void doTransaction(final List<KeyLockIntent> keys,
            final KeyMunger munger, final Callback cb, final int lastBackoff,
            final long startTime) throws IOException {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("No keys supplied for txn");
        }
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    TreeMap<String, Byte> sortedKeys = new TreeMap<String, Byte>();
                    for (KeyLockIntent k : keys) {
                        sortedKeys.put(k.m_key, k.m_getValue);
                    }

                    final long expirationTime = System.currentTimeMillis() + (60 * 1000);
                    final String rootKey = sortedKeys.firstEntry().getKey();
                    final Byte rootKeyValue = sortedKeys.firstEntry()
                            .getValue();
                    // Gonna reuse r later, hold on to your hat
                    ClientResponse r = m_client.callProcedure("Lock", rootKey,
                            null, null, expirationTime, rootKeyValue);
                    if (r.getStatus() != ClientResponse.SUCCESS) {
                        new Throwable(r.getStatusString()).printStackTrace();
                        System.exit(-1);
                    }
                    // also going to reuse this variable later
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
                        return;
                    } else if (appStatus == Constants.KEY_DOES_NOT_EXIST) {
                        /*
                         * Exercise left to the reader
                         */
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

                    final TreeMap<String, byte[]> retrievedValues = new TreeMap<String, byte[]>();
                    if (rootKeyValue == 1) {
                        VoltTable vt = r.getResults()[0];
                        vt.advanceRow();
                        retrievedValues.put(rootKey, vt.getVarbinary(1));
                    }

                    /*
                     * Now lock the rest of the keys
                     */
                    final long lockTxnId = Long.valueOf(r.getAppStatusString());
                    Iterator<Map.Entry<String, Byte>> iter = sortedKeys
                            .entrySet().iterator();
                    iter.next();
                    TreeMap<String, SyncCallback> lockCallbacks = new TreeMap<String, SyncCallback>();
                    while (iter.hasNext()) {
                        Map.Entry<String, Byte> nextKey = iter.next();
                        SyncCallback cb = new SyncCallback();
                        m_client.callProcedure(cb, "Lock", nextKey.getKey(),
                                lockTxnId, rootKey, expirationTime,
                                nextKey.getValue());
                        lockCallbacks.put(nextKey.getKey(), cb);
                    }

                    // Need to queue a replay to unlock these, and only after
                    // all are processed can we continue
                    Map<String, ClientResponse> keysNeedingReplay = new TreeMap<String, ClientResponse>();
                    // If this set is not empty then also apply exponential
                    // backoff
                    Map<String, ClientResponse> lockedKeys = new TreeMap<String, ClientResponse>();
                    // Don't need to do anyting with these, but if this is not
                    // empty, apply exponential backoff
                    Map<String, ClientResponse> expireTimeReached = new TreeMap<String, ClientResponse>();
                    // These need to be straight up unlocked since we locked
                    // them.
                    Map<String, ClientResponse> success = new TreeMap<String, ClientResponse>();

                    /*
                     * Dear god... check if all of them were locked... and ****
                     * all roll back and possibly recover an unrelated txn if
                     * they weren't. What did I do to deserve this?
                     */
                    for (Map.Entry<String, SyncCallback> entry : lockCallbacks
                            .entrySet()) {
                        SyncCallback scb = entry.getValue();
                        String attemptedKey = entry.getKey();
                        scb.waitForResponse();
                        r = scb.getResponse();
                        if (r.getStatus() != ClientResponse.SUCCESS) {
                            new Throwable(r.getStatusString()).printStackTrace();
                            System.exit(-1);
                        }

                        appStatus = r.getAppStatus();
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
                     * **** yes it worked
                     */
                    if (keysNeedingReplay.isEmpty() && lockedKeys.isEmpty()
                            && expireTimeReached.isEmpty()) {
                        List<KeyUpdateIntent> updateKeys = munger
                                .mungeKeys(retrievedValues);
                        if (updateKeys == null) {
                            updateKeys = new LinkedList<KeyUpdateIntent>();
                        }

                        TreeMap<String, KeyUpdateIntent> keysToUnlock = new TreeMap<String, KeyUpdateIntent>();
                        for (KeyUpdateIntent kui : updateKeys) {
                            keysToUnlock.put(kui.m_key, kui);
                        }
                        iter = sortedKeys.entrySet().iterator();
                        while (iter.hasNext()) {
                            Map.Entry<String, Byte> entry = iter.next();
                            if (!keysToUnlock.containsKey(entry.getKey())) {
                                keysToUnlock.put(entry.getKey(),
                                        new KeyUpdateIntent(entry.getKey(),
                                                null, false));
                            }
                        }

                        KeyUpdateIntent rootUpdateIntent = keysToUnlock
                                .firstEntry().getValue();
                        if (!rootUpdateIntent.m_key.equals(rootKey)) {
                            new Throwable("Should never happen").printStackTrace();
                            System.exit(-1);
                        }

                        /*
                         * Going to do the commit to the root key, remove it
                         * from the list of stuff to unlock
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

                        // Reusing r again!
                        r = m_client.callProcedure("Commit",
                                rootUpdateIntent.m_key,
                                rootUpdateIntent.m_newValue,
                                rootUpdateIntent.m_setValue ? (byte) 1
                                        : (byte) 0, lockTxnId, expirationTime,
                                        journal);
                        if (r.getStatus() != ClientResponse.SUCCESS) {
                            System.err.println(r.getStatusString());
                            System.exit(-1);
                        }
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
                            m_client.callProcedure( "ClearJournal", rootKey, lockTxnId);
                            Response response = new Response(null, System.currentTimeMillis() - startTime);
                            cb.response(response);
                        } else if (r.getAppStatus() == Constants.EXPIRE_TIME_REACHED) {
                            /*
                             * HAH sucks to be you, unlock everything else to
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
                        } else {
                            new Throwable("Shouldn't happen").printStackTrace();
                            System.exit(-1);
                        }

                    } else {
                        /*
                         * Life sucks and then you die, do cleanup
                         */
                        LinkedList<Runnable> runnablesThatWillWaitOnStuffToBeDone = new LinkedList<Runnable>();
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
                         * Now schedule a replay/unlock process for every stupid
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
                             * Resubmit the task so that it is restarted with a clean stack
                             */
                            m_blockingService.submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        doTransaction(keys, munger, cb,
                                                lastBackoff, startTime);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        System.exit(-1);
                                    }
                                }
                            });
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        m_blockingService.submit(r);
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
