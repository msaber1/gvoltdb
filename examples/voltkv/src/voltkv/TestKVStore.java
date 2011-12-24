package voltkv;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalSingleProcessServer;

import voltkv.procedures.ClearJournal;
import voltkv.procedures.Commit;
import voltkv.procedures.Get;
import voltkv.procedures.GetJournal;
import voltkv.procedures.Lock;
import voltkv.procedures.Put;
import voltkv.procedures.Remove;
import voltkv.procedures.Unlock;
import voltkv.KVStore.KeyUpdateIntent;
import voltkv.KVStore.SCallback;
import voltkv.KVStore.KeyLockIntent;
import voltkv.KVStore.KeyMunger;

public class TestKVStore {

    private static LocalSingleProcessServer m_config;
    private final List<Client> m_clients = new ArrayList<Client>();
    private KVStore m_store;

    private static final Class<?>[] PROCEDURES =
            new Class<?>[] {
        ClearJournal.class, Commit.class, Get.class, GetJournal.class,
        Lock.class, Put.class, Remove.class, Unlock.class};

    @BeforeClass
    public static void compileProject() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.addSchema("/home/aweisberg/hz_src/examples/voltkv/ddl.sql");
        builder.addProcedures(PROCEDURES);
        builder.addPartitionInfo("store", "key");
        builder.addPartitionInfo("journal", "key");

        m_config = new LocalSingleProcessServer("kvproctest.jar", 4,
                BackendTarget.NATIVE_EE_JNI);
        boolean success = m_config.compile(builder);
        assertTrue(success);
    }

    @Before
    public void setUp() throws Exception {
        m_config.startUp(true);
    }

    private Client getClient() throws Exception {
        Client client = org.voltdb.client.ClientFactory.createClient();
        client.createConnection("localhost");
        m_clients.add(client);
        return client;
    }

    @After
    public void tearDown() throws Exception {
        if (m_store != null) {
            m_store.shutdown();
        }
        m_store = null;
        for (Client c : m_clients) {
            c.close();
        }
        m_clients.clear();
        m_config.shutDown();
    }

    //    @Test
    //    public void testPut() throws Exception {
    //        Client client = getClient();
    //        m_store = new KVStore();
    //        m_store.init(client);
    //
    //        /*
    //         * Put a key uncontended
    //         */
    //        SCallback cb = new SCallback();
    //        m_store.put("foo", new byte[0], cb);
    //        cb.getResponse();
    //        VoltTable vt = client.callProcedure("@AdHoc", "SELECT * FROM STORE;").getResults()[0];
    //        assertTrue(vt.advanceRow());
    //        assertTrue("foo".equals(vt.getString(0)));
    //        assertTrue(vt.getVarbinary(1).length == 0);
    //        vt.getLong(2);
    //        assertTrue(vt.wasNull());
    //        vt.getLong(3);
    //        assertTrue(vt.wasNull());
    //        vt.getString(4);
    //        assertTrue(vt.wasNull());
    //
    //        /*
    //         * Lock the key and make it wait for the lock to time out
    //         */
    //        cb = new SCallback();
    //        client.callProcedure("@AdHoc", "UPDATE STORE SET lock_txnid = 5, lock_expiration_time = " + (System.currentTimeMillis() + 1000));
    //        m_store.put("foo", null, cb);
    //        KVStore.Response r = cb.getResponse();
    //        assertTrue(r.rtt > 800);
    //        vt = client.callProcedure("@AdHoc", "SELECT * FROM STORE;").getResults()[0];
    //        assertTrue(vt.advanceRow());
    //        vt.getVarbinary(1);
    //        assertTrue(vt.wasNull());
    //
    //        //make it not null
    //        cb = new SCallback();
    //        m_store.put("foo", new byte[0], cb); cb.getResponse();
    //        cb = new SCallback();
    //        m_store.put("z", new byte[0], cb); cb.getResponse();
    //
    //        /*
    //         * Set the key up so the txn needs to be recovered, replay should set z to null, and foo should get the value
    //         * from the put which will undo the actions from the replay
    //         */
    //        client.callProcedure("@AdHoc", "UPDATE STORE SET lock_txnid = 10, lock_expiration_time = 5, lock_root_key='bar' where key = 'foo';");
    //        client.callProcedure("@AdHoc", "UPDATE STORE SET lock_txnid = 10, lock_expiration_time = 5, lock_root_key='bar' where key = 'z';");
    //        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'bar', NULL, 10, 5, NULL);");
    //        client.callProcedure("@AdHoc", "INSERT INTO JOURNAL VALUES ( 'bar', 10, 5, 'z', 1, NULL);");
    //        client.callProcedure("@AdHoc", "INSERT INTO JOURNAL VALUES ( 'bar', 10, 5, 'foo', 0, NULL);");
    //
    //        cb = new SCallback();
    //        m_store.put("foo", new byte[3], cb);
    //        r = cb.getResponse();
    //        assertTrue(r.rtt < 500);//shouldn't take long, already expired
    //
    //        vt = client.callProcedure("@AdHoc", "SELECT * FROM STORE;").getResults()[0];
    //        assertTrue(vt.getRowCount() == 3);
    //        while (vt.advanceRow()) {
    //            final String key = vt.getString(0);
    //            if (key.equals("foo")) {
    //                assertTrue(vt.getVarbinary(1).length == 3);
    //            } else {
    //                vt.getVarbinary(1);
    //                assertTrue(vt.wasNull());
    //            }
    //        }
    //    }
    //
    //    private void recoverTransaction(String contendedKey, String lockKey, long lockTxn, long expirationTime) throws Exception {
    //        VoltTable lockState = getLockState( lockTxn, expirationTime, lockKey);
    //        final CountDownLatch latch = new CountDownLatch(1);
    //        m_store.recoverTransaction(new Runnable() {
    //            @Override
    //            public void run() {
    //                latch.countDown();
    //            }
    //        }, contendedKey, lockState);
    //        latch.await();
    //    }
    //
    //    @Test
    //    public void testRecoverTransaction() throws Exception {
    //        Client client = getClient();
    //        m_store = new KVStore();
    //        m_store.init(client);
    //
    //        /*
    //         * Locked row, root key doesn't exist
    //         */
    //        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'foo', NULL, 10, 5, 'bar');");
    //        recoverTransaction("foo", "bar", 10, 5);
    //        assertLockState(client, "foo", false);
    //        client.callProcedure("@AdHoc", "DELETE FROM STORE;");
    //
    //        /*
    //         * Locked row, root key exists, but there is no journal
    //         */
    //        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'foo', NULL, 10, 5, 'bar');");
    //        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'bar', NULL, 10, 5, NULL);");
    //        recoverTransaction("foo", "bar", 10, 5);
    //        assertLockState(client, "foo", false);
    //        client.callProcedure("@AdHoc", "DELETE FROM STORE;");
    //
    //        /*
    //         * Locked row, root key exists, and there is a journal
    //         */
    //        client.callProcedure("Put", "foo", new byte[0]);
    //        client.callProcedure("@AdHoc", "UPDATE STORE SET lock_txnid = 10, lock_expiration_time = 5, lock_root_key='bar' where key = 'foo';");
    //        VoltTable vt = client.callProcedure("@AdHoc", "SELECT * FROM STORE;").getResults()[0];
    //        assertTrue(vt.advanceRow());
    //        vt.getVarbinary(1);
    //        assertFalse(vt.wasNull());
    //        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'bar', NULL, 10, 5, NULL);");
    //        client.callProcedure("@AdHoc", "INSERT INTO JOURNAL VALUES ( 'bar', 10, 5, 'foo', 1, NULL);");
    //        recoverTransaction("foo", "bar", 10, 5);
    //        vt = client.callProcedure("Get", "foo", (byte)0).getResults()[0];
    //        assertTrue(vt.advanceRow());
    //        vt.getVarbinary(1);
    //        assertTrue(vt.wasNull());
    //        client.callProcedure("@AdHoc", "DELETE FROM STORE;");
    //    }

    private VoltTable getLockState(long lockTxnId, long expireTime, String rootKey ) {
        VoltTable vt =  new VoltTable(
                new ColumnInfo("LOCK_TXNID", VoltType.BIGINT),
                new ColumnInfo("LOCK_EXPIRATION_TIME", VoltType.BIGINT),
                new ColumnInfo("ROOT_KEY", VoltType.STRING));
        vt.addRow(lockTxnId, expireTime, rootKey);
        return vt;
    }

    private void assertLockState(Client c, String key, boolean locked) throws Exception {
        VoltTable vt = c.callProcedure("@AdHoc", "select lock_txnid, lock_expiration_time, " +
                "lock_root_key from store where key = '" + key + "';").getResults()[0];
        assertTrue(vt.advanceRow());
        if (locked) {
            vt.getLong(0);
            assertFalse(vt.wasNull());
            vt.getLong(1);
            assertFalse(vt.wasNull());
            vt.getString(2);
            assertFalse(vt.wasNull());
        } else {
            vt.getLong(0);
            assertTrue(vt.wasNull());
            vt.getLong(1);
            assertTrue(vt.wasNull());
            vt.getString(2);
            assertTrue(vt.wasNull());
        }
    }

    @Test
    public void testDoContendedTransactionWithPartialLockAcquisitionNoReplay() throws Exception {
        Client client = getClient();
        KVStore kv = new KVStore();
        kv.init(client);

        long expireTime = System.currentTimeMillis() + 5000 ;
        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'foo', null, 0, " + expireTime + ", 'bar');");
        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'bar', null, 0, " + expireTime + ", 'null');");
        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'a', null, 0, " + Long.MAX_VALUE + " , null);");
        client.callProcedure("@AdHoc", "INSERT INTO STORE VALUES ( 'b', null, null, null, null);");

        KeyMunger munger = new KeyMunger() {
            @Override
            public List<KeyUpdateIntent> mungeKeys(Map<String, byte[]> keys) {
                List<KeyUpdateIntent> updates = new ArrayList<KeyUpdateIntent>();
                updates.add(new KeyUpdateIntent("foo", new byte[32]));
                updates.add(new KeyUpdateIntent("bar", new byte[31]));
                updates.add(new KeyUpdateIntent("b", new byte[30]));
                updates.add(new KeyUpdateIntent("a", new byte[29]));
                return updates;
            }
        };

        SCallback cb = new SCallback();
        kv.doTransaction(
                Arrays.asList(
                        new KeyLockIntent[] {
                                new KeyLockIntent("foo", false),
                                new KeyLockIntent("bar", false),
                                new KeyLockIntent("b", false),
                                new KeyLockIntent("a", false) }),
                                munger, cb);
        cb.getResponse();
        assertTrue(System.currentTimeMillis() > expireTime);

        VoltTable journal = client.callProcedure("@AdHoc", "SELECT * FROM JOURNAL").getResults()[0];
        assertEquals(0, journal.getRowCount());

        VoltTable store = client.callProcedure("@AdHoc", "SELECT * FROM STORE").getResults()[0];
        assertEquals(4, store.getRowCount());
        while (store.advanceRow()) {
            final String key = store.getString(0);
            final byte value[] = store.getVarbinary(1);
            final int length = value.length;
            if (key.equals("foo")) {
                assertEquals(32, length);
            } else if (key.equals("bar")) {
                assertEquals(31, length);
            } else if (key.equals("b")) {
                assertEquals(30, length);
            } else if (key.equals("a")) {
                assertEquals(29, length);
            }
            store.getLong(2);
            assertTrue(store.wasNull());
            store.getLong(3);
            assertTrue(store.wasNull());
            store.getString(4);
            assertTrue(store.wasNull());
        }
    }

    @Test
    public void testDoTransaction() throws Exception {
        Client client = getClient();
        KVStore kv = new KVStore();
        kv.init(client);

        KVStore.SCallback cb = new KVStore.SCallback();
        kv.put("foo", new byte[0], cb);
        cb.getResponse();
        cb = new KVStore.SCallback();
        kv.put("bar", new byte[0], cb);
        cb.getResponse();

        cb = new KVStore.SCallback();
        final CountDownLatch mungerLatch = new CountDownLatch(1);
        final CountDownLatch keysReadyToMunge = new CountDownLatch(1);
        KVStore.KeyMunger munger = new KVStore.KeyMunger() {

            @Override
            public List<KeyUpdateIntent> mungeKeys(Map<String, byte[]> keys) {
                keysReadyToMunge.countDown();
                try {
                    mungerLatch.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                assertTrue(keys.get("foo").length == 0);
                assertTrue(keys.get("bar").length == 0);
                List<KeyUpdateIntent> updates = new ArrayList<KeyUpdateIntent>();
                updates.add(new KeyUpdateIntent("foo", null));
                updates.add(new KeyUpdateIntent("bar", new byte[32]));
                return updates;
            }
        };

        kv.doTransaction(
                Arrays.asList(new KeyLockIntent[] { new KeyLockIntent("foo", true), new KeyLockIntent("bar", true) }),
                munger, cb);
        keysReadyToMunge.await();

        VoltTable store = client.callProcedure("@AdHoc", "SELECT * FROM STORE").getResults()[0];
        assertEquals(2, store.getRowCount());

        while (store.advanceRow()) {        while (store.advanceRow()) {
            final String key = store.getString(0);
            final byte value[] = store.getVarbinary(1);
            if (key.equals("foo")) {
                assertTrue(store.wasNull());
            } else{
                assertEquals(32, value.length);
            }
            store.getLong(2);
            assertTrue(store.wasNull());
            store.getLong(3);
            assertTrue(store.wasNull());
            store.getString(4);
            assertTrue(store.wasNull());
        }
        final String key = store.getString(0);
        final byte value[] = store.getVarbinary(1);
        assertEquals(value.length, 0);
        assertTrue(key.equals("foo") || key.equals("bar"));
        if (key.equals("foo")) {
            assertTrue(store.getString(4).equals("bar"));
        } else {
            store.getString(4);
            assertTrue(store.wasNull());
        }
        store.getLong(2); assertFalse(store.wasNull());
        store.getLong(3); assertFalse(store.wasNull());
        }

        VoltTable journal = client.callProcedure("@AdHoc", "SELECT * FROM JOURNAL").getResults()[0];
        assertEquals(0, journal.getRowCount());

        mungerLatch.countDown();
        cb.getResponse();

        journal = client.callProcedure("@AdHoc", "SELECT * FROM JOURNAL").getResults()[0];
        assertEquals(0, journal.getRowCount());

        store = client.callProcedure("@AdHoc", "SELECT * FROM STORE").getResults()[0];
        assertEquals(2, store.getRowCount());
        while (store.advanceRow()) {
            final String key = store.getString(0);
            final byte value[] = store.getVarbinary(1);
            if (key.equals("foo")) {
                assertTrue(store.wasNull());
            } else{
                assertEquals(32, value.length);
            }
            store.getLong(2);
            assertTrue(store.wasNull());
            store.getLong(3);
            assertTrue(store.wasNull());
            store.getString(4);
            assertTrue(store.wasNull());
        }
        kv.shutdown();
    }
}
