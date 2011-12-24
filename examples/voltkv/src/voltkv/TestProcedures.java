package voltkv;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltTable;
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
public class TestProcedures {

    private static LocalSingleProcessServer m_config;
    private final List<Client> m_clients = new ArrayList<Client>();

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
        for (Client c : m_clients) {
            c.close();
        }
        m_clients.clear();
        m_config.shutDown();
    }



    @Test
    public void testGet() throws Exception {
        Client client = getClient();

        //Get nothing
        assertFalse(client.callProcedure("Get", "foo", (byte)0).getResults()[0].advanceRow());

        client.callProcedure("@AdHoc", "insert into store values ( 'foo', NULL, NULL, NULL, NULL );");

        //Get an unlocked row
        assertTrue(client.callProcedure("Get", "foo", (byte)0).getResults()[0].advanceRow());

        //lock the row
        long lockTxnId = TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 0);
        long lockExpirationTime = Long.MAX_VALUE;
        client.callProcedure("@AdHoc", "update store set lock_txnid = " + Long.toString(lockTxnId) +
                ", lock_expiration_time = " + Long.toString(lockExpirationTime) + " where key = 'foo'");

        //Can't get it if it is locked
        assertTrue(client.callProcedure("Get", "foo", (byte)0).getAppStatus() == Constants.ROW_LOCKED);

        //Can get a dirty read
        assertTrue(client.callProcedure("Get", "foo", (byte)1).getResults()[0].advanceRow());

        //Expire the lock
        client.callProcedure("@AdHoc", "update store set lock_expiration_time = 1 where key = 'foo'");

        //Lock is expired and it is the root key, should unlock immediately
        assertTrue(client.callProcedure("Get", "foo", (byte)0).getResults()[0].advanceRow());

        //Check that it was unlocked
        assertUnlocked(client, "foo");

        //Relock it, but not as the root key, and expired
        client.callProcedure("@AdHoc", "update store set lock_txnid = " + Long.toString(lockTxnId) +
                ", lock_expiration_time = 1, lock_root_key = 'bar' where key = 'foo'");

        ClientResponse response = client.callProcedure("Get", "foo", (byte)0);
        assertTrue(response.getAppStatus() == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY);
        VoltTable vt = response.getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getLong(0) == lockTxnId);
        assertTrue(vt.getLong(1) == 1);
        assertTrue(vt.getString(2).equals("bar"));
    }

    @Test
    public void testPut() throws Exception {
        Client client = getClient();

        //Putting in a new row should succeed
        client.callProcedure("Put", "foo", null);
        VoltTable vt = client.callProcedure("Get", "foo", (byte)0).getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getVarbinary(1);
        assertTrue(vt.wasNull());

        //Updating should also just work
        client.callProcedure("Put", "foo", new byte[0]);
        vt = client.callProcedure("Get", "foo", (byte)0).getResults()[0];
        assertTrue(vt.advanceRow());
        byte bytes[] = vt.getVarbinary(1);
        assertFalse(vt.wasNull());
        assertTrue(bytes.length == 0);

        //Lock the row
        long lockTxnId = TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 0);
        long lockExpirationTime = Long.MAX_VALUE;
        client.callProcedure("@AdHoc", "update store set lock_txnid = " + Long.toString(lockTxnId) +
                ", lock_expiration_time = " + Long.toString(lockExpirationTime) + " where key = 'foo'");

        //Should fail with row locked
        ClientResponse response = client.callProcedure("Put", "foo", null);
        assertTrue(response.getAppStatus() == Constants.ROW_LOCKED);

        //Should still not be null!
        vt = client.callProcedure("Get", "foo", (byte)1).getResults()[0];
        assertTrue(vt.advanceRow());
        bytes = vt.getVarbinary(1);
        assertFalse(vt.wasNull());
        assertTrue(bytes.length == 0);


        //Expire the lock
        client.callProcedure("@AdHoc", "update store set lock_expiration_time = 1 where key = 'foo'");

        //Should succeed because it is expired and is the root lock
        client.callProcedure("Put", "foo", null);
        vt = client.callProcedure("Get", "foo", (byte)0).getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getVarbinary(1);
        assertTrue(vt.wasNull());

        //Check that it was unlocked as well
        assertUnlocked(client, "foo");

        //Relock it, but not as the root key, and expired
        client.callProcedure("@AdHoc", "update store set lock_txnid = " + Long.toString(lockTxnId) +
                ", lock_expiration_time = 1, lock_root_key = 'bar' where key = 'foo'");

        //Now the put should fail saying it expired, but replay may be necessary
        response = client.callProcedure("Put", "foo", new byte[0]);
        assertTrue(response.getAppStatus() == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY);
        vt = response.getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getLong(0) == lockTxnId);
        assertTrue(vt.getLong(1) == 1);
        assertTrue(vt.getString(2).equals("bar"));

        //Check that the row also wasn't modified as well
        vt = client.callProcedure("Get", "foo", (byte)1).getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getVarbinary(1);
        assertTrue(vt.wasNull());
    }

    private void assertUnlocked(Client c, String key) throws Exception {
        VoltTable vt = c.callProcedure("@AdHoc", "select lock_txnid, lock_expiration_time, " +
                "lock_root_key from store where key = '" + key + "';").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0);
        assertTrue(vt.wasNull());
        vt.getLong(1);
        assertTrue(vt.wasNull());
        vt.getString(2);
        assertTrue(vt.wasNull());
    }

    @Test
    public void testRemove() throws Exception {
        Client client = getClient();

        //Removing a nonexistent row should succeed and return 0
        assertTrue(0L == client.callProcedure("Remove", "foo").getResults()[0].asScalarLong());

        client.callProcedure("Put", "foo", new byte[0]);

        //Removing an unlocked row should succeed and return 1
        assertTrue(1L == client.callProcedure("Remove", "foo").getResults()[0].asScalarLong());

        //Put and lock the row
        client.callProcedure("Put", "foo", new byte[0]);
        long lockTxnId = TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 0);
        long lockExpirationTime = Long.MAX_VALUE;
        client.callProcedure("@AdHoc", "update store set lock_txnid = " + Long.toString(lockTxnId) +
                ", lock_expiration_time = " + Long.toString(lockExpirationTime) +
                ", lock_root_key = 'bar' where key = 'foo'");

        //Row locked, not deleted
        assertTrue(Constants.ROW_LOCKED == client.callProcedure("Remove", "foo").getAppStatus());
        assertTrue(client.callProcedure("Get", "foo", (byte)1).getResults()[0].advanceRow());

        //Expire the lock
        client.callProcedure("@AdHoc", "update store set lock_expiration_time = 1 where key = 'foo'");

        //Should say there is an update that may not be committed
        ClientResponse response = client.callProcedure("Remove", "foo");
        assertTrue(response.getAppStatus() == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY);
        VoltTable vt = response.getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getLong(0) == lockTxnId);
        assertTrue(vt.getLong(1) == 1);
        assertTrue(vt.getString(2).equals("bar"));

        //Make sure it wasn't removed
        assertTrue(client.callProcedure("Get", "foo", (byte)1).getResults()[0].advanceRow());

        //Make it the root lock, then delete it and make sure it was deleted
        client.callProcedure("@AdHoc", "update store set lock_root_key = NULL where key = 'foo'");
        client.callProcedure("Remove", "foo");
        assertTrue(0 == client.callProcedure("@AdHoc", "select count(*) from store").getResults()[0].asScalarLong());
    }

    @Test
    public void testLock() throws Exception {
        Client client = getClient();

        //Insert a row, sanity check it is unlocked
        client.callProcedure("Put", "foo", new byte[0]);
        VoltTable vt = client.callProcedure("@AdHoc", "SELECT lock_txnid, lock_expiration_time, " +
                "lock_root_key from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0); assertTrue(vt.wasNull());
        vt.getLong(1); assertTrue(vt.wasNull());
        vt.getString(2); assertTrue(vt.wasNull());

        //Locking fails if expire time is in the past
        ClientResponse response = client.callProcedure("Lock", "foo", null, null, 1, (byte)1);
        assertTrue(Constants.EXPIRE_TIME_REACHED == response.getAppStatus());
        //Check that it is still unlocked as well
        assertUnlocked(client, "foo");

        //Locking also fails if a lock txnid is supplied but not the root lock key
        boolean threw = false;
        try {
            client.callProcedure("Lock", "foo", 0, null, Long.MAX_VALUE, (byte)1);
        } catch (Exception e) {
            threw = true;
        }
        assertTrue(threw);
        //Check that it is still unlocked as well
        assertUnlocked(client, "foo");

        //Locking also fails if a lock root key is supplied but not the lock txnid
        threw = false;
        try {
            client.callProcedure("Lock", "foo", null, "foo", Long.MAX_VALUE, (byte)1);
        } catch (Exception e) {
            threw = true;
        }
        assertTrue(threw);

        //Check that it is still unlocked as well
        assertUnlocked(client, "foo");

        //Now test locking it as the root lock
        ClientResponse cr = client.callProcedure("Lock", "foo", null, null, Long.MAX_VALUE, (byte)1);
        long lockTxnId = Long.valueOf(Long.valueOf(cr.getAppStatusString()));
        assertTrue(lockTxnId > 0);
        //Did it return the value?
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getVarbinary(1).length == 0);

        //Reset the key
        client.callProcedure("@AdHoc", "delete from store;");
        client.callProcedure("Put", "foo", new byte[0]);
        //Now test locking it as the root lock, but don't get the value
        cr = client.callProcedure("Lock", "foo", null, null, Long.MAX_VALUE, (byte)0);
        lockTxnId = Long.valueOf(Long.valueOf(cr.getAppStatusString()));
        assertTrue(lockTxnId > 0);
        //Did it return the value?
        assertTrue(cr.getResults().length == 0);
        //Did it lock the row with the appropriate data?
        vt = client.callProcedure(
                "@AdHoc",
                "select lock_txnid, lock_expiration_time, lock_root_key from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(vt.getLong(0), lockTxnId);
        assertFalse(vt.wasNull());
        assertTrue(Long.MAX_VALUE == vt.getLong(1));
        vt.getString(2);
        assertTrue(vt.wasNull());

        //Reset the key
        client.callProcedure("@AdHoc", "delete from store;");
        client.callProcedure("Put", "foo", new byte[0]);
        //Now test locking it as not the root lock
        cr = client.callProcedure("Lock", "foo", 0, "bar", Long.MAX_VALUE, (byte)1);
        assertNull(cr.getAppStatusString());
        //Did it return the value?
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getVarbinary(1).length == 0);
        //Did it lock the row with the appropriate data?
        vt = client.callProcedure(
                "@AdHoc",
                "select lock_txnid, lock_expiration_time, lock_root_key from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getLong(0) == 0);
        assertFalse(vt.wasNull());
        assertTrue(Long.MAX_VALUE == vt.getLong(1));
        assertTrue("bar".equals(vt.getString(2)));

        //Check that relocking a non-expired lock fails
        cr = client.callProcedure("Lock", "foo", null, null, Long.MAX_VALUE, (byte)1);
        assertTrue(cr.getAppStatus() == Constants.ROW_LOCKED);

        //Check that locking a non-existent key fails
        cr = client.callProcedure("Lock", "zoo", null, null, Long.MAX_VALUE, (byte)1);
        assertTrue(cr.getAppStatus() == Constants.KEY_DOES_NOT_EXIST);

        //Make the lock expired
        client.callProcedure(
                "@AdHoc",
                "update store set lock_expiration_time = 1 where key = 'foo';");

        //Since it is not the root lock it should require you to go check the status of the txn
        cr = client.callProcedure("Lock", "foo", null, null, Long.MAX_VALUE, (byte)1);
        assertTrue(cr.getAppStatus() == Constants.EXPIRED_LOCK_MAY_NEED_REPLAY);

        //Make the lock the root lock
        client.callProcedure(
                "@AdHoc",
                "update store set lock_root_key = NULL where key = 'foo';");

        //This should succeed and return the value and create a new root lock
        cr = client.callProcedure("Lock", "foo", null, null, Long.MAX_VALUE, (byte)1);
        lockTxnId = Long.valueOf(Long.valueOf(cr.getAppStatusString()));
        assertTrue(lockTxnId > 0);
        //Did it return the value?
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getVarbinary(1).length == 0);
        //Did it lock the row with the appropriate data?
        vt = client.callProcedure(
                "@AdHoc",
                "select lock_txnid, lock_expiration_time, lock_root_key from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getLong(0) == lockTxnId);
        assertFalse(vt.wasNull());
        assertTrue(Long.MAX_VALUE == vt.getLong(1));
        vt.getString(2);
        assertTrue(vt.wasNull());

        //Make the lock expired
        client.callProcedure(
                "@AdHoc",
                "update store set lock_expiration_time = 1 where key = 'foo';");
        //This should succeed and create a new lock pointing to the provided root lock
        cr = client.callProcedure("Lock", "foo", 0, "bar", Long.MAX_VALUE, (byte)0);
        assertNull(cr.getAppStatusString());
        //Did it return the value?
        assertEquals(0, cr.getResults().length);
        //Did it lock the row with the appropriate data?
        vt = client.callProcedure(
                "@AdHoc",
                "select lock_txnid, lock_expiration_time, lock_root_key from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(vt.getLong(0) == 0);
        assertFalse(vt.wasNull());
        assertTrue(Long.MAX_VALUE == vt.getLong(1));
        assertTrue("bar".equals(vt.getString(2)));
    }

    @Test
    public void testUnlock() throws Exception {
        Client client = getClient();

        //Check that unlocking a non-existent row returns an error
        ClientResponse cr = client.callProcedure("Unlock", "foo", null, (byte)0, 0, 0);
        assertEquals(Constants.KEY_DOES_NOT_EXIST, cr.getAppStatus());

        //Unlock a row that isn't locked, this is a noop
        //because you assume this part of the txn was alread applied
        client.callProcedure("Put", "foo", null);
        cr = client.callProcedure("Unlock", "foo", new byte[0], (byte)1, null, 0);
        assertEquals(Constants.ROW_UNLOCKED, cr.getAppStatus());

        //Make sure no changes were made on noop unlock
        VoltTable vt = client.callProcedure("Get", "foo", (byte)0).getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getVarbinary(1);
        assertTrue(vt.wasNull());

        //Now make it a locked row but with the wrong lock txnid, should also be a noop
        client.callProcedure("@AdHoc", "update store set lock_txnid = 5 , lock_expiration_time = " + Long.MAX_VALUE + ", lock_root_key = 'bar' where key = 'foo';");
        cr = client.callProcedure("Unlock", "foo", new byte[0], (byte)1, null, 0);
        assertEquals(Constants.ROW_LOCKED, cr.getAppStatus());

        //Make sure no changes were made on noop unlock
        vt = client.callProcedure("@AdHoc", "select * from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getVarbinary(1);
        assertTrue(vt.wasNull());
        assertEquals(vt.getLong(2), 5);
        assertEquals(Long.MAX_VALUE, vt.getLong(3));
        assertTrue("bar".equals(vt.getString(4)));

        //Now do a successful unlock, but don't apply the value
        cr = client.callProcedure("Unlock", "foo", new byte[0], (byte)0, 5, Long.MAX_VALUE);
        assertEquals(Constants.ROW_UNLOCKED, cr.getAppStatus());
        vt = client.callProcedure("@AdHoc", "select * from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getVarbinary(1);
        assertTrue(vt.wasNull());
        vt.getLong(2);
        assertTrue(vt.wasNull());
        vt.getLong(3);
        assertTrue(vt.wasNull());
        vt.getString(4);
        assertTrue(vt.wasNull());

        //Make it locked again to test applying the value when unlocking
        client.callProcedure("@AdHoc", "update store set lock_txnid = 5 , lock_expiration_time = " + Long.MAX_VALUE + ", lock_root_key = 'bar' where key = 'foo';");
        cr = client.callProcedure("Unlock", "foo", new byte[0], (byte)1, 5, Long.MAX_VALUE);
        assertEquals(Constants.ROW_UNLOCKED, cr.getAppStatus());
        vt = client.callProcedure("@AdHoc", "select * from store where key = 'foo';").getResults()[0];
        assertTrue(vt.advanceRow());
        byte bytes[] = vt.getVarbinary(1);
        assertFalse(vt.wasNull());
        assertEquals(0, bytes.length);
        vt.getLong(2);
        assertTrue(vt.wasNull());
        vt.getLong(3);
        assertTrue(vt.wasNull());
        vt.getString(4);
        assertTrue(vt.wasNull());
    }

    @Test
    public void testCommit() throws Exception {
        Client client = getClient();

        //Key that doesn't exist
        ClientResponse cr = client.callProcedure("Commit", "foo", null, null, null, null, null);
        assertEquals(Constants.KEY_DOES_NOT_EXIST, cr.getAppStatus());

        client.callProcedure("Put", "foo", null);

        //The row is unlocked, and the expire time was reached so it is ok
        cr = client.callProcedure("Commit", "foo", null, null, null, null, null);
        assertEquals(Constants.EXPIRE_TIME_REACHED, cr.getAppStatus());

        //The lock ids don't match, but it is ok because the lock expired
        client.callProcedure("@AdHoc", "update store set lock_txnid = 5, lock_expiration_time = 5 where key = 'foo';");
        cr = client.callProcedure("Commit", "foo", null, null, null, null, null);
        assertEquals(Constants.EXPIRE_TIME_REACHED, cr.getAppStatus());

        cr = client.callProcedure("Commit", "foo", null, null, 5, 5, null);
    }
}
