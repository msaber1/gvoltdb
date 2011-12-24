/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

// Put stored procedure
//
//   Puts the given Key-Value pair


package voltkv.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import voltkv.Constants;

@ProcInfo
(
        partitionInfo   = "store.key:0",
        singlePartition = true
        )

public class Lock extends VoltProcedure
{
    // Checks if key exists, and is locked
    public final SQLStmt checkStmt = new SQLStmt("SELECT lock_txnid, lock_expiration_time, lock_root_key " +
            "FROM store WHERE key = ?;");

    // Updates a key/value pair
    public final SQLStmt lockStmt = new SQLStmt("UPDATE store SET lock_txnid = ?, lock_expiration_time = ?, lock_root_key = ? WHERE key = ?;");

    public final SQLStmt selectStmt = new SQLStmt("SELECT key, value FROM store WHERE key = ?");

    public VoltTable run(String key, long lockTxnId, String rootLock, long expireTime, byte returnValue)
    {
        if (lockTxnId != VoltType.NULL_BIGINT && rootLock == null) {
            throw new VoltAbortException("If a lock txnid is supplied then the root lock key must be supplied as well");
        }
        if (lockTxnId == VoltType.NULL_BIGINT && rootLock != null) {
            throw new VoltAbortException("If a root lock key is supplied then the lock txnid must be supplied as well");
        }

        final long now = getTransactionTime().getTime();
        if (expireTime < now) {
            setAppStatusCode(Constants.EXPIRE_TIME_REACHED);
            return null;
        }

        // Check whether the key exists
        voltQueueSQL(checkStmt, key);

        VoltTable rowTable = voltExecuteSQL()[0];
        // Does the row to be locked exist?
        if (rowTable.getRowCount() == 0) {
            setAppStatusCode(Constants.KEY_DOES_NOT_EXIST);
            return null;
        } else {
            rowTable.advanceRow();
            //Check if the row is locked
            final long lockExpirationTime = rowTable.getLong(1);
            if (rowTable.wasNull()) {
                //No lock
                if (rootLock == null) {
                    //this is the root lock, use this txnid for the lock
                    final long txnId = super.getTransactionId();
                    voltQueueSQL(lockStmt, txnId, expireTime, rootLock, key);
                    //Be weird and put it in the app status string
                    setAppStatusString(Long.toString(txnId));
                } else {
                    //The txnid for the lock is provided
                    voltQueueSQL(lockStmt, lockTxnId, expireTime, rootLock, key);
                }
                if (returnValue == 1) {
                    //Optionally return  the locked row data
                    voltQueueSQL(selectStmt, key);
                    return voltExecuteSQL(true)[1];
                } else {
                    voltExecuteSQL(true);
                    return null;
                }
            } else {
                final long lock_txnid = rowTable.getLong(0);
                //It was locked, has the lock expired?
                if (lockExpirationTime < now) {
                    //Crap it expired. More work for me
                    String lockRootKey = rowTable.getString(2);
                    if (rowTable.wasNull()) {
                        //If the root lock (this key) is expired
                        //that means the commit never happened
                        if (rootLock == null) {
                            //this is the root lock, use this txnid for the lock
                            final long txnId = super.getTransactionId();
                            voltQueueSQL(lockStmt, txnId, expireTime, rootLock, key);
                            //Be weird and put it in the app status string
                            setAppStatusString(Long.toString(txnId));
                        } else {
                            //The txnid for the lock is provided
                            voltQueueSQL(lockStmt, lockTxnId, expireTime, rootLock, key);
                        }
                        if (returnValue == 1) {
                            //Optionally return  the locked row data
                            voltQueueSQL(selectStmt, key);
                            return voltExecuteSQL(true)[1];
                        } else {
                            voltExecuteSQL(true);
                            return null;
                        }
                    } else {
                        //The root lock is elsewhere, need to go fishing to find out the
                        //fate of the txn associated with this lock
                        VoltTable vt = new VoltTable(
                                new ColumnInfo("LOCK_TXNID", VoltType.BIGINT),
                                new ColumnInfo("LOCK_EXPIRATION_TIME", VoltType.BIGINT),
                                new ColumnInfo("ROOT_KEY", VoltType.STRING));
                        vt.addRow(lock_txnid, lockExpirationTime, lockRootKey);
                        setAppStatusCode(Constants.EXPIRED_LOCK_MAY_NEED_REPLAY);
                        return vt;
                    }
                } else {
                    //It's locked, too bad so sad
                    setAppStatusCode(Constants.ROW_LOCKED);
                    return null;
                }
            }
        }
    }
}