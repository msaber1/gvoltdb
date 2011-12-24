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

public class Get extends VoltProcedure
{
    // Selects a key/value pair's value
    public final SQLStmt selectStmt = new SQLStmt("SELECT * FROM store WHERE key = ?;");

    public final SQLStmt updateLockStatus = new SQLStmt("UPDATE store SET lock_txnid = NULL, " +
            "lock_expiration_time = NULL, lock_root_key = NULL WHERE key = ?;");

    public VoltTable run(String key, byte readLockedRows)
    {
        voltQueueSQL(selectStmt, key);
        final VoltTable retval = voltExecuteSQL(false)[0];
        if (retval.advanceRow()) {
            //Do I care if it is locked? Maybe I want inconsistent reads
            if (readLockedRows == 1) {
                return retval;
            }

            //Check if the row is locked
            final long lockExpirationTime = retval.getLong(3);
            if (retval.wasNull()) {
                //No lock
                return retval;
            } else {
                final long lock_txnid = retval.getLong(2);
                //It was locked, has the lock expired?
                if (lockExpirationTime < getTransactionTime().getTime()) {
                    //Crap it expired. More work for me
                    String lockRootKey = retval.getString(4);
                    if (retval.wasNull()) {
                        //If the root lock (this key) is expired
                        //that means the commit never happened
                        voltQueueSQL(updateLockStatus, key);
                        voltExecuteSQL(true);
                        return retval;
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
        return retval;
    }
}
