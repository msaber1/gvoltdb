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

import voltkv.Constants;

@ProcInfo
(
        partitionInfo   = "store.key:0",
        singlePartition = true
        )

public class Commit extends VoltProcedure
{
    // Checks if key exists, and is locked
    public final SQLStmt checkStmt = new SQLStmt("SELECT lock_txnid, lock_expiration_time, lock_root_key " +
            "FROM store WHERE key = ?;");

    /*
     * Apply the update and unlock the row
     */
    public final SQLStmt unlockIncludingValueUpdateStmt = new SQLStmt("UPDATE store SET value = ?, " +"" +
            "lock_txnid = NULL, lock_expiration_time = NULL, lock_root_key = NULL WHERE key = ?;");

    /*
     * Just unlock the row
     */
    public final SQLStmt unlockStmt = new SQLStmt("UPDATE store SET lock_txnid = NULL, " +
            "lock_expiration_time = NULL, lock_root_key = NULL WHERE key = ?;");

    /*
     * Insert the updates of the transaction into the journal.
     */
    public final SQLStmt insertJournal = new SQLStmt("INSERT INTO JOURNAL VALUES( ?, ?, ?, ?, ?, ?);");

    public VoltTable run(String key, byte value[], byte setValue, long lockTxnId, long expireTime, VoltTable journal)
    {
        final long now = getTransactionTime().getTime();

        // Check whether the key exists
        voltQueueSQL(checkStmt, key);

        VoltTable rowTable = voltExecuteSQL()[0];
        // Does the row to be journalled exist?
        if (rowTable.getRowCount() == 0) {
            if (expireTime < now) {
                setAppStatusCode(Constants.EXPIRE_TIME_REACHED);
                return null;
            }
            throw new VoltAbortException();
        } else {
            /*
             * Do some basic sanity checks to make sure that the thing is locked with the right lock
             */
            rowTable.advanceRow();

            //If this is not the root lock key, did expiration occur?
            rowTable.getString(2);
            if (!rowTable.wasNull()) {
                if (expireTime < now) {
                    //Not a problem, we just missed our window to commit
                    setAppStatusCode(Constants.EXPIRE_TIME_REACHED);
                    return null;
                }
                throw new VoltAbortException("Really shouldn't happen");
            }

            final long storeLockTxnId = rowTable.getLong(0);
            if (rowTable.wasNull()) {
                if (expireTime < now) {
                    //Not a problem, we just missed our window to commit
                    setAppStatusCode(Constants.EXPIRE_TIME_REACHED);
                    return null;
                }
                throw new VoltAbortException("Really shouldn't happen");
            }
            if (storeLockTxnId != lockTxnId) {
                if (expireTime < now) {
                    //Not a problem, we just missed our window to commit
                    setAppStatusCode(Constants.EXPIRE_TIME_REACHED);
                    return null;
                }
                throw new VoltAbortException("Lock txn ids don't match! Ouch!");
            }

            final long lockExpirationTime = rowTable.getLong(1);
            if (lockExpirationTime != expireTime) {
                throw new VoltAbortException("Shouldn't happen");
            }

            /*
             * At this point if the commit is happening
             * it doesn't matter if the expire time has passed if there hasn't been contention
             * at the root key. If the lock is still valid you can commit past the expiration time
             * All the previous checks show there is no contention because all the lock metadata is still
             * there
             */

            /*
             * Unlock the row and update if requested
             */
            if (setValue == 1) {
                voltQueueSQL(unlockIncludingValueUpdateStmt, value, key);
            } else {
                voltQueueSQL(unlockStmt, key);
            }

            if (journal != null) {
                /*
                 * Journal the updates to the other locked rows to make it atomic
                 */
                while (journal.advanceRow()) {
                    voltQueueSQL(insertJournal,
                            key,
                            lockTxnId,
                            lockExpirationTime,
                            journal.getStringAsBytes(0),
                            (byte)journal.getLong(1),
                            journal.getVarbinary(2));
                }
            }
            voltExecuteSQL(true);
            return null;
        }
    }
}