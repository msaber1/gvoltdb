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
        partitionInfo   = "store.key:0"
        , singlePartition = true
        )

public class Remove extends VoltProcedure
{
    // Checks if key exists
    public final SQLStmt checkStmt = new SQLStmt("SELECT lock_txnid, lock_expiration_time, lock_root_key " +
            "FROM store WHERE key = ?;");

    // Inserts a key/value pair
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO store (key, value) VALUES (?, ?);");

    // Deletes a key/value pair
    public final SQLStmt deleteStmt = new SQLStmt("DELETE FROM store WHERE key = ?;");

    public VoltTable run(String key)
    {
        // Check whether the pair exists and is locked
        voltQueueSQL(checkStmt, key);

        VoltTable rowTable = voltExecuteSQL()[0];
        // Insert new or update existing key depending on result
        if (rowTable.getRowCount() == 0) {
            VoltTable vt = new VoltTable(new ColumnInfo("TUPLES_CHANGED", VoltType.BIGINT));
            vt.addRow(0L);
            return vt;
        } else {
            rowTable.advanceRow();
            //Check if the row is locked
            final long lockExpirationTime = rowTable.getLong(1);
            if (rowTable.wasNull()) {
                //No lock
                voltQueueSQL(deleteStmt, key);
                return voltExecuteSQL(true)[0];
            } else {
                final long lock_txnid = rowTable.getLong(0);
                //It was locked, has the lock expired?
                if (lockExpirationTime < getTransactionTime().getTime()) {
                    //Crap it expired. More work for me
                    String lockRootKey = rowTable.getString(2);
                    if (rowTable.wasNull()) {
                        //If the root lock (this key) is expired
                        //that means the commit never happened, safe to clobber
                        voltQueueSQL(deleteStmt, key);
                        return voltExecuteSQL(true)[0];
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
