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

@ProcInfo
(
        partitionInfo   = "store.key:0",
        singlePartition = true
        )

/*
 * Attempt to retrieve the journal for a specific key in order to apply all the updates
 * and unlock the rows from the journal
 */
public class GetJournal extends VoltProcedure
{

    public final SQLStmt selectJournal = new SQLStmt("SELECT target_key, set_payload, payload " +
            "FROM journal WHERE lock_txnid = ?;");

    // Checks if key exists, and is locked
    public final SQLStmt checkStmt = new SQLStmt("SELECT lock_txnid, lock_expiration_time " +
            "FROM store WHERE key = ?;");

    /*
     * Just unlock the row
     */
    public final SQLStmt unlockStmt = new SQLStmt("UPDATE store SET lock_txnid = NULL, " +
            "lock_expiration_time = NULL, lock_root_key = NULL WHERE key = ?;");

    public VoltTable run(String key, long lockTxnId)
    {
        final long now = getTransactionTime().getTime();
        voltQueueSQL( checkStmt, key);
        voltQueueSQL( selectJournal, lockTxnId);
        VoltTable vt[] = voltExecuteSQL(false);
        if (vt[0].advanceRow()) {
            final long storeLockTxnId = vt[0].getLong(0);
            if (storeLockTxnId == lockTxnId) {
                final long expirationTime = vt[0].getLong(1);
                if (!(expirationTime < now)) {
                    throw new VoltAbortException("Shouldn't happen");
                }
                voltQueueSQL(unlockStmt, key);
                voltExecuteSQL(true);
            }
        }
        return vt[1];
    }
}