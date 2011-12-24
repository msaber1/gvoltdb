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

/*
 * Unlock a row possibly applying an update in the process
 */
public class Unlock extends VoltProcedure
{
    // Checks if key exists, and is locked
    public final SQLStmt checkStmt = new SQLStmt("SELECT lock_txnid " +
            "FROM store WHERE key = ?;");

    /*
     * Apply the update and unlock the row
     */
    public final SQLStmt unlockIncludingValueUpdateStmt = new SQLStmt("UPDATE store SET value = ?, " +
            "lock_txnid = NULL, lock_expiration_time = NULL, lock_root_key = NULL WHERE key = ?;");

    /*
     * Just unlock the row
     */
    public final SQLStmt unlockStmt = new SQLStmt("UPDATE store SET lock_txnid = NULL, " +
            "lock_expiration_time = NULL, lock_root_key = NULL WHERE key = ?;");

    public VoltTable[] run(String key, byte value[], byte setValue, long lockTxnId, long expireTime)
    {
        final long now = getTransactionTime().getTime();
        
        // Check whether the key exists
        voltQueueSQL(checkStmt, key);

        VoltTable rowTable = voltExecuteSQL()[0];
        // Does the row to be unlocked exist?
        if (rowTable.getRowCount() == 0) {
        	if (expireTime < now) {
	            /*
	             * Might be bad, might not, really depends.
	             */
	            setAppStatusCode(Constants.KEY_DOES_NOT_EXIST);
	            return null;
        	} else {
        		throw new VoltAbortException("Really shouldn't happen");
        	}
        } else {
            /*
             * Do some basic sanity checks to make sure that the thing is locked with the right lock
             * If it isn't at unlock it might not be important, it could be that you are racing
             * with someone to apply the journal updates.
             */
            rowTable.advanceRow();
            final long storeLockTxnId = rowTable.getLong(0);
            if (rowTable.wasNull()) {
            	if (expireTime < now) {
	                /*
	                 * It has already been unlocked. Nothing to do.
	                 */
	            	setAppStatusCode(Constants.ROW_UNLOCKED);
	                return null;
            	} else {
            		throw new VoltAbortException("Really shouldn't happen");
            	}
            }
            if (storeLockTxnId != lockTxnId) {
            	if (expireTime < now) {
	                /*
	                 * And someone else relocked it. Nothing to do.
	                 */
	            	setAppStatusCode(Constants.ROW_LOCKED);
	                return null;
            	} else {
            		throw new VoltAbortException("Really shouldn't happen");
            	}
            }

            /*
             * Unlock the row and update if requested
             */
            if (setValue == 1) {
                voltQueueSQL(unlockIncludingValueUpdateStmt, value, key);
            } else {
                voltQueueSQL(unlockStmt, key);
            }
        	setAppStatusCode(Constants.ROW_UNLOCKED);
            voltExecuteSQL(true);
            return null;
        }
    }
}