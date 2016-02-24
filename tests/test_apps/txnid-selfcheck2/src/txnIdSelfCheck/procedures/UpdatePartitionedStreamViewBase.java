/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public class UpdatePartitionedStreamViewBase extends VoltProcedure {

//    public final SQLStmt d_getCount = new SQLStmt(
//            "SELECT count(*) FROM dimension where cid = ?;");
//
//    // join partitioned tbl to replicated tbl. This enables detection of some replica faults.
//    public final SQLStmt p_getCIDData = new SQLStmt(
//            "SELECT * FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY p.cid, p.rid desc;");
//
//    public final SQLStmt p_cleanUp = new SQLStmt(
//            "DELETE FROM partitioned WHERE cid = ? and cnt < ?;");
//
//    public final SQLStmt p_getAdhocData = new SQLStmt(
//            "SELECT * FROM adhocp ORDER BY ts DESC, id LIMIT 1");
//
//    public final SQLStmt p_insert = new SQLStmt(
//            "INSERT INTO partitioned VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
//
//    public final SQLStmt p_export = new SQLStmt(
//            "INSERT INTO partitioned_export VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final SQLStmt p_getViewData = new SQLStmt(
            "SELECT * FROM partview WHERE cid=? ORDER BY cid DESC;");

    public final SQLStmt p_getExViewData = new SQLStmt(
            "SELECT * FROM ex_partview WHERE cid=? ORDER BY cid DESC;");

    public final SQLStmt p_getExViewShadowData = new SQLStmt(
            "SELECT * FROM ex_partview_shadow WHERE cid=? ORDER BY cid DESC;");

    public final SQLStmt p_upsertExViewShadowData = new SQLStmt(
            "UPSERT INTO ex_partview_shadow VALUES(?, ?, ?, ?, ?);");

    public final SQLStmt p_deleteExViewData = new SQLStmt(
            "DELETE FROM ex_partview WHERE cid=?;");

    public final SQLStmt p_deleteExViewShadowData = new SQLStmt(
            "DELETE FROM ex_partview_shadow WHERE cid=?;");

    public final SQLStmt p_updateExViewData = new SQLStmt(
            "UPDATE ex_partview SET entries = ?, minimum = ?, maximum = ?, summation = ? WHERE cid=?;");

    public final SQLStmt p_updateExViewShadowData = new SQLStmt(
            "UPDATE ex_partview_shadow SET entries = ?, minimum = ?, maximum = ?, summation = ? WHERE cid=?;");

    public long run() {
        return 0; // never called in base procedure
    }

    protected VoltTable[] doWork(SQLStmt getViewData, byte cid, long rid,
            byte[] value, byte shouldRollback, boolean usestreamviews)
    {

        long cnt = 0;
        voltQueueSQL(getViewData, cid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];

        // get the most recent row's data
        int rowCount = data.getRowCount();
        if (rowCount != 0) {
            VoltTableRow row = data.fetchRow(0);
            cnt = row.getLong("entries") + 1;
        }

            validateView(cid, cnt, "insert");

        // update export materialized view & validate
        int someData = 5;
        voltQueueSQL(p_updateExViewData, someData, someData, someData+1, someData+2, cid);
        voltQueueSQL(p_updateExViewShadowData, someData, someData, someData+1, someData+2, cid);
        voltExecuteSQL();
        validateView(cid, cnt, "update");

        // delete from export materialized view & validate
        voltQueueSQL(p_deleteExViewData, cid);
        voltQueueSQL(p_deleteExViewShadowData, cid);
        voltExecuteSQL();
        validateView(cid, cnt, "delete");

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }

        return results;
    }

    protected void validateView(byte cid, long cnt, String type) {
        // we've inserted a row in the export (streaming) table.
        // now pull derived data from the materialized view for
        // checking that it's been updated
        voltQueueSQL(p_getExViewData, cid);
        voltQueueSQL(p_getExViewShadowData, cid);
        VoltTable[] streamresults = voltExecuteSQL();
        validateStreamData(type, streamresults[0], streamresults[1], cid, cnt);
    }

    private void validateStreamData(String type, VoltTable exview, VoltTable shadowview, byte cid, long cnt) {
        if (type == "delete") {
            if (exview.getRowCount() == 0) {
                return;      // success
            } else {
                throw new VoltAbortException("Export view has "+exview.getRowCount()+" rows for this id. Zero expected after delete");
            }
        }

        if (exview.getRowCount() != 1)
            throw new VoltAbortException("Export view has "+exview.getRowCount()+" entries of the same cid, that should not happen. Type: "+type);
        VoltTableRow row0 = exview.fetchRow(0);
        long v_entries = row0.getLong("entries");
        long v_max = row0.getLong("maximum");
        long v_min = row0.getLong("minimum");
        long v_sum = row0.getLong("summation");

        if (shadowview.getRowCount() == 1) {
            row0 = shadowview.fetchRow(0);
            long shadow_entries = row0.getLong("entries");
            long shadow_max = row0.getLong("maximum");
            long shadow_min = row0.getLong("minimum");
            long shadow_sum = row0.getLong("summation");

            // adjust the shadow values for updated cnt, not done for "update"
            if (type == "insert") {
                shadow_entries++;
                shadow_max = Math.max(shadow_max, v_max);
                shadow_min = Math.min(shadow_min, v_min);
                shadow_sum += cnt;
            }

            if (v_entries != shadow_entries)
                throw new VoltAbortException("View entries:" + v_entries +
                        " materialized view aggregation does not match the number of shadow entries:" + shadow_entries + " for cid:" + cid + ". Type: " + type);

            if (v_max != shadow_max)
                throw new VoltAbortException("View v_max:" + v_max +
                        " materialized view aggregation does not match the shadow max:" + shadow_max + " for cid:" + cid + ". Type: " + type);

            if (v_min != shadow_min)
                throw new VoltAbortException("View v_min:" + v_min +
                        " materialized view aggregation does not match the shadow min:" + shadow_min + " for cid:" + cid + ". Type: " + type);

            if (v_sum != shadow_sum)
                throw new VoltAbortException("View v_sum:" + v_sum +
                        " materialized view aggregation does not match the shadow sum:" + shadow_sum + " for cid:" + cid + ". Type: " + type);

            voltQueueSQL(p_upsertExViewShadowData, cid, shadow_entries, shadow_max, shadow_min, shadow_sum);
        } else {
            // first time through, get initial values into the shadow table
            voltQueueSQL(p_upsertExViewShadowData, cid, v_entries, v_max, v_min, v_sum);
        }
        // update the shadow table with the new matching values and return
        voltExecuteSQL();
        return;
    }
}
