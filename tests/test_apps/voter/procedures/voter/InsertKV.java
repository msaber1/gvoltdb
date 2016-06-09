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
package voter;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.DeprecatedProcedureAPIAccess;

import voter.SampleRecord;

public class InsertKV extends VoltProcedure {
    public final SQLStmt export = new SQLStmt("INSERT INTO kv_stream (seq, event_date) " +
            "VALUES (?, ?)");
    public final SQLStmt mirror_export = new SQLStmt("INSERT INTO kv_stream_mirror (seq, event_date) " +
            "VALUES (?, ?)");

    public long run(long seq)
    {
        @SuppressWarnings("deprecation")
        long txid = DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this);

        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = new Random(txid);

        // Insert a new record
        SampleRecord record = new SampleRecord(seq, 16, rand);

        voltQueueSQL(
                export
                , seq
                , record.event_date
        );
        voltQueueSQL(
                mirror_export
                , seq
                , record.event_date
        );

        // Execute last statement batch
        voltExecuteSQL(true);

        // Retun to caller
        return 0;
    }
}
