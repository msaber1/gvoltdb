/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package lr.procedures;

import java.lang.Math;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

public class Solve extends VoltProcedure
{
    //public final SQLStmt selectAllStmt = new SQLStmt("SELECT * FROM aus ORDER BY id limit ? offset ?;");
    public final SQLStmt selectAllStmt = new SQLStmt("SELECT * FROM aus;");
    //public final SQLStmt selectWeightsStmt = new SQLStmt("SELECT * FROM weights;");

    // use all data in a single partition to calculate grad, using all weights
    // TODO: 1. add l2/l1 regularization 2. use partial weights
    public VoltTable run(int id, double[] weights, double stepsize)
    {
        voltQueueSQL(selectAllStmt);
        //voltQueueSQL(selectWeightsStmt);
        VoltTable[] tables = voltExecuteSQL();
        VoltTable table = tables[0];
        VoltTable gradTable = new VoltTable(new VoltTable.ColumnInfo("value", VoltType.FLOAT));
        //VoltTable gradTable = tables[1];
        int len = weights.length;
        double[] data = new double[len];
        double[] grad = new double[len];

        VoltTableRow row = table.fetchRow(0);
        row.resetRowPosition();
        while (row.advanceRow())
        {
            // the first field is id, skip it
            int flag = (int)row.getLong(1);
            data[0] = 1;
            for (int i=1; i<len; i++)
                data[i] = row.getDouble(i+1);

            double t = (sigmoid(flag, weights, data)) - 1.0;
            t = (flag == 1) ? t : -t;

            for (int i=0; i<len; i++)
                grad[i] += data[i] * t;
        }

        for (int i=0; i<len; i++) {
            grad[i] *= stepsize;
            gradTable.addRow(grad[i]);
        }

        //gradTable.addRow(grad[0], grad[1], grad[2], grad[3], grad[4], grad[5], grad[6], grad[7], grad[7], grad[8], grad[9], grad[10], grad[11], grad[12], grad[13]);

        return gradTable;
    }

    // TODO: change flag's type to int
    private double sigmoid(int flag, double[] weights, double[] data)
    {
        int len = weights.length;
        double s = 0.0;

        for(int i=0;i<len;i++)
            s += weights[i] * data[i];
        s = (flag == 1) ? s : -s;
        //s *= flag;

        return 1.0 / (1 + Math.exp(-s));
    }
}
