/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.voltdb.catalog.Procedure;

/**
 * This procedure has identical functionality to FragmentWriteProc,
 * but the catalog procedure advertises it as read-only. This is
 * useful for transaction management.
 *
 */
public class FragmentReadProc extends FragmentWriteProc {

    static final Procedure CAT_PROC;

    static {
        CAT_PROC = new Procedure();
        CAT_PROC.setClassname(FragmentReadProc.class.getName());
        CAT_PROC.setReadonly(true); // key diff from write proc
        CAT_PROC.setEverysite(false);
        CAT_PROC.setSinglepartition(true);
        CAT_PROC.setSystemproc(false);
        CAT_PROC.setHasjava(true);
        CAT_PROC.setPartitiontable(null);
        CAT_PROC.setPartitioncolumn(null);
        CAT_PROC.setPartitionparameter(0);
    }

    public static Procedure asCatalogProcedure() {
        return CAT_PROC;
    }

    public VoltTable[] run(long partitionId,
            long determinismTxnId,
            byte finishTransaction,
            byte rollbackProgress,
            long[] fragmentIds,
            byte[] paramData,
            byte[] depData)
    throws VoltAbortException {

        return super.run(partitionId,
                         determinismTxnId,
                         finishTransaction,
                         rollbackProgress,
                         fragmentIds,
                         paramData,
                         depData);
    }
}
