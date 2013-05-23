/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

@ProcInfo(singlePartition = true)
public class GetApplicationCatalog extends VoltSystemProcedure {

    @Override
    public void init() {}

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                              long fragmentId,
                                              ParameterSet params,
                                              SystemProcedureExecutionContext context) {
        throw new RuntimeException("@GetApplicationCatalog was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    /**
     * Handled fully in clientinterface. Must exist though for metadata reasons.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) throws Exception
    {
        throw new RuntimeException("@GetApplicationCatalog was actually called.");
    }
}
