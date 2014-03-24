/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltSystemProcedure;
import static org.voltdb.VoltSystemProcedure.CNAME_HOST_ID;
import static org.voltdb.VoltSystemProcedure.CTYPE_ID;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.VoltTableUtil;

@ProcInfo(
        singlePartition = false
)

public class StopNode extends VoltSystemProcedure {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    static final int DEP_DISTRIBUTE = (int) SysProcFragmentId.PF_stopNode | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_AGGREGATE = (int) SysProcFragmentId.PF_stopNodeAggregate;

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_stopNode);
        registerPlanFragment(SysProcFragmentId.PF_stopNodeAggregate);
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
            long fragmentId,
            ParameterSet params,
            SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_stopNode) {
            VoltTable result = null;
            // Choose the lowest site ID on this host to do the killing
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId()) {
                result = stopServerNode();
            } else {
                result = new VoltTable(
                        new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                        new ColumnInfo("KEY", VoltType.STRING),
                        new ColumnInfo("VALUE", VoltType.STRING));
            }
            return new DependencyPair(DEP_DISTRIBUTE, result);
        } else if (fragmentId == SysProcFragmentId.PF_stopNodeAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_DISTRIBUTE));
            return new DependencyPair(DEP_AGGREGATE, result);
        }
        assert (false);
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String selector) throws VoltProcedure.VoltAbortException {
        VoltTable[] results = new VoltTable[1];

        // This selector provides the old @SystemInformation behavior
        int hid = Integer.parseInt(selector);

        boolean flag = VoltDB.instance().getHostMessenger().getHostId() == hid;
        if (flag) {
            return stopAndGetClusterStatus();
        }

        VoltTable table = constructOverviewTable();
        table.addRow(VoltDB.instance().getHostMessenger().getHostId(), "STATUS", "UP");
        results[0] = table;

        return results;
    }

    private VoltTable[] stopAndGetClusterStatus() {

        VoltSystemProcedure.SynthesizedPlanFragment spf[] = null;

        spf = new VoltSystemProcedure.SynthesizedPlanFragment[2];

        int idx = 0;
        spf[idx] = new VoltSystemProcedure.SynthesizedPlanFragment();
        spf[idx].fragmentId = SysProcFragmentId.PF_stopNode;
        spf[idx].outputDepId = DEP_DISTRIBUTE;
        spf[idx].inputDepIds = new int[]{};
        spf[idx].multipartition = true;
        spf[idx].parameters = ParameterSet.emptyParameterSet();
        idx++;

        spf[idx] = new VoltSystemProcedure.SynthesizedPlanFragment();
        spf[idx].fragmentId = SysProcFragmentId.PF_stopNodeAggregate;
        spf[idx].outputDepId = DEP_AGGREGATE;
        spf[idx].inputDepIds = new int[]{DEP_DISTRIBUTE};
        spf[idx].multipartition = false;
        spf[idx].parameters = ParameterSet.emptyParameterSet();

        return executeSysProcPlanFragments(spf, DEP_AGGREGATE);
    }

    public static VoltTable constructOverviewTable() {
        return new VoltTable(
                new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID,
                        VoltSystemProcedure.CTYPE_ID),
                new ColumnInfo("KEY", VoltType.STRING),
                new ColumnInfo("VALUE", VoltType.STRING));
    }

    private VoltTable stopServerNode() {
        final VoltTable table = constructOverviewTable();
        if (!VoltDB.instance().isSafeToSuicide()) {
            int hid = VoltDB.instance().getHostMessenger().getHostId();
            hostLog.info("Its unsafe to shutdown node with hostId: " + hid + " StopNode is will not stop node.");
            table.addRow(VoltDB.instance().getHostMessenger().getHostId(), "STATUS", "FORCED-TO-STAY-UP-FOR-KSAFETY");
            return table;
        }
        table.addRow(VoltDB.instance().getHostMessenger().getHostId(), "STATUS", "STOPPING");

        Thread shutdownThread = new Thread() {
            @Override
            public void run() {
                //Check if I am supposed to die and its safe to die then die.
                if (VoltDB.instance().isSafeToSuicide()) {
                    boolean die = false;
                    try {
                        die = VoltDB.instance().shutdown(this);
                    } catch (InterruptedException e) {
                        hostLog.error(
                                "Exception while attempting to shutdown VoltDB from StopNode sysproc",
                                e);
                    }
                    if (die) {
                        hostLog.warn("VoltDB shutting down as requested by @StopNode command.");
                        System.exit(0);
                    } else {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                        }
                    }
                } else {
                    //Send error.
                }
            }
        };
        shutdownThread.start();

        return table;
    }
}
