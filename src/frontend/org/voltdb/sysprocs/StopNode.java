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
            final VoltTable result = constructResultTable();
            final String shid = (String) params.toArray()[0];
            int ihid;
            try {
                ihid = Integer.parseInt(shid);
            } catch (NumberFormatException nfe) {
                return new DependencyPair(DEP_DISTRIBUTE, result);
            }
            // Choose the lowest site ID on this host to do the killing
            // All other sites should just return empty results tables.
            int hid = VoltDB.instance().getHostMessenger().getHostId();
            if (hid == ihid && context.isLowestSiteId()) {
                stopServerNode(result, ihid);
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
        VoltSystemProcedure.SynthesizedPlanFragment spf[] = null;

        spf = new VoltSystemProcedure.SynthesizedPlanFragment[2];

        spf[0] = new VoltSystemProcedure.SynthesizedPlanFragment();
        spf[0].fragmentId = SysProcFragmentId.PF_stopNode;
        spf[0].outputDepId = DEP_DISTRIBUTE;
        spf[0].inputDepIds = new int[]{};
        spf[0].multipartition = true;
        spf[0].parameters = ParameterSet.fromArrayWithCopy(selector);

        spf[1] = new VoltSystemProcedure.SynthesizedPlanFragment();
        spf[1].fragmentId = SysProcFragmentId.PF_stopNodeAggregate;
        spf[1].outputDepId = DEP_AGGREGATE;
        spf[1].inputDepIds = new int[]{DEP_DISTRIBUTE};
        spf[1].multipartition = false;
        spf[1].parameters = ParameterSet.emptyParameterSet();

        return executeSysProcPlanFragments(spf, DEP_AGGREGATE);
    }

    public static VoltTable constructResultTable() {
        return new VoltTable(
                new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID,
                        VoltSystemProcedure.CTYPE_ID),
                new ColumnInfo("RESULT", VoltType.STRING),
                new ColumnInfo("ERR_MSG", VoltType.STRING));
    }

    private void stopServerNode(VoltTable result, int hid) {
        if (!VoltDB.instance().isSafeToSuicide()) {
            hostLog.info("Its unsafe to shutdown node with hostId: " + hid
                    + " StopNode is will not stop node as stopping will violate k-safety.");
            result.addRow(VoltDB.instance().getHostMessenger().getHostId(), "FAILED",
                    "Server Node can not be stopped because stopping will violate k-safety.");
            return;
        }
        result.addRow(VoltDB.instance().getHostMessenger().getHostId(), "SUCCESS", "");

        Thread shutdownThread = new Thread() {
            @Override
            public void run() {
                //Check if I am supposed to die and its safe to die then die.
                boolean die = false;
                try {
                    Thread.sleep(5000);
                    die = VoltDB.instance().shutdown(this);
                } catch (InterruptedException e) {
                    hostLog.error("Exception while attempting to shutdown VoltDB from StopNode sysproc", e);
                }
                if (die) {
                    hostLog.warn("VoltDB node shutting down as requested by @StopNode command.");
                    System.exit(0);
                } else {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        shutdownThread.start();
    }
}
