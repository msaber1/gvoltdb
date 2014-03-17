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
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

/**
 * A wholly improper shutdown. No promise is given to return a result to a client,
 * to finish work queued behind this procedure or to return meaningful errors for
 * those queued transactions.
 *
 * Invoking this procedure immediately attempts to terminate each node in the cluster.
 */
@ProcInfo(singlePartition = false)
public class StopNode extends VoltSystemProcedure {

    private static final int DEP_stopDone = (int) SysProcFragmentId.PF_stopNode;

    private static AtomicBoolean m_failsafeArmed = new AtomicBoolean(false);
    private static Thread m_failsafe = new Thread() {
        @Override
        public void run() {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {}
            new VoltLogger("HOST").warn("VoltDB shutting down as requested by @Shutdown command.");
            System.exit(0);
        }
    };

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_stopNode);
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                           long fragmentId,
                                           ParameterSet params,
                                           SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_stopNode) {
            final String hostId = (String) params.toArray()[0];
            final int hid = Integer.parseInt(hostId);

            Thread shutdownThread = new Thread() {
                @Override
                public void run() {
                    //Check if I am supposed to die and its safe to die then die.
                    if (VoltDB.instance().getHostMessenger().getHostId() == hid) {
                        //Die
                        if (VoltDB.instance().isSafeToSuicide()) {
                            boolean die = false;
                            try {
                                die = VoltDB.instance().shutdown(this);
                            } catch (InterruptedException e) {
                                new VoltLogger("HOST").error(
                                        "Exception while attempting to shutdown VoltDB from shutdown sysproc",
                                        e);
                            }
                            if (die) {
                                new VoltLogger("HOST").warn("VoltDB shutting down as requested by @Shutdown command.");
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
                }
            };
            shutdownThread.start();
        }
        return null;
    }

    /**
     * Begin an un-graceful shutdown.
     * @param ctx Internal parameter not exposed to the end-user.
     * @return Never returned, no he never returned...
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_stopNode;
        pfs[0].outputDepId = DEP_stopDone;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        executeSysProcPlanFragments(pfs, (int) SysProcFragmentId.PF_procedureDone);
        return new VoltTable[0];
    }
}
