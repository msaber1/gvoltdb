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

import java.io.IOException;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Procedure;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.pmsg.DTXN;
import org.voltdb.utils.LogKeys;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * FragmentWriteProc and its bretheren (or sisteren) are subclasses
 * of VoltProcedure that do fragment work on behalf of a larger
 * (and presumably multi-partition) transaction. Unlike most procedures,
 * they are not complete transactions.
 *
 * Note: this functionality is identical to existing ExecutionSite
 * functionality, but with a different interface (i.e. a procedure).
 * The aim is to eventually replace that code in the E.S..
 *
 */
public class FragmentWriteProc extends VoltProcedure {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    protected ExecutionEngine m_ee = null;
    protected SiteProcedureConnection m_site = null;
    protected ProcedureRunner m_runner = null;
    static final Procedure CAT_PROC;

    static {
        CAT_PROC = new Procedure();
        CAT_PROC.setClassname(FragmentWriteProc.class.getName());
        CAT_PROC.setReadonly(false);
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

    @Override
    void init(ProcedureRunner runner) {
        super.init(runner);
        m_runner = runner;
    }

    void initFragmentProc(ExecutionEngine ee, SiteProcedureConnection site) {
        m_ee = ee;
        m_site = site;
    }

    public VoltTable[] run(long partitionId,
                           long determinismTxnId,
                           byte finishTransaction,
                           byte rollbackProgress,
                           long[] fragmentIds,
                           byte[] paramData,
                           byte[] depData)
    throws VoltAbortException
    {
        assert(m_runner != null);
        assert(m_ee != null);
        assert(m_site != null);
        assert(fragmentIds != null);
        assert((paramData == null) || (paramData.length > 0));
        assert((depData == null) || (depData.length > 0));
        assert((fragmentIds.length == 0) || (paramData != null));

        // return one table per fragment
        VoltTable[] retval = new VoltTable[fragmentIds.length];

        // ProtoBufs representation of dependencies and params
        DTXN.ParameterSet parameterSet = null;
        DTXN.DependencySet depSet = null;

        // deserialize the protobuf stuff and load dependencies in the ee
        try {
            // load dependencies into the EE
            if (depData != null) {
                depSet = DTXN.DependencySet.parseFrom(depData);
                m_ee.stashWorkUnitDependencies(depSet);
            }
            if (paramData != null) {
                parameterSet = DTXN.ParameterSet.parseFrom(paramData);
                assert(parameterSet.getParameterSetsCount() == fragmentIds.length);
            }
        }
        catch (InvalidProtocolBufferException e) {
            throw new VoltAbortException(e);
        }

        // do all the work in the EE
        for (int i = 0; i < fragmentIds.length; ++i) {

            // this is a horrible performance hack, and can be removed with small changes
            // to the ee interface layer.. (rtb: not sure what 'this' encompasses...)
            // (jhh: the previous two lines of comments are copied from the ExecutionSite)
            ParameterSet params = null;
            try {
                ByteString bs = parameterSet.getParameterSets(i);
                final FastDeserializer fds = new FastDeserializer(bs.asReadOnlyByteBuffer());
                params = fds.readObject(ParameterSet.class);
            }
            catch (final IOException e) {
                hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedDeserializingParamsForFragmentTask.name(), e);
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // get the input id if there is one
            int inputDepId = -1;
            if (depSet != null) {
                inputDepId = depSet.getDependencies(i).getDependencyId();
            }

            /*
             * Currently the error path when executing plan fragments
             * does not adequately distinguish between fatal errors and
             * abort type errors that should result in a roll back.
             * Assume that it is ninja: succeeds or doesn't return.
             * No roll back support.
             */
            retval[i] = m_site.executePlanFragment(fragmentIds[i],
                                                   inputDepId,
                                                   params);
        }

        return retval;
    }
}
