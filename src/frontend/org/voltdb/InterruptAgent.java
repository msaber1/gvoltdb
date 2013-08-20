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
package org.voltdb;

import java.util.ArrayList;
import java.util.HashMap;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltcore.utils.Pair;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;

/**
 * Agent responsible for collecting stats on this host.
 */
public class InterruptAgent extends OpsAgent
{
    private final HashMap<StatsSelector, HashMap<Long, ArrayList<StatsSource>>> registeredStatsSources =
        new HashMap<StatsSelector, HashMap<Long, ArrayList<StatsSource>>>();

    public InterruptAgent()
    {
        super("InterruptAgent");
        StatsSelector selectors[] = StatsSelector.values();
        for (int ii = 0; ii < selectors.length; ii++) {
            registeredStatsSources.put(selectors[ii], new HashMap<Long, ArrayList<StatsSource>>());
        }
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
            ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", "INTERRUPT");
        // parseParamsForStatistics has a clumsy contract, see definition
        String err = null;
        if (selector == OpsSelector.INTERRUPT) {
            err = parseParamsForInterrupt(params, obj);
        }
        else {
            err = "InterruptAgent received non-INTERRUPT selector: " + selector.name();
        }
        if (err != null) {
            sendErrorResponse(c, ClientResponse.GRACEFUL_FAILURE, err, clientHandle);
            return;
        }
        String subselector = obj.getString("subselector");

        PendingOpsRequest psr =
            new PendingOpsRequest(
                    selector,
                    subselector,
                    c,
                    clientHandle,
                    System.currentTimeMillis());
        distributeOpsWork(psr, obj);
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParamsForInterrupt(ParameterSet params, JSONObject obj) throws Exception
    {
        if ((params.toArray().length != 1)) {
            return "Incorrect number of arguments to @Interrupt (expects 1, received " +
                    params.toArray().length + ")";
        }

        long uniqueId = ((Number)(params.toArray()[0])).longValue();
        obj.put("subselector", "INTERRUPT");
        obj.put("uniqueId", uniqueId);

        return null;
    }

    private VoltTable[] setUniqueIdToInterrupt(JSONObject obj) throws JSONException {
        long uniqueId = obj.getLong("uniqueId");
        VoltDB.instance().setUniqueIdToInterrupt(uniqueId);
        VoltTable result = new VoltTable(new ColumnInfo("Look for further information", VoltType.STRING));
        VoltTable[] results = {result};
        return results;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        VoltTable[] results = null;

        OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
        if (selector == OpsSelector.INTERRUPT) {
            results = setUniqueIdToInterrupt(obj);
        }
        else {
            hostLog.warn("InterruptAgent received a non-INTERRUPT OPS selector: " + selector);
        }

        sendOpsResponse(results, obj);
    }
}
