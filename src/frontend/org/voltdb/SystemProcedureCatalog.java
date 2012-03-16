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

import java.util.HashMap;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Procedure;


/**
 * Lists built-in system stored procedures with metadata
 */
public class SystemProcedureCatalog {

    // Historical note:

    // We used to list syprocs in the catalog (inserting them in
    // VoltCompiler). That adds unnecessary content to catalogs,
    // couples catalogs to versions (an old catalog wouldn't be able
    // to invoke a new sysprocs), and complicates the idea of
    // commercial-only sysprocs.

    // Now we maintain this list here in code. This is also not
    // roses - as ProcedureWrapper really wants catalog.Procedures
    // and ClientInterface has to check two lists at dispatch
    // time.

    /* Data about each registered procedure */
    public static class Config {
        public final String className;
        public final boolean readOnly;
        public final boolean everySite;
        public final boolean commercial;
        // whether this procedure will terminate replication
        public final boolean terminatesReplication;
        // whether this procedure should be skipped in replication
        public final boolean skipReplication;
        // whether normal clients can call this sysproc in secondary
        public final boolean allowedInReplica;
        // Advertisement stuff below here
        public final boolean advertise;
        public final String name;
        public final VoltType[] paramTypes;
        public final String helpString;

        public Config(String className,
                boolean readOnly,
                boolean everySite,
                boolean commercial,
                boolean terminatesReplication,
                boolean skipReplication,
                boolean allowedInReplica,
                boolean advertise,
                String name,
                VoltType[] paramTypes,
                String helpString)
        {
            this.className = className;
            this.readOnly = readOnly;
            this.everySite = everySite;
            this.commercial = commercial;
            this.terminatesReplication = terminatesReplication;
            this.skipReplication = skipReplication;
            this.allowedInReplica = allowedInReplica;
            this.advertise = advertise;
            this.name = name;
            this.paramTypes = paramTypes;
            this.helpString = helpString;
        }

        boolean getEverysite() {
            return everySite;
        }

        boolean getReadonly() {
            return readOnly;
        }

        boolean getSinglepartition() {
            return className.equals("org.voltdb.sysprocs.UpdateApplicationCatalog");
        }

        String getClassname() {
            return className;
        }

        String getName() {
            return name;
        }

        boolean advertise() {
            return advertise;
        }

        VoltType[] getParamTypes() {
            return paramTypes;
        }

        Procedure asCatalogProcedure() {
            Procedure p = new Procedure();
            p.setClassname(className);
            p.setReadonly(readOnly);
            p.setEverysite(everySite);
            p.setSinglepartition(false);
            p.setSystemproc(true);
            p.setHasjava(true);
            p.setPartitiontable(null);
            p.setPartitioncolumn(null);
            p.setPartitionparameter(0);
            return p;
        }

        JSONObject serializeToJson() throws JSONException
        {
            JSONObject json = new JSONObject();
            json.put("procedure", name);
            json.put("parameters", new JSONArray());
            json.put("help", helpString);
            for (int i = 0; i < paramTypes.length; i++)
            {
                json.append("parameters", paramTypes[i].toSQLString());
            }
            return json;
        }
    }

    public static final HashMap<String, Config> listing =
        new HashMap<String, Config>();

    public static JSONObject serializeSysprocCatalogToJson() throws JSONException
    {
        JSONObject json = new JSONObject();
        json.put("sysprocs", new JSONArray());
        for (Config sysproc : listing.values())
        {
            if (sysproc.advertise())
            {
                json.append("sysprocs", sysproc.serializeToJson());
            }
        }
        return json;
    }

    static {
        listing.put("@AdHoc",
                    new Config("org.voltdb.sysprocs.AdHoc",
                               false, false, false, false, false, true,
                               true, "@AdHoc",
                               new VoltType[] {VoltType.STRING},
                               ""));
        listing.put("@AdHocSP",
                    new Config("org.voltdb.sysprocs.AdHocSP",
                               false, false, false, false, false, true,
                               true, "@AdHocSP",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.STRING},
                               ""));
        listing.put("@BalancePartitions",
                    new Config("org.voltdb.sysprocs.BalancePartitions",
                               false, false, true, true, true, false,
                               false, "@BalancePartitions",
                               new VoltType[] {},
                               ""));
        listing.put("@LoadMultipartitionTable",
                    new Config("org.voltdb.sysprocs.LoadMultipartitionTable",
                               false, false, false, false, false, false,
                               true, "@LoadMultipartitionTable",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.VOLTTABLE},
                               ""));
        listing.put("@LoadSinglepartitionTable",
                    new Config("org.voltdb.sysprocs.LoadSinglepartitionTable",
                               false, false, false, false, false, false,
                               true, "@LoadSinglePartitionTable",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.VOLTTABLE},
                               ""));
        listing.put("@Pause",
                    new Config("org.voltdb.sysprocs.Pause",
                               false, true,  false, false, true, true,
                               true, "@Pause",
                               new VoltType[] {},
                               ""));
        listing.put("@ProfCtl",
                    new Config("org.voltdb.sysprocs.ProfCtl",
                               false, false, false, false, true, true,
                               false, "@ProfCtl",
                               new VoltType[] {},
                               ""));
        listing.put("@Promote",
                    new Config("org.voltdb.sysprocs.Promote",
                               false, true, false, false, true, true,
                               true, "@Promote",
                               new VoltType[] {},
                               ""));
        listing.put("@Quiesce",
                    new Config("org.voltdb.sysprocs.Quiesce",
                               false, false, false, false, true, true,
                               true, "@Quiesce",
                               new VoltType[] {},
                               ""));
        listing.put("@Rejoin",
                    new Config("org.voltdb.sysprocs.Rejoin",
                               false, false, false, false, true, true,
                               true, "@Rejoin",
                               new VoltType[] {},
                               ""));
        listing.put("@Resume",
                    new Config("org.voltdb.sysprocs.Resume",
                               false, true,  false, false, true, true,
                               true, "@Resume",
                               new VoltType[] {},
                               ""));
        listing.put("@Shutdown",
                    new Config("org.voltdb.sysprocs.Shutdown",
                               false, false, false, false, true, true,
                               true, "@Shutdown",
                               new VoltType[] {},
                               ""));
        listing.put("@SnapshotDelete",
                    new Config("org.voltdb.sysprocs.SnapshotDelete",
                               false, false, false, false, true, true,
                               true, "@SnapshotDelete",
                               new VoltType[] {},
                               ""));
        listing.put("@SnapshotRestore",
                    new Config("org.voltdb.sysprocs.SnapshotRestore",
                               false, false, false, true, true, false,
                               true, "@SnapshotRestore",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.STRING},
                               ""));
        listing.put("@SnapshotSave",
                    new Config("org.voltdb.sysprocs.SnapshotSave",
                               false, false, false, false, true, true,
                               true, "@SnapshotSave",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.STRING,
                                               VoltType.INTEGER},
                               ""));
        listing.put("@SnapshotScan",
                    new Config("org.voltdb.sysprocs.SnapshotScan",
                               false, false, false, false, true, true,
                               true, "@SnapshotScan",
                               new VoltType[] {VoltType.STRING},
                               ""));
        listing.put("@SnapshotStatus",
                    new Config("org.voltdb.sysprocs.SnapshotStatus",
                               false, false, false, false, true, true,
                               true, "@SnapshotStatus",
                               new VoltType[] {},
                               ""));
        listing.put("@Statistics",
                    new Config("org.voltdb.sysprocs.Statistics",
                               true,  false, false, false, true, true,
                               true, "@Statistics",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.INTEGER},
                               ""));
        listing.put("@SystemCatalog",
                    new Config("org.voltdb.sysprocs.SystemCatalog",
                               true,  false, false, false, true, true,
                               true, "@SystemCatalog",
                               new VoltType[] {VoltType.STRING},
                               ""));
        listing.put("@SystemInformation",
                    new Config("org.voltdb.sysprocs.SystemInformation",
                               true,  false, false, false, true, true,
                               true, "@SystemInformation",
                               new VoltType[] {VoltType.STRING},
                               ""));
        listing.put("@UpdateApplicationCatalog",
                    new Config("org.voltdb.sysprocs.UpdateApplicationCatalog",
                               false, true,  false, true, true, false,
                               true, "@UpdateApplicationCatalog",
                               new VoltType[] {VoltType.STRING,
                                               VoltType.STRING},
                               ""));
        listing.put("@UpdateLogging",
                    new Config("org.voltdb.sysprocs.UpdateLogging",
                               false, true,  false, false, true, true,
                               true, "@UpdateLogging",
                               new VoltType[] {VoltType.STRING},
                               ""));
    }
}
