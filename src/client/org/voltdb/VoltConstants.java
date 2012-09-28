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

public class VoltConstants {
    // originally from org.voltdb.VoltDB
    public final static int DEFAULT_PORT = 21212;

    // originally from org.voltdb.ClientInterface
    // reasons a connection can fail
    public static final byte AUTHENTICATION_FAILURE = -1;
    public static final byte MAX_CONNECTIONS_LIMIT_ERROR = 1;
    public static final byte WIRE_PROTOCOL_TIMEOUT_ERROR = 2;
    public static final byte WIRE_PROTOCOL_FORMAT_ERROR = 3;
    public static final byte AUTHENTICATION_FAILURE_DUE_TO_REJOIN = 4;
    public static final byte EXPORT_DISABLED_REJECTION = 5;

    // originally from org.voltdb.JdbcDatabaseMetaDataGenerator
    public static final String JSON_PARTITION_PARAMETER = "partitionParameter";
    public static final String JSON_SINGLE_PARTITION = "singlePartition";
    public static final String JSON_READ_ONLY = "readOnly";
}
