/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package org.voltdb;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class TestProc {
    public static void main(String[] args) throws Exception {
        String          hostname            = null;
        String          alertSeverity       = null;
        String          alertStatus[]       = null;
        int             sourceTopologyId[]  = null;
        TimestampType   fromDate            = null;
        TimestampType   toDate              = null;
        String          categories[]        = null;
        String          source              = null;
        int             start               = -1;
        int             limit               = -1;
        /*
         * Instantiate a client and connect to the database.
         */
        for (int argc = 0; argc < args.length; argc += 1) {
            if ("-H".equals(args[argc])) {
                argc += 1;
                hostname = stringArg(args, argc);
            } else if ("--alertSeverity".equals(args[argc])) {
                argc += 1;
                alertSeverity = stringArg(args, argc);
            } else if ("--alertStatus".equals(args[argc])) {
                argc += 1;
                alertStatus = stringArrayArg(args, argc);
            } else if ("--source".equals(args[argc])) {
                argc += 1;
                source = stringArg(args, argc);
            } else if ("--sourceTopologyId".equals(args[argc])) {
                argc += 1;
                sourceTopologyId = intArrayArg(args, argc);
            } else if ("--fromDate".equals(args[argc])) {
                argc += 1;
                fromDate = timestampArg(args, argc);
            } else if ("--toDate".equals(args[argc])) {
                argc += 1;
                toDate = timestampArg(args, argc);
            } else if ("--categories".equals(args[argc])) {
                argc += 1;
                categories = stringArrayArg(args, argc);
            } else if ("--start".equals(args[argc])) {
                argc += 1;
                start = intArg(args, argc);
            } else if ("--limit".equals(args[argc])) {
                argc += 1;
                limit = intArg(args, argc);
            } else {
                System.err.printf("Unknown command line parameter: \"%s\"\n",
                                  args[argc]);
                System.exit(100);
            }

        }
        if (hostname == null) {
            System.err.printf("Need a hostname.\n");
            System.exit(100);
        } else if (start < 0) {
            System.err.printf("Start is negative.\n");
            System.exit(100);
        } else if (limit < 0) {
            System.err.printf("Limit it negative.\n");
            System.exit(100);
        }
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        myApp.createConnection(hostname);
        /*
         * Load the database.
         */
        ClientResponse cr = myApp.callProcedure("t_alert_select_alert_criteria",
                                                alertSeverity,
                                                alertStatus,
                                                sourceTopologyId,
                                                fromDate,
                                                toDate,
                                                categories,
                                                source,
                                                start,
                                                limit);
        if (ClientResponse.SUCCESS != cr.getStatus()) {
            System.out.printf("Failed: Client Response is %d\n", cr.getStatus());
            return;
        }
        VoltTable vt = cr.getResults()[0];
        System.out.printf("Table:\n%s\n", vt.toString());
    }

    private static boolean isNull(String arg) {
        return "null".equals(arg);
    }

    private static String getArg(String[] args, int argc) {
        return args[argc];
    }

    private static int intArg(String[] args, int argc) {
        String arg = getArg(args, argc);
        return Integer.valueOf(arg);
    }

    private static TimestampType timestampArg(String[] args, int argc) {
        String arg = getArg(args, argc);
        if (isNull(arg)) {
            return null;
        }
        return null;
    }

    private static String[] stringArrayArg(String[] args, int argc) {
        String arg = getArg(args, argc);
        if (isNull(arg)) {
            return null;
        }
        String strings[] = arg.split("\\.");
        List<String> l = new ArrayList<>();
        for (String str : strings) {
            if (str.length() > 0) {
                l.add(str);
            }
        }
        strings = new String[l.size()];
        for (int idx = 0; idx < l.size(); idx += 1) {
            strings[idx] = l.get(idx);
        }
        return strings;
    }

    private static int[] intArrayArg(String[] args, int argc) {
        String arg = getArg(args, argc);
        if (isNull(arg)) {
            return null;
        }
        String[] strings = stringArrayArg(args, argc);
        int  answer[] = new int[strings.length];
        for (int idx = 0; idx < strings.length; idx += 1) {
            answer[idx] = Integer.parseInt(strings[idx]);
        }
        return answer;
    }

    private static String stringArg(String[] args, int argc) {
        String arg = getArg(args, argc);
        if (isNull(arg)) {
            return null;
        }
        return arg;
    }

}
