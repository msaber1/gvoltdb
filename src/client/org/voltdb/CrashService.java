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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.PlatformProperties;

public class CrashService {

    /*
     * For tests that causes failures,
     * allow them stop the crash and inspect.
     */
    public static boolean ignoreCrash = false;

    public static boolean wasCrashCalled = false;

    public static String crashMessage;

    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown) {
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }

        List<String> throwerStacktrace = null;
        if (thrown != null) {
            throwerStacktrace = new ArrayList<String>();
            throwerStacktrace.add("Stack trace of thrown exception: " + thrown.toString());
            for (StackTraceElement ste : thrown.getStackTrace()) {
                throwerStacktrace.add(ste.toString());
            }
        }

        // Even if the logger is null, don't stop.  We want to log the stack trace and
        // any other pertinent information to a .dmp file for crash diagnosis
        List<String> currentStacktrace = new ArrayList<String>();
        currentStacktrace.add("Stack trace from crashLocalVoltDB() method:");
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        StackTraceElement[] myTrace = traces.get(Thread.currentThread());
        for (StackTraceElement ste : myTrace) {
            currentStacktrace.add(ste.toString());
        }

        // Create a special dump file to hold the stack trace
        try
        {
            TimestampType ts = new TimestampType(new java.util.Date());
            //CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
            //String root = catalogContext != null ? catalogContext.cluster.getVoltroot() + File.separator : "";
            String root = "";
            PrintWriter writer = new PrintWriter(root + "voltdb_crash" + ts.toString().replace(' ', '-') + ".txt");
            writer.println("Time: " + ts);
            writer.println("Message: " + errMsg);

            writer.println();
            writer.println("Platform Properties:");
            PlatformProperties pp = PlatformProperties.getPlatformProperties();
            String[] lines = pp.toLogLines().split("\n");
            for (String line : lines) {
                writer.println(line.trim());
            }

            if (thrown != null) {
                writer.println();
                writer.println("****** Exception Thread ****** ");
                for (String throwerStackElem : throwerStacktrace) {
                    writer.println(throwerStackElem);
                }
            }

            writer.println();
            writer.println("****** Current Thread ****** ");
            for (String currentStackElem : currentStacktrace) {
                writer.println(currentStackElem);
            }

            writer.println("****** All Threads ******");
            Iterator<Thread> it = traces.keySet().iterator();
            while (it.hasNext())
            {
                Thread key = it.next();
                writer.println();
                StackTraceElement[] st = traces.get(key);
                writer.println("****** " + key + " ******");
                for (StackTraceElement ste : st)
                    writer.println(ste);
            }
            writer.close();
        }
        catch (Throwable err)
        {
            // shouldn't fail, but..
            err.printStackTrace();
        }

        VoltLogger log = null;
        try
        {
            log = new VoltLogger("HOST");
        }
        catch (RuntimeException rt_ex)
        { /* ignore */ }

        if (log != null)
        {
            log.fatal(errMsg);
            if (thrown != null) {
                if (stackTrace) {
                    for (String throwerStackElem : throwerStacktrace) {
                        log.fatal(throwerStackElem);
                    }
                } else {
                    log.fatal(thrown.toString());
                }
            } else {
                if (stackTrace) {
                    for (String currentStackElem : currentStacktrace) {
                        log.fatal(currentStackElem);
                    }
                }
            }
        } else {
            System.err.println(errMsg);
            if (thrown != null) {
                if (stackTrace) {
                    for (String throwerStackElem : throwerStacktrace) {
                        System.err.println(throwerStackElem);
                    }
                } else {
                    System.err.println(thrown.toString());
                }
            } else {
                if (stackTrace) {
                    for (String currentStackElem : currentStacktrace) {
                        System.err.println(currentStackElem);
                    }
                }
            }
        }

        System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
        System.err.println("The log may contain additional information.");
        System.exit(-1);
    }
}
