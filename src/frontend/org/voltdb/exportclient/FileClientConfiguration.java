/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.exportclient;

import java.io.File;

import org.apache.commons.lang3.StringEscapeUtils;

public class FileClientConfiguration {

    private char delimiter = '\0';
    char delimiter() { return delimiter; }

    private char[] fullDelimiters;
    char[] fullDelimiters() { return fullDelimiters; }

    private final String extension;
    String extension() { return extension; }

    private String nonce = null;
    String nonce() { return nonce; }

    private File outDir = null;
    File outDir() { return outDir; }

    private int period = 60;
    int period() { return period; }

    private String dateFormatOriginalString = "yyyyMMddHHmmss";
    String dateFormatOriginalString() { return dateFormatOriginalString; }

    private int firstfield = 0;
    int firstField() { return firstfield; }

    private String user = null;
    String user() { return user; }

    private String password = null;
    String password() { return password; }

    private String[] volt_servers = null;
    String[] voltServers() { return volt_servers; }

    private char connect = ' '; // either ' ', 'c', or 'a'
    char connect() { return connect; }
    boolean useAdminPorts() { return connect == 'a'; }

    private boolean batched = false;
    boolean batched() { return batched; }

    private boolean withSchema = false;
    boolean withSchema() { return withSchema; }

    private int throughputMonitorPeriod = 0;
    int throughputMonitorPeriod() { return throughputMonitorPeriod; }

    private boolean autoDiscoverTopology = true;
    boolean autoDiscoverTopology() { return autoDiscoverTopology; }


    protected static void printHelpAndQuit(int code) {
        System.out.println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--help");
        System.out.println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--servers server1[,server2,...,serverN] "
                        + "--connect (admin|client) "
                        + "--type (csv|tsv) "
                        + "--nonce file_prefix "
                        + "[--batched] "
                        + "[--with-schema] "
                        + "[--period rolling_period_in_minutes] "
                        + "[--dateformat date_pattern_for_file_name] "
                        + "[--outdir target_directory] "
                        + "[--skipinternals] "
                        + "[--delimiters html-escaped delimiter set (4 chars)] "
                        + "[--user export_username] "
                        + "[--password export_password]");
        System.out.println("Note that server hostnames may be appended with a specific port:");
        System.out.println("  --servers server1:port1[,server2:port2,...,serverN:portN]");

        System.exit(code);
    }

    public FileClientConfiguration(String args[]) {

        String tmpDelimiters = null;

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
            }
            else if (arg.equals("--discard")) {
                System.err.println("Option \"--discard\" is no longer supported.");
                System.err.println("Try org.voltdb.exportclient.DiscardingExportClient.");
                printHelpAndQuit(-1);
            }
            else if (arg.equals("--skipinternals")) {
                firstfield = 6;
            }
            else if (arg.equals("--connect")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --connect");
                    printHelpAndQuit(-1);
                }
                String connectStr = args[ii + 1];
                if (connectStr.equalsIgnoreCase("admin")) {
                    connect = 'a';
                } else if (connectStr.equalsIgnoreCase("client")) {
                    connect = 'c';
                } else {
                    System.err.println("Error: --type must be one of \"admin\" or \"client\"");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--servers")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --servers");
                    printHelpAndQuit(-1);
                }
                volt_servers = args[ii + 1].split(",");
                ii++;
            }
            else if (arg.equals("--type")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --type");
                    printHelpAndQuit(-1);
                }
                String type = args[ii + 1];
                if (type.equalsIgnoreCase("csv")) {
                    delimiter = ',';
                } else if (type.equalsIgnoreCase("tsv")) {
                    delimiter = '\t';
                } else {
                    System.err.println("Error: --type must be one of CSV or TSV");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--outdir")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --outdir");
                    printHelpAndQuit(-1);
                }
                boolean invalidDir = false;
                outDir = new File(args[ii + 1]);
                if (!outDir.exists()) {
                    System.err.println("Error: " + outDir.getPath() + " does not exist");
                    invalidDir = true;
                }
                if (!outDir.canRead()) {
                    System.err.println("Error: " + outDir.getPath() + " does not have read permission set");
                    invalidDir = true;
                }
                if (!outDir.canExecute()) {
                    System.err.println("Error: " + outDir.getPath() + " does not have execute permission set");
                    invalidDir = true;
                }
                if (!outDir.canWrite()) {
                    System.err.println("Error: " + outDir.getPath() + " does not have write permission set");
                    invalidDir = true;
                }
                if (invalidDir) {
                    System.exit(-1);
                }
                ii++;
            }
            else if (arg.equals("--nonce")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --nonce");
                    printHelpAndQuit(-1);
                }
                nonce = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--user")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --user");
                    printHelpAndQuit(-1);
                }
                user = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--password")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --password");
                    printHelpAndQuit(-1);
                }
                password = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--period")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --period");
                    printHelpAndQuit(-1);
                }
                period = Integer.parseInt(args[ii + 1]);
                if (period < 1) {
                    System.err.println("Error: Specified value for --period must be >= 1.");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--dateformat")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --dateformat");
                    printHelpAndQuit(-1);
                }
                dateFormatOriginalString = args[ii + 1].trim();
                ii++;
            }
            else if (arg.equals("--batched")) {
                batched = true;
            }
            else if (arg.equals("--with-schema")) {
                withSchema = true;
            }
            else if (arg.equals("--delimiters")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --delimiters");
                    printHelpAndQuit(-1);
                }
                tmpDelimiters = args[ii + 1].trim();
                ii++;
                String charsAsStr = StringEscapeUtils.unescapeHtml4(tmpDelimiters.trim());
                if (charsAsStr.length() != 4) {
                    System.err.println("The delimiter set must contain exactly 4 characters (after any html escaping).");
                    printHelpAndQuit(-1);
                }
            }
            else if (arg.equals("--disable-topology-autodiscovery")) {
                autoDiscoverTopology = false;
            }
            else if (arg.equals("--throughput-monitor-period")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --throughput-monitor-period");
                    printHelpAndQuit(-1);
                }
                throughputMonitorPeriod = Integer.parseInt(args[ii + 1].trim());
                ii++;
            }
            else {
                System.err.println("Unrecognized parameter " + arg);
                System.exit(-1);
            }
        }
        // Check args for validity
        if (volt_servers == null || volt_servers.length < 1) {
            System.err.println("ExportToFile: must provide at least one VoltDB server");
            printHelpAndQuit(-1);
        }

        if (connect != 'a' && connect != 'c') {
            System.err.println("ExportToFile: must specify connection type as admin or client using --connect argument");
            printHelpAndQuit(-1);
        }

        if (user == null) {
            user = "";
        }
        if (password == null) {
            password = "";
        }
        if (nonce == null) {
            System.err.println("ExportToFile: must provide a filename nonce");
            printHelpAndQuit(-1);
        }
        if (outDir == null) {
            outDir = new File(".");
        }
        if (delimiter == '\0') {
            System.err.println("ExportToFile: must provide an output type");
            printHelpAndQuit(-1);
        }

        // Set conditional defaults
        extension = (delimiter == ',') ? ".csv" : ".tsv";

        if (tmpDelimiters != null) {
            tmpDelimiters = StringEscapeUtils.unescapeHtml4(tmpDelimiters);
            fullDelimiters = new char[4];
            for (int i = 0; i < 4; i++) {
                fullDelimiters[i] = tmpDelimiters.charAt(i);
            }
        }
        else {
            fullDelimiters = null;
        }
    }
}

