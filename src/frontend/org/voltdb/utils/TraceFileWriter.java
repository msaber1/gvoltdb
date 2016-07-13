/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *TODO:
 */
public class TraceFileWriter implements Runnable {
    private final ObjectMapper m_jsonMapper = new ObjectMapper();
    private final VoltTrace m_voltTrace;
    private boolean m_shutdown;
    private Map<String, BufferedWriter> m_fileWriters = new HashMap<>();

    public TraceFileWriter(VoltTrace voltTrace) {
        m_voltTrace = voltTrace;
    }

    public void run() {
        while (!m_shutdown) {
            try {
                VoltTrace.TraceEvent event = m_voltTrace.takeEvent();
                boolean firstRow = false;
                if (event.getType()==VoltTrace.TraceEventType.VOLT_INTERNAL_CLOSE) {
                    handleCloseEvent(event);
                } else {
                    if (m_fileWriters.get(event.getFileName()) == null) {
                        firstRow = true;
                    }
                    startTraceFile(event);
                }
                BufferedWriter bw = m_fileWriters.get(event.getFileName());
                if (bw != null) {
                    String json = m_jsonMapper.writeValueAsString(event);
                    if (!firstRow) bw.write(",");
                    bw.newLine();
                    bw.write(json);
                    bw.flush();
                }
            } catch(InterruptedException e) {
                e.printStackTrace();
                //TODO: log that thread got interrupted
                m_shutdown = true;
            } catch(IOException e) { // also catches JSON exceptions
                e.printStackTrace();
                // TODO: OK to assume something went really bad?
                //TODO: log exception and log that thread is exiting
                m_shutdown = true;
            }
        }
    }

    private void handleCloseEvent(VoltTrace.TraceEvent event) {
        BufferedWriter bw = m_fileWriters.get(event.getFileName());
        if (bw==null) return;

        try {
            bw.newLine();
            bw.write("]");
            bw.newLine();
            bw.close();
        } catch(IOException e) {
            //TODO: Debug log
        }
        m_fileWriters.remove(event.getFileName());
    }

    private void startTraceFile(VoltTrace.TraceEvent event) {
        BufferedWriter bw = m_fileWriters.get(event.getFileName());
        if (bw != null) return;

        File file = new File(event.getFileName());
        // if the file exists already, we don't want to overwrite
        if (file.exists()) {
            // TODO: log
            return;
        }

        try {
            //TODO: Path and full file name
            // Uses the default platform encoding for now.
            bw = new BufferedWriter(new FileWriter(event.getFileName()));
            m_fileWriters.put(event.getFileName(), bw);
            bw.write("[");
        } catch(IOException e) {
            //TODO: rate limited log
        }
    }
}
