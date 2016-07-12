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
                BufferedWriter bw = m_fileWriters.get(event.getFileName());
                if (bw == null) {
                    try {
                        //TODO: Path and full file name
                        // Uses the default platform encoding for now.
                        bw = new BufferedWriter(new FileWriter(event.getFileName(), true));
                        m_fileWriters.put(event.getFileName(), bw);
                        bw.write("[");
                        bw.newLine();
                    } catch(FileNotFoundException e) {
                        //TODO: rate limited log
                        continue;
                    }
                }
                String json = m_jsonMapper.writeValueAsString(event);
                bw.write(json);
                bw.newLine();
                bw.flush();
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
}
