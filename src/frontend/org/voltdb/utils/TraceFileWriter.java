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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import org.codehaus.jackson.map.ObjectMapper;
import org.voltcore.logging.VoltLogger;


/**
 * Reads trace events from VoltTrace queue and writes them to files.
 */
public class TraceFileWriter implements Runnable {
    private static final VoltLogger s_logger = new VoltLogger("TRACER");

    private final ObjectMapper m_jsonMapper = new ObjectMapper();
    private final File m_path;
    private final Queue<Supplier<VoltTrace.TraceEvent>> m_events;
    private BufferedWriter m_fileWriter = null;
    private Long m_firstEventTime = null;

    public TraceFileWriter(File path, Queue<Supplier<VoltTrace.TraceEvent>> events) {
        m_events = events;
        m_path = path;
    }

    @Override
    public void run() {
        long count = 0;

        try {
            Supplier<VoltTrace.TraceEvent> eventSupplier;
            while ((eventSupplier = m_events.poll()) != null) {
                VoltTrace.TraceEvent event = eventSupplier.get();
                if (event == null) {
                    continue;
                }

                if (m_fileWriter == null) {
                    startTraceFile(event);
                } else {
                    m_fileWriter.write(",");
                }

                event.setSyncNanos(m_firstEventTime);
                String json = m_jsonMapper.writeValueAsString(event);
                m_fileWriter.newLine();
                m_fileWriter.write(json);
                m_fileWriter.flush();

                count++;
            }
        } catch(IOException e) { // also catches JSON exceptions
            s_logger.warn("Unexpected IO exception in trace file writer. Stopping trace file writer.", e);
        }

        handleCloseEvent();

        s_logger.info("Wrote " + count + " trace events to " + m_path.getAbsolutePath());
    }

    private void handleCloseEvent() {
        BufferedWriter bw = m_fileWriter;
        if (bw==null) return;

        try {
            bw.newLine();
            bw.write("]");
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Exception closing trace file buffered writer", e);
            }
        }
        m_fileWriter = null;
        m_firstEventTime = null;
    }

    private void startTraceFile(VoltTrace.TraceEvent event) throws IOException {
        BufferedWriter bw = m_fileWriter;
        if (bw != null) return;

        // Uses the default platform encoding for now.
        bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(m_path))));
        m_fileWriter = bw;
        m_firstEventTime = event.getNanos();
        bw.write("[");
        bw.flush();
    }
}
