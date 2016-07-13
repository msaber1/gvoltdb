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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonWriteNullProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.voltcore.utils.CoreUtils;

/**
 *TODO:
 */
public class VoltTrace {
    private static int s_pid = -1;
    static {
        try {
            s_pid = Integer.parseInt(CoreUtils.getPID());
        } catch(NumberFormatException e) {
            //TODO:
        }
    }

    private static Map<Character, TraceEventType> s_typeMap = new HashMap<>();
    public static enum TraceEventType {

        ASYNC_BEGIN('b'),
        ASYNC_END('e'),
        ASYNC_INSTANT('n'),
        CLOCK_SYNC('c'),
        COMPLETE('X'),
        CONTEXT(','),
        COUNTER('C'),
        DURATION_BEGIN('B'),
        DURATION_END('E'),
        FLOW_END('f'),
        FLOW_START('s'),
        FLOW_STEP('t'),
        INSTANT('i'),
        MARK('R'),
        MEMORY_DUMP_GLOBAL('V'),
        MEMORY_DUMP_PROCESS('v'),
        METADATA('M'),
        OBJECT_CREATED('N'),
        OBJECT_DESTROYED('D'),
        OBJECT_SNAPSHOT('O'),
        SAMPLE('P'),
        VOLT_INTERNAL_CLOSE('z');

        private final char m_typeChar;

        private TraceEventType(char typeChar) {
            m_typeChar = typeChar;
            s_typeMap.put(typeChar, this);
        }

        public char getTypeChar() {
            return m_typeChar;
        }

        public static TraceEventType fromTypeChar(char ch) {
            return s_typeMap.get(ch);
        }
    }

    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    public static class TraceEvent {
        private String m_fileName;
        private TraceEventType m_type;
        private String m_name;
        private String m_category;
        private Long m_id;
        private long m_tid;
        private long m_micros;
        private String[] m_argsArr;
        private Map<String, String> m_args;

        // Empty constructor and setters for jackson deserialization for ease of testing
        public TraceEvent() {
        }

        public TraceEvent(String fileName,
                TraceEventType type,
                String name,
                String category,
                Long asyncId,
                String... args) {
            this();
            m_fileName = fileName;
            m_type = type;
            m_name = name;
            m_category = category;
            m_id = asyncId;
            m_argsArr = args;
            m_tid = Thread.currentThread().getId();
            m_micros = System.nanoTime()/1000;
        }

        private void mapFromArgArray() {
            m_args = new HashMap<>();
            if (m_argsArr == null) {
                return;
            }
            
            for (int i=0; i<m_argsArr.length; i+=2) {
                if (i+1 == m_argsArr.length) break;
                m_args.put(m_argsArr[i], m_argsArr[i+1]);
            }
        }

        @JsonIgnore
        public String getFileName() {
            return m_fileName;
        }

        public void setFileName(String fileName) {
            m_fileName = fileName;
        }

        @JsonIgnore
        public TraceEventType getType() {
            return m_type;
        }

        @JsonProperty("ph")
        public char getTypeChar() {
            return m_type.getTypeChar();
        }

        public void setTypeChar(char ch) {
            m_type = TraceEventType.fromTypeChar(ch);
        }

        public String getName() {
            return m_name;
        }

        public void setName(String name) {
            m_name = name;
        }

        @JsonProperty("cat")
        public String getCategory() {
            return m_category;
        }

        public void setCategory(String cat) {
            m_category = cat;
        }

        public Long getId() {
            return m_id;
        }

        public void setId(Long id) {
            m_id = id;
        }

        public int getPid() {
            return s_pid;
        }

        public void setPid(int pid) {
        }

        public long getTid() {
            return m_tid;
        }

        public void setTid(long tid) {
            m_tid = tid;
        }

        public long getTs() {
            return m_micros;
        }

        public void setTs(long ts) {
            m_micros = ts;
        }

        public Map<String, String> getArgs() {
            if (m_args==null) {
                mapFromArgArray();
            }
            return m_args;
        }

        public void setArgs(Map<String, String> args) {
            m_args = args;
        }
    }

    private static int QUEUE_SIZE = 1024;
    private static VoltTrace s_tracer = new VoltTrace();
    private LinkedBlockingQueue<TraceEvent> m_traceEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

    private VoltTrace() {
        new Thread(new TraceFileWriter(this)).start();
    }

    private void queueEvent(TraceEvent event) {
        boolean queued = m_traceEvents.offer(event);
        if (!queued) {
            //TODO: rate limited log?
        }
    }

    public TraceEvent takeEvent() throws InterruptedException {
        return m_traceEvents.take();
    }

    public static void main(String[] args) throws Exception {
        String[] eargs = { "One", "1", "Two", "2" };
        TraceEvent event = new TraceEvent("fileName", TraceEventType.METADATA, "test", "cat1", null, eargs);
        ObjectMapper mapper = new ObjectMapper();
        String str = mapper.writeValueAsString(event);
        System.out.println("JSON=" + str);
        event = mapper.readValue(str, TraceEvent.class);
        str = mapper.writeValueAsString(event);
        System.out.println("JSON looped around=" + str);
    }

    public static void meta(String fileName, String name, String... args) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.METADATA, name, null, null, args));
    }

    public static void instant(String fileName, String name, String category, long id, String... args) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.INSTANT, name, category, id, args));
    }

    public static void beginDuration(String fileName, String name, String category, String... args) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.DURATION_BEGIN, name, category, null, args));
    }

    public static void endDuration(String fileName) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.DURATION_END, null, null, null));
    }

    public static void beginAsync(String fileName, String name, String category, long id, String... args) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.ASYNC_BEGIN, name, category, id, args));
    }

    public static void endAsync(String fileName, String name, String category, long id, String... args) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.ASYNC_END, name, category, id, args));
    }
    public static void instantAsync(String fileName, String name, String category, long id, String... args) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.ASYNC_INSTANT, name, category, id, args));
    }

    public static void close(String fileName) {
        s_tracer.queueEvent(new TraceEvent(fileName, TraceEventType.VOLT_INTERNAL_CLOSE, null, null, null));
    }

    public static boolean hasEvents() {
        return !s_tracer.m_traceEvents.isEmpty();
    }
}
