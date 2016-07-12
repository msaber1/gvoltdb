/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.TestCase;

public class TestVoltTrace extends TestCase {

    private static final String FILE_NAME = "testfile1";
    private ObjectMapper m_mapper = new ObjectMapper();
    private final String[] m_testData = {
            //TODO: generate process name metadata event
            "{\"ph\":\"M\",\"name\":\"process_name\",\"args\":{\"Seq\":\"1\"}}",
            "{\"ph\":\"B\",\"name\":\"test-dur-beg\",\"args\":{\"Seq\":\"2\"},\"cat\":\"cat1\"}",
            "{\"ph\":\"E\",\"name\":\"test-dur-end\",\"args\":{\"Seq\":\"3\"},\"cat\":\"cat1\"}",
            "{\"ph\":\"b\",\"name\":\"test-async-beg\",\"args\":{\"Seq\":\"4\"},\"cat\":\"cat2\",\"id\":101}",
            "{\"ph\":\"e\",\"name\":\"test-async-end\",\"args\":{\"Seq\":\"5\"},\"cat\":\"cat2\",\"id\":101}",
    };

    private ConcurrentLinkedQueue<VoltTrace.TraceEvent> m_eventQueue = new ConcurrentLinkedQueue<>();

    @Override
    protected void setUp() throws Exception {
        File file = new File(FILE_NAME);
        if (file.exists()) {
            if (!file.delete()) {
                throw new RuntimeException("Failed to delete file " + file);
            }
        }
        for (int i=0; i<m_testData.length; i++) {
            VoltTrace.TraceEvent event = m_mapper.readValue(m_testData[i], VoltTrace.TraceEvent.class);
            // TODO: use single file name for now
            event.setFileName(FILE_NAME);
            m_eventQueue.offer(event);
        }
    }

    public void testVoltTrace() throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(new SenderRunnable());
        es.submit(new SenderRunnable());
        es.shutdown();
        es.awaitTermination(60, TimeUnit.SECONDS);

        while (VoltTrace.hasEvents()) {
            Thread.sleep(250);
        }
        VoltTrace.close(FILE_NAME);

        // TODO: read from file and verify
    }

    public class SenderRunnable implements Runnable {
        public void run() {
            VoltTrace.TraceEvent event = null;
            while ((event=m_eventQueue.poll()) != null) {
                String[] args = new String[event.getArgs().size()*2];
                int i=0;
                for (String key : event.getArgs().keySet()) {
                    args[i++] = key;
                    args[i++] = event.getArgs().get(key);
                }
                switch(event.getType()) {
                case METADATA:
                    VoltTrace.meta(event.getFileName(), event.getName(), args);
                    break;
                case DURATION_BEGIN:
                    VoltTrace.beginDuration(event.getFileName(), event.getName(), event.getCategory(), args);
                    break;
                case DURATION_END:
                    VoltTrace.endDuration(event.getFileName(), event.getName(), event.getCategory(), args);
                    break;
                case ASYNC_BEGIN:
                    VoltTrace.beginAsync(event.getFileName(), event.getName(), event.getCategory(),
                            event.getId(), args);
                    break;
                case ASYNC_END:
                    VoltTrace.endAsync(event.getFileName(), event.getName(), event.getCategory(),
                            event.getId(), args);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported event type: " + event.getType());
                }
            }
        }
    }
}
