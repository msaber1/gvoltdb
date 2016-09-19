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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.codehaus.jackson.map.ObjectMapper;

import junit.framework.TestCase;
import org.voltdb.StatsAgent;

import static org.mockito.Mockito.mock;
import static org.voltdb.utils.VoltTrace.Category.CI;

public class TestVoltTrace extends TestCase {

    private static final String FILE_NAME_PREFIX = "tracetest";

    private ObjectMapper m_mapper = new ObjectMapper();

    @Override
    public void setUp() throws Exception {
        System.setProperty("VOLT_JUSTATEST", "YESYESYES");
        VoltTrace.startTracer(".", mock(StatsAgent.class));
        cleanupTraceFiles();
    }

    @Override
    public void tearDown() throws Exception {
        cleanupTraceFiles();
    }

    private void cleanupTraceFiles() {
        File[] files = new File(".").listFiles();
        for (int i=0; i<files.length; i++) {
            if (files[i].getName().startsWith(FILE_NAME_PREFIX) && !files[i].isFile()) {
                throw new RuntimeException("Invalid trace file");
            }
            if (files[i].getName().startsWith(FILE_NAME_PREFIX)) {
                if (!files[i].delete()) {
                    throw new RuntimeException("Failed to delete file " + files[i]);
                }
            }
        }
    }

    public void testVoltTrace() throws Exception {
        int fileCount = 3;
        ExecutorService es = Executors.newFixedThreadPool(fileCount);
        SenderRunnable[] senders = new SenderRunnable[fileCount];
        for (int i=0; i<fileCount; i++) {
            senders[i] = new SenderRunnable();
            es.submit(senders[i]);
        }
        es.shutdown();
        es.awaitTermination(60, TimeUnit.SECONDS);

        final String path = VoltTrace.closeAllAndShutdown(true, 0);

        verifyFileContents(Arrays.stream(senders).map(SenderRunnable::getSentList).flatMap(List::stream).collect(Collectors.toList()), path);
    }

    public void testTraceLimit() throws Exception {
        int maxFiles = 0;
        for (int i=0; i<maxFiles; i++) {
            int finalI = i;
            VoltTrace.add(() -> VoltTrace.meta(FILE_NAME_PREFIX+ finalI, "name"+ finalI));
        }

        while (VoltTrace.hasEvents()) {
            Thread.sleep(250);
        }
        int count = countTraceFiles();
        assertEquals(maxFiles, count);

        // One more should not increase the count
        VoltTrace.add(() -> VoltTrace.meta(FILE_NAME_PREFIX+maxFiles, "name"+maxFiles));
        while (VoltTrace.hasEvents()) {
            Thread.sleep(250);
        }
        count = countTraceFiles();
        assertEquals(maxFiles, count);
        assertFalse(hasTraceFile(FILE_NAME_PREFIX+maxFiles));

        // Closing one should allow one more trace
//        VoltTrace.close();
        VoltTrace.add(() -> VoltTrace.meta(FILE_NAME_PREFIX+maxFiles, "name"+maxFiles));
        while (VoltTrace.hasEvents()) {
            Thread.sleep(250);
        }
        count = countTraceFiles();
        assertEquals(maxFiles+1, count);
        assertTrue(hasTraceFile(FILE_NAME_PREFIX+maxFiles));

        // cleanup
        for (int i=0; i<=maxFiles; i++) {
//            VoltTrace.close();
        }
    }

    public void testTracePurge() throws Exception {
        int maxFiles = 0;
        long startTime = System.currentTimeMillis();
        for (int i=0; i<maxFiles; i++) {
            int finalI = i;
            VoltTrace.add(() -> VoltTrace.meta(FILE_NAME_PREFIX+ finalI, "name"+ finalI));
        }

        // wait till purge time is up
        while ((System.currentTimeMillis()-startTime) < 1000) {
            Thread.sleep(500);
        }
        VoltTrace.add(() -> VoltTrace.meta(FILE_NAME_PREFIX+maxFiles, "name"+maxFiles));
        while (VoltTrace.hasEvents()) {
            Thread.sleep(250);
        }
        int count = countTraceFiles();
        assertEquals(maxFiles+1, count);
        assertTrue(hasTraceFile(FILE_NAME_PREFIX+maxFiles));

        // cleanup
        for (int i=0; i<=maxFiles; i++) {
//            VoltTrace.close();
        }
    }

    private boolean hasTraceFile(String fileName) {
        return Arrays.asList(new File(".").list()).contains(fileName);
    }

    private int countTraceFiles() {
        File[] files = new File(".").listFiles();
        int count = 0;
        for (int i=0; i<files.length; i++) {
            if (files[i].getName().startsWith(FILE_NAME_PREFIX)) {
                count++;
            }
        }

        return count;
    }

    private ArrayList<VoltTrace.TraceEventType> m_allEventTypes = new ArrayList<>(EnumSet.allOf(VoltTrace.TraceEventType.class));
    private Random m_random = new Random();
    private VoltTrace.TraceEvent randomEvent() {
        VoltTrace.TraceEvent event = null;
        while (event==null) {
            VoltTrace.TraceEventType type = m_allEventTypes.get(m_random.nextInt(m_allEventTypes.size()));
            switch(type) {
            case ASYNC_BEGIN:
                event = randomAsync(true);
                break;
            case ASYNC_END:
                event = randomAsync(false);
                break;
            case ASYNC_INSTANT:
                event = randomInstant(true);
                break;
            case DURATION_BEGIN:
                event = randomDurationBegin();
                break;
            case DURATION_END:
                event = randomDurationEnd();
                break;
            case INSTANT:
                event = randomInstant(false);
                break;
            case METADATA:
                event = randomMeta();
                break;
            default:
                break;
            }
        }

        return event;
    }

    private VoltTrace.TraceEvent randomDurationBegin() {
        return new VoltTrace.TraceEvent(VoltTrace.TraceEventType.DURATION_BEGIN,
                "name"+m_random.nextInt(5), CI, null, randomArgs());
    }

    private VoltTrace.TraceEvent randomDurationEnd() {
        return new VoltTrace.TraceEvent(VoltTrace.TraceEventType.DURATION_END,
                null, null, null);
    }

    private VoltTrace.TraceEvent randomAsync(boolean begin) {
        VoltTrace.TraceEventType type = (begin) ?
                VoltTrace.TraceEventType.ASYNC_BEGIN : VoltTrace.TraceEventType.ASYNC_END;
        return new VoltTrace.TraceEvent(type, "name"+m_random.nextInt(5),
                CI, Long.toString(m_random.nextLong()), randomArgs());
    }

    private VoltTrace.TraceEvent randomInstant(boolean async) {
        VoltTrace.TraceEventType type = (async) ?
                VoltTrace.TraceEventType.ASYNC_INSTANT : VoltTrace.TraceEventType.INSTANT;
        String id = (async) ? Long.toString(m_random.nextLong()) : null;
        return new VoltTrace.TraceEvent(type,
                "name"+m_random.nextInt(5), CI,
                id, randomArgs());
    }

    private static String[] s_metadataNames = { "process_name", "process_labels", "process_sort_index",
            "thread_name", "thread_sort_index"
    };
    private VoltTrace.TraceEvent randomMeta() {
        String name = s_metadataNames[m_random.nextInt(s_metadataNames.length)];
        return new VoltTrace.TraceEvent(VoltTrace.TraceEventType.METADATA, name, null, null,
                randomArgs());
    }

    private static String[] s_argKeys = { "name", "dest", "ciHandle", "txnid", "commit", "key1", "keyn" };
    private String[] randomArgs() {
        int count = m_random.nextInt(4);
        String[] args = new String[count*2];
        for (int i=0; i<count; i++) {
            String key = s_argKeys[m_random.nextInt(s_argKeys.length)];
            args[i*2] = key;
            args[i*2+1] = key+"-val";
        }

        return args;
    }

    private void verifyFileContents(List<VoltTrace.TraceEvent> expectedList, String outfile)
        throws IOException {
        List<VoltTrace.TraceEvent> readEvents = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(outfile))));
        String line = null;
        while ((line=reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("]") || line.equals("[")) {
                continue;
            }

            if (line.charAt(line.length()-1)==',') {
                line = line.substring(0, line.length()-1);
            }
            readEvents.add(m_mapper.readValue(line, VoltTrace.TraceEvent.class));
        }
        reader.close();
        assertEquals(expectedList.size(), readEvents.size());

        readEvents.sort((a, b) -> Double.compare(a.getTs(), b.getTs()));
        expectedList.sort((a, b) -> Double.compare(a.getNanos(), b.getNanos()));
        System.out.println("Expected");
        expectedList.forEach(System.out::println);
        System.out.println("Read");
        readEvents.forEach(System.out::println);
        for (int i = 0; i < expectedList.size(); i++) {
            compare(expectedList.get(i), readEvents.get(i));
        }
    }

    private void compare(VoltTrace.TraceEvent expected, VoltTrace.TraceEvent actual) {
        assertEquals(expected.getCategory(), actual.getCategory());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getPid(), actual.getPid());
//        assertEquals(expected.getTid(), actual.getTid());
        //assertEquals(expected.getTs(), actual.getTs());
        assertEquals(expected.getTypeChar(), actual.getTypeChar());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getArgs(), actual.getArgs());
    }

    public class SenderRunnable implements Runnable {
        private List<VoltTrace.TraceEvent> m_sentList = new ArrayList<>();

        public void run() {
            try {
                for (int i=0; i<10; i++) {
                    VoltTrace.TraceEvent event = randomEvent();
                    String[] args = new String[event.getArgs().size()*2];
                    int j=0;
                    for (String key : event.getArgs().keySet()) {
                        args[j++] = key;
                        args[j++] = event.getArgs().get(key);
                    }
                    event.setNanos(System.nanoTime());
                    switch(event.getType()) {
                    case ASYNC_BEGIN:
                        VoltTrace.add(() -> VoltTrace.beginAsync(event.getName(),
                                                                 VoltTrace.Category.valueOf(event.getCategory()),
                                                                 event.getId(), args));
                        break;
                    case ASYNC_END:
                        VoltTrace.add(() -> VoltTrace.endAsync(event.getName(),
                                                               VoltTrace.Category.valueOf(event.getCategory()),
                                                               event.getId(), args));
                        break;
                    case ASYNC_INSTANT:
                        VoltTrace.add(() -> VoltTrace.instantAsync(event.getName(),
                                                                   VoltTrace.Category.valueOf(event.getCategory()),
                                                                   event.getId(), args));
                        break;
                    case DURATION_BEGIN:
                        VoltTrace.add(() -> VoltTrace.beginDuration(event.getName(),
                                                                    VoltTrace.Category.valueOf(event.getCategory()),
                                                                    args));
                        break;
                    case DURATION_END:
                        VoltTrace.add(VoltTrace::endDuration);
                        break;
                    case INSTANT:
                        VoltTrace.add(() -> VoltTrace.instant(event.getName(),
                                                              VoltTrace.Category.valueOf(event.getCategory()),
                                                              args));
                        break;
                    case METADATA:
                        VoltTrace.add(() -> VoltTrace.meta(event.getName(), args));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported event type: " + event.getType());
                    }
                    m_sentList.add(event);
                }
            } catch(Throwable t) {
                t.printStackTrace();
            }
        }

        public List<VoltTrace.TraceEvent> getSentList() {
            return m_sentList;
        }
    }
}
