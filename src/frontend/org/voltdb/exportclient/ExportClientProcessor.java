package org.voltdb.exportclient;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.exportclient.ExportClient.CompletionEvent;

/**
 * ExportClientProcessor is the interface between the
 * ExportClient I/O handling and the export data encoders,
 * like the export file client, for example. The I/O handler
 * is running in its own thread. An ExportClientProcessor
 * can safely provide back-pressure on the I/O handler by
 * blocking in the emptyBuffer() and offer() methods.
 */
public interface ExportClientProcessor {
    /** Obtain a byte buffer to fill with export data */
    public ByteBuffer emptyBuffer();

    /**
     * Hand-off a buffer of export data.
     * @throws IOException if data could not be buffered or processed.
     * */
    public void offer(String advertisement, ByteBuffer buf) throws IOException;

    /** Indicate an advertisement is complete. */
    public void done(CompletionEvent completionEvent);
}
