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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.export.AdvertisedDataSource;
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
    public void offer(AdvertisedDataSource advertisement, ByteBuffer buf) throws IOException;

    /** Indicate an advertisement is complete. */
    public void done(CompletionEvent completionEvent);
}
