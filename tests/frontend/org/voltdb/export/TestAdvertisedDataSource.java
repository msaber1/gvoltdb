/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.voltdb.VoltType;
import org.voltdb.messaging.FastSerializer;


public class TestAdvertisedDataSource extends TestCase {

    public void testEquals(){

        ArrayList<String> n1 = new ArrayList<String>();
        n1.add("abc");
        n1.add("def");

        ArrayList<VoltType> t1 = new ArrayList<VoltType>();
        t1.add(VoltType.BIGINT);
        t1.add(VoltType.FLOAT);

        AdvertisedDataSource ad1 =
            new AdvertisedDataSource(
                    10,
                    "signature",
                    "name",
                    100L,
                    10L,
                    n1,
                    t1);

        AdvertisedDataSource ad2 =
            new AdvertisedDataSource(
                    10,
                    "signature",
                    "name",
                    100L,
                    10L,
                    n1,
                    t1);

        AdvertisedDataSource ad3 =
            new AdvertisedDataSource(
                    10,
                    "signature2",
                    "name",
                    100L,
                    10L,
                    n1,
                    t1);

        assertTrue(ad1.equals(ad1));
        assertTrue(ad1.equals(ad2));
        assertTrue(ad2.equals(ad1));
        assertFalse(ad1.equals(ad3));
        assertFalse(ad3.equals(ad1));
    }

    public void testDeserialize() throws IOException {
        String[] names = new String[]{"abc", "def"};

        ExportDataSource eds =
            new ExportDataSource(
                    null,
                    "database",
                    1, 2, "signature!vv", 3L,
                    names,
                    "/tmp");

        assertTrue(eds.getSignature().equals("signature!vv"));
        assertTrue(eds.getTableName().equals("signature"));

        FastSerializer fs = new FastSerializer();
        eds.writeAdvertisementTo(fs);

        // throw away the leading length preceeding int.
        ByteBuffer buf = fs.getBuffer();
        buf.getInt();

        AdvertisedDataSource ads =
            AdvertisedDataSource.deserialize(buf);

        assertTrue(ads.m_generation == 3L);
        assertTrue(ads.partitionId == 1);
        assertTrue(ads.tableName.equals("signature"));
        assertTrue(ads.signature.equals("signature!vv"));
        assertTrue(ads.columnNames.size() > 0); // metadata columns??
        assertTrue(ads.columnTypes.size() == ads.columnNames.size());

    }

}
