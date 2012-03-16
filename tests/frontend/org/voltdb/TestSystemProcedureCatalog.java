/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb;

import java.io.UnsupportedEncodingException;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import junit.framework.TestCase;

public class TestSystemProcedureCatalog extends TestCase {

    public void testSerializeConfigToJson() throws JSONException
    {
        SystemProcedureCatalog.Config dut = SystemProcedureCatalog.listing.get("@Promote");
        JSONObject blah = dut.serializeToJson();
        System.out.println(blah.toString(2));
        assertTrue(blah.toString() != null);
        assertTrue(!blah.toString().isEmpty());
    }

    public void testSerializeSysprocCatalogToJson() throws JSONException, UnsupportedEncodingException
    {
        String blah = SystemProcedureCatalog.serializeSysprocCatalogToJson().toString();
        assertTrue(blah.toString() != null);
        assertTrue(!blah.toString().isEmpty());
    }

    public void testEasyStuff() throws JSONException, UnsupportedEncodingException
    {
        JSONObject blah = SystemProcedureCatalog.serializeSysprocCatalogToJson();
        System.out.println(blah.toString(2));
        System.out.println("LENGTH: " + blah.toString().getBytes("UTF-8").length);
        JSONArray procs = blah.getJSONArray("sysprocs");
        boolean foundProfCtl = false;
        for (int i = 0; i < procs.length(); i++)
        {
            JSONObject proc = procs.getJSONObject(i);
            if (proc.getString("procedure").equals("@ProfCtl")) {
                foundProfCtl = true;
            }
        }
        assertFalse(foundProfCtl);
    }
}
