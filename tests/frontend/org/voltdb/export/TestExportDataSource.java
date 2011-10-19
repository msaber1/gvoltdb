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

import java.io.File;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
public class TestExportDataSource extends TestCase {

    static {
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
    }

    MockVoltDB m_mockVoltDB;
    int m_host = 0;
    int m_site = 1;
    int m_part = 2;

    @Override
    public void setUp() {
        m_mockVoltDB = new MockVoltDB();
        m_mockVoltDB.addHost(m_host);
        m_mockVoltDB.addPartition(m_part);
        m_mockVoltDB.addSite(m_site, m_host, m_part, true);
        m_mockVoltDB.addTable("TableName", false);
        m_mockVoltDB.addColumnToTable("TableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("TableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
        m_mockVoltDB.addTable("RepTableName", false);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);

        File directory = new File("/tmp");
        for (File f : directory.listFiles()) {
            if (f.getName().endsWith(".pbd") || f.getName().endsWith(".ad")) {
                f.delete();
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        m_mockVoltDB.shutdown(null);
    }

    public void testExportDataSource() throws Exception
    {
        String[] tables = {"TableName", "RepTableName"};
        for (String table_name : tables)
        {
            Table table = m_mockVoltDB.getCatalogContext().database.getTables().get(table_name);
            ExportDataSource s = new ExportDataSource( null, "database",
                                                table.getTypeName(),
                                                m_part,
                                                m_site,
                                                table.getSignature(),
                                                0,
                                                table.getColumns(),
                                                "/tmp");

            assertEquals("database", s.getDatabase());
            assertEquals(table_name, s.getTableName());
            assertEquals(m_part, s.getPartitionId());
            assertEquals(m_site, s.getSiteId());
            assertEquals(table.getSignature(), s.getSignature());
            // There are 6 additional Export columns added
            assertEquals(2 + 6, s.m_columnNames.size());
            assertEquals(2 + 6, s.m_columnTypes.size());
            assertEquals("VOLT_TRANSACTION_ID", s.m_columnNames.get(0));
            assertEquals("VOLT_EXPORT_OPERATION", s.m_columnNames.get(5));
            assertEquals("COL1", s.m_columnNames.get(6));
            assertEquals("COL2", s.m_columnNames.get(7));
            assertEquals(VoltType.INTEGER.ordinal(), s.m_columnTypes.get(6).intValue());
            assertEquals(VoltType.STRING.ordinal(), s.m_columnTypes.get(7).intValue());
        }
    }
}
