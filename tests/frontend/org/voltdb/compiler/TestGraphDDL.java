package org.voltdb.compiler;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltdb.AllTpccSQL;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.utils.BuildDirectoryUtils;

import junit.framework.TestCase;

public class TestGraphDDL extends TestCase {

    HSQLInterface m_hsql;
    Database m_db;
    AllTpccSQL m_allSQL;
	
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        m_db = catalog.getClusters().get("cluster").getDatabases().get("database");

        URL url = TPCCProjectBuilder.class.getResource("viewusers_ddl.sql");
        m_hsql = HSQLInterface.loadHsqldb();
        try {
            m_hsql.runDDLFile(URLDecoder.decode(url.getPath(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        m_allSQL = new AllTpccSQL();
        
        VoltXMLElement xml = m_hsql.getXMLFromCatalog();
        //System.out.println(xml);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    public void test() {
    	int v;
    }
    
}
