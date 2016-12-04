package org.voltdb.planner;

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
import org.voltdb.utils.BuildDirectoryUtils;

import junit.framework.TestCase;

public class TestGraphPlanner extends TestCase {

    HSQLInterface m_hsql;
    Database m_db;
    AllTpccSQL m_allSQL;
	
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        m_db = catalog.getClusters().get("cluster").getDatabases().get("database");

        URL url = TPCCProjectBuilder.class.getResource("graph-ddl.sql");
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
    
    void runSQLTest(String stmtName, String stmtSQL) {
        // use HSQLDB to get XML that describes the semantics of the statement
        // this is much easier to parse than SQL and is checked against the catalog
        VoltXMLElement xmlSQL = null;
        try {
            xmlSQL = m_hsql.getXMLCompiledStatement(stmtSQL);
        } catch (HSQLParseException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        // output the xml from hsql to disk for debugging
        BuildDirectoryUtils.writeFile("statement-hsql-xml", stmtName + ".xml", xmlSQL.toString(), true);

        System.out.println(xmlSQL);
        
        // get a parsed statement from the xml
        AbstractParsedStmt parsedStmt = AbstractParsedStmt.parse(stmtSQL, xmlSQL, null, m_db, null);
        // analyze expressions
        // except for "insert" statements that currently do without a joinTree.
        if (parsedStmt.m_joinTree != null) {
            parsedStmt.m_joinTree.analyzeJoinExpressions(parsedStmt.m_noTableSelectionList);
        }
        // output a description of the parsed stmt
        BuildDirectoryUtils.writeFile("statement-hsql-parsed", stmtName + ".txt", parsedStmt.toString(), true);

        assertTrue(parsedStmt.m_noTableSelectionList.isEmpty());

        System.out.println(parsedStmt.toString());
    }
	
    public void testParsedInStatements() {
    	//runSQLTest("1", "select lName "
        //        + " from Users " 
        //        + " WHERE lName = 'Smith';");
    	
    	runSQLTest("1", "select * from SocialNetwork.Vertexes;"); // 
        
    	//runSQLTest("1", "select fanOut "
        //              + " from SocialNetwork.Vertexes " 
        //              + " WHERE lstName = 'Smith';");
        
    	runSQLTest("1", "select * from SocialNetwork.Edges;"); //
    	
    	runSQLTest("1", "select V.FanOut, R.lName from SocialNetwork.Vertexes V JOIN USERS R ON V.ID = R.UID;");
    	
    	
    	//runSQLTest("1", "select PS.EndVertex.ID "
    	//			  + "from SocialNetwork.Paths PS "
    	//			  + "WHERE PS.StartVertex.ID = 1 and PS.Length = 2 "
    	//			  + "and PS.Edges.relative = 1;");
    	
    	
    }
    
}
