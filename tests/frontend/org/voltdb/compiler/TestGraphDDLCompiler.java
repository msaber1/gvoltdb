package org.voltdb.compiler;

import java.io.File;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltdb.catalog.DatabaseConfiguration;
import org.voltdb.utils.CatalogUtil;

import junit.framework.TestCase;

public class TestGraphDDLCompiler extends TestCase {

	public void testSimpleCreateGraph() throws HSQLParseException {
		    HSQLInterface hsql = HSQLInterface.loadHsqldb();
		    
		    String ddl1 =
	            "CREATE TABLE Users ( " +
	            "uId integer default '0' NOT NULL, " +
	            "lName varchar(16) default NULL, " +
	            "dob varchar(16) default NULL, " +
	            "PRIMARY KEY  (uId) " +
	            ");";

            hsql.runDDLCommand(ddl1);
		
            String ddl2 =
                "CREATE TABLE Ralationships ( " +
                "relId integer default '0' NOT NULL, " +
                "uId integer default '0' NOT NULL, " +
                "uId2 integer default '0' NOT NULL, " +
                "isRelative integer default NULL, " +
                "sDate varchar(16) default NULL, " +
                "PRIMARY KEY  (relId) " +
                ");";

            hsql.runDDLCommand(ddl2);
		
		String ddl3 =
            "CREATE UNDIRECTED GRAPH VIEW SocialNetwork "
            + "VERTEXES (ID = uId, lstName = lName, birthdat = dob) "
            + "FROM Users "
            + "WHERE 1 = 1 "
            + "EDGES (ID = relId, FROM = uId, TO = uId2, "
            + "startDate = sDate, relative = isRelative) "
            + "FROM Ralationships "
            + "WHERE 1 = 1;"; 

        hsql.runDDLCommand(ddl3);

        // TODO Outputs only tables, not graphs
        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);
        
        // DROP
        
        String ddl4 =
                "DROP GRAPH VIEW SocialNetwork;"; 

            hsql.runDDLCommand(ddl4);

            // TODO Outputs only tables, not graphs
            xml = hsql.getXMLFromCatalog();
            System.out.println(xml);
            assertTrue(xml != null);

    }
	
    public void testGraphDDL() {
        File jarOut = new File("graphddl.jar");
        jarOut.deleteOnExit();

        VoltCompiler compiler = new VoltCompiler();
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(
        		"CREATE TABLE Users ( " +
	            "uId integer default '0' NOT NULL, " +
	            "lName varchar(16) default NULL, " +
	            "dob varchar(16) default NULL, " +
	            "PRIMARY KEY  (uId) " +
	            ");\n"+ 
				"CREATE TABLE Ralationships ( " +
				"relId integer default '0' NOT NULL, " +
				"uId integer default '0' NOT NULL, " +
				"uId2 integer default '0' NOT NULL, " +
				"isRelative integer default NULL, " +
				"sDate varchar(16) default NULL, " +
				"PRIMARY KEY  (relId) " +
				");\n"+
				"CREATE UNDIRECTED GRAPH VIEW SocialNetwork "
				+ "VERTEXES (ID = uId, lstName = lName, birthdat = dob) "
				+ "FROM Users "
				+ "WHERE 1 = 1 "
				+ "EDGES (ID = relId, FROM = uId, TO = uId2, "
				+ "startDate = sDate, relative = isRelative) "
				+ "FROM Ralationships "
				+ "WHERE 1 = 1;\n"
        		);
        String schemaPath = schemaFile.getPath();

        try {
            assertTrue(compiler.compileFromDDL(jarOut.getPath(), schemaPath));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        // cleanup after the test
        jarOut.delete();
    }
	
}
