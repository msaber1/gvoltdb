package org.voltdb;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateGraph extends AdhocDDLTestBase {

    public void testBasicCreateTable() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            //assertFalse(findTableInSystemCatalogResults("SocialNetwork"));
            
            try {
                m_client.callProcedure("@AdHoc",
                		"CREATE TABLE Users ( " +
        	            "uId integer default '0' NOT NULL, " +
        	            "lName varchar(16) default NULL, " +
        	            "dob varchar(16) default NULL, " +
        	            "PRIMARY KEY  (uId) " +
        	            ");");
            }
            catch (ProcCallException pce) {
                fail("create table Users should have succeeded");
            }
            
            try {
                m_client.callProcedure("@AdHoc",
                		"CREATE TABLE Ralationships ( " +
                        "relId integer default '0' NOT NULL, " +
                        "uId integer default '0' NOT NULL, " +
                        "uId2 integer default '0' NOT NULL, " +
                        "isRelative integer default NULL, " +
                        "sDate varchar(16) default NULL, " +
                        "PRIMARY KEY  (relId) " +
                        ");");
            }
            catch (ProcCallException pce) {
                fail("create table Ralationships should have succeeded");
            }
            
            
            try {
                m_client.callProcedure("@AdHoc",
                		"CREATE UNDIRECTED GRAPH VIEW SocialNetwork "
        	            + "VERTEXES (ID = uId, lstName = lName, birthdat = dob) "
        	            + "FROM Users "
        	            //+ "WHERE 1 = 1 "
        	            + "EDGES (ID = relId, FROM = uId, TO = uId2, "
        	            + "startDate = sDate, relative = isRelative) "
        	            + "FROM Ralationships; "
        	            //+ "WHERE 1 = 1;"
                		);
               System.out.println("END callProc");
            }
            catch (ProcCallException pce) {
                fail("create graph view SocialNetwork should have succeeded");
            }
            
            //assertTrue(findGraphInSystemCatalogResults("SocialNetwork"));
            //System.out.println(findGraphInSystemCatalogResults("SocialNetwork"));
            
            System.out.println("CREATE VIEW START");
            
            try {
                m_client.callProcedure("@AdHoc",
                		"CREATE VIEW USERS_V AS "
                		+ ""
        	            + "SELECT lName, count(*) "
        	            + "FROM Users "
        	            + "GROUP BY lName;"
                		);
               System.out.println("END callProc");
            }
            catch (ProcCallException pce) {
                fail("create graph view SocialNetwork should have succeeded");
            }
            
            System.out.println("CREATE VIEW END");
            
            /*
            try {
	            VoltTable results[] = m_client.callProcedure("@AdHoc", 
	            		              "SELECT * "+
	                                  "FROM SocialNetwork.Vertexes;").getResults();
	            assertTrue(results[0].advanceRow());
            }
            catch (Exception e) {
                e.printStackTrace();
                fail();
            }
            */
        }
        finally {
            teardownSystem();
        }
    }
	
}
