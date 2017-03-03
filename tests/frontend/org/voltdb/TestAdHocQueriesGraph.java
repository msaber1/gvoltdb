package org.voltdb;

import java.io.IOException;

import org.voltdb.TestAdHocQueries.TestEnv;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdHocQueriesGraph extends //AdHocQueryTester, 
                                           AdhocDDLTestBase {

    /*Client m_client;
    private final static boolean m_debug = false;
    public static final boolean retry_on_mismatch = true;
	
    public static String m_catalogJar = "adhoc.jar";
    public static String m_pathToCatalog = Configuration.getPathToCatalogForTest(m_catalogJar);
    public static String m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");
    */
    /*public void testSimple() throws Exception {
        System.out.println("Starting testSimple");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();

            //VoltTable modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1);").getResults()[0];
            //assertEquals(1, modCount.getRowCount());
            //assertEquals(1, modCount.asScalarLong());

            VoltTable result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM SocialNetwork.Vertexes;").getResults()[0];
            assertEquals(1, result.getRowCount());
            System.out.println(result.toString());

        }
        finally {
            env.tearDown();
            System.out.println("Ending testSimple");
        }
    }*/

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
             
            try {
            	System.out.println("CREATE TABLE");
            	m_client.callProcedure("@AdHoc",
                		"CREATE TABLE Users ( " +
        	            "uId integer NOT NULL);");
            }
            catch (ProcCallException pce) {
                fail("create table Users should have succeeded");
            }
            
            try {
            	System.out.println("BEFORE CALL");
            	VoltTable result = m_client.callProcedure("@AdHoc", "SELECT * FROM Users;").getResults()[0];
                System.out.println("AFTER CALL");
                System.out.println("RESULT:" + result.toString());
                assertEquals(1, result.getRowCount());
               
            }
            catch (ProcCallException pce) {
                fail("create graph view SocialNetwork should have succeeded");
            }

        }
        finally {
            teardownSystem();
        }
    }
	
    public void testBasicCreateGraph() throws Exception
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
             
            try {
            	System.out.println("CREATE TABLE");
            	m_client.callProcedure("@AdHoc",
            			"CREATE TABLE Users ( " +
        	            "uId integer default '0' NOT NULL, " +
        	            "lName varchar(16) default NULL, " +
        	            "dob varchar(16) default NULL, " +
        	            "PRIMARY KEY  (uId) " +
        	            ");");
            	m_client.callProcedure("@AdHoc",
            			"CREATE TABLE Ralationships ( " +
                        "relId integer default '0' NOT NULL, " +
                        "uId integer default '0' NOT NULL, " +
                        "uId2 integer default '0' NOT NULL, " +
                        "isRelative integer default NULL, " +
                        "sDate varchar(16) default NULL, " +
                        "PRIMARY KEY  (relId) " +
                        ");");
                m_client.callProcedure("@AdHoc",
                		"CREATE UNDIRECTED GRAPH VIEW SocialNetwork "
        	            + "VERTEXES (ID = uId, lstName = lName, birthdat = dob) "
        	            + "FROM Users "
        	            + "WHERE 1 = 1 "
        	            + "EDGES (ID = relId, FROM = uId, TO = uId2, "
        	            + "startDate = sDate, relative = isRelative) "
        	            + "FROM Ralationships "
        	            + "WHERE 1 = 1;");
            }
            catch (ProcCallException pce) {
                fail("create table Users should have succeeded");
                System.out.println("create table Users should have succeeded");
            }
            
            try {
            	System.out.println("BEFORE CALL");
            	VoltTable result = m_client.callProcedure("@AdHoc", "SELECT * FROM SocialNetwork.Vertexes;").getResults()[0];
                System.out.println("AFTER CALL");
                assertEquals(1, result.getRowCount());
                System.out.println("RESULT:" + result.toString());
               
            }
            catch (ProcCallException pce) {
                fail("create graph view SocialNetwork should have succeeded");
            }

        }
        finally {
            teardownSystem();
        }
    }
    
    /**
     * @param query
     * @param hashable - used to pick a single partition for running the query
     * @param spPartialSoFar - counts from prior SP queries to compensate for unpredictable hashing
     * @param expected - expected value of MP query (and of SP query, adjusting by spPartialSoFar, and only if validatingSPresult).
     * @param validatingSPresult - disables validation for non-deterministic SP results (so we don't have to second-guess the hashinator)
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    /*@Override
    public int runQueryTest(String query, int hashable, int spPartialSoFar, int expected, int validatingSPresult)
            throws IOException, NoConnectionsException, ProcCallException {
        VoltTable result;
        result = m_client.callProcedure("@AdHoc", query).getResults()[0];
        //System.out.println(result.toString());
        assertEquals(expected, result.getRowCount());

        result = m_client.callProcedure("@AdHocSpForTest", query, hashable).getResults()[0];
        int spResult = result.getRowCount();
        //System.out.println(result.toString());
        if (validatingSPresult != 0) {
            assertEquals(expected, spPartialSoFar + spResult);
        }

        return spResult;
    }*/
	
}
