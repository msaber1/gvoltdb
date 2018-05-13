/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.planner;

import java.io.File;
import java.io.IOException;

import org.voltdb.CatalogContext;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Supplier;

import junit.framework.TestCase;

public class TestGraphPlannerTool extends TestCase {

    PlannerTool m_pt = null;

    public void testSimple() throws Exception {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addAllDefaults();
        final File jar = new File("tpcc-oop.jar");
        jar.deleteOnExit();

        builder.compile("tpcc-oop.jar");

        byte[] bytes = MiscUtils.fileToBytes(new File("tpcc-oop.jar"));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        Supplier<ClusterSettings> settings = ClusterSettings.create().asSupplier();
        CatalogContext context = new CatalogContext(0, 0, catalog, settings, bytes, null, new byte[] {}, 0);

        m_pt = new PlannerTool(context.cluster, context.database, context.getCatalogHash());

        AdHocPlannedStatement result = null;
        
        //result = m_pt.planSqlForTest("select * from SocialNetwork.Vertexes;");
        //System.out.println(result);

        /*
        result = null;
        result = m_pt.planSqlForTest("select birthdate, fanOut "
        		                   + " from SocialNetwork.Vertexes " 
        		                   + " WHERE lstName = 'Smith';");
        System.out.println(result);
        */
        /*
        result = null;
        result = m_pt.planSqlForTest("select V.FanOut, R.lName from SocialNetwork.Vertexes V JOIN USERS R ON V.ID = R.UID;");
        System.out.println(result);
		*/
        
        //result = null;
        //result = m_pt.planSqlForTest("select V.isRelative, R.lName from Ralationships V JOIN USERS R ON V.uId = R.UID;");
        //System.out.println(result);
        
        // Sigmod Demo
        //result = null;
        //result = m_pt.planSqlForTest("Select * from Users U, SocialNetwork.Paths P " + 
        //                             "Where P.StartVertexID = U.UId and P.Length = 2;");
        //System.out.println(result);
        
        
        //result = null;
        //result = m_pt.planSqlForTest("Select * from Users U, Users U2, SocialNetwork.Paths P "+
        //							 "Where P.StartVertexID = U.UId And P.EndVertexId = U2.UId and P.Length = 2;");
        //System.out.println(result);
        
        
        result = null;
        result = m_pt.planSqlForTest("Select * from Users U, Users U2, SocialNetwork.Paths P Hint(SHORTESTPATH(Weight)) " + 
		                             "Where P.StartVertexID = U.UId And P.EndVertexId = U2.UId;");
        System.out.println(result);
        
        /*
        result = null;
        result = m_pt.planSqlForTest( "select PS.EndVertex.ID "
        							+ "from SocialNetwork.Paths PS "
        							+ "WHERE PS.StartVertex.ID = 1 and PS.Length = 2 "
        							+ "and PS.Edges.relative = 1;");
        System.out.println(result);
        */
        /*
        result = null;
        result = m_pt.planSqlForTest("select ES.from.lstName, ES.to.lstName "
        		                   + " from SocialNetwork.Edges ES "
        		                   + " WHERE ES.StartDate < 1/1/2000;");
        System.out.println(result);
        */
    }
}
