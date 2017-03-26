package org.hsqldb_voltpatches;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;

public class GraphView implements SchemaObject {

    public static final GraphView[] emptyArray = new GraphView[]{};
    
	protected HsqlName GraphName;
    public Database database;
    protected boolean isDirected;
    protected int type;
	
    String VSubQuery;
    String ESubQuery;
    String statement;

    public HashMappedList VertexPropList;          // columns in table
    public int vertexPropCount;
    
    public HashMappedList EdgePropList;          // columns in table
    public int edgePropCount;
    
    public HashMappedList PathPropList;          // artificial columns
    public int pathPropCount;
    
    private final long DefPrecision = 10;
    
    //HsqlName[] VertexProperties;
    //HsqlName[] VertexColumns;
    
    //HsqlName[] EdgeProperties;
    //HsqlName[] EdgeColumns;
        
    HsqlName VTableName;
    HsqlName ETableName;

	//public boolean focusVertexes;
	//public boolean focusEdges;
	//public boolean focusPaths;
	
    public GraphView(Database database, HsqlName name, int type) {
    	this.database = database;
    	GraphName = name;
    	
    	this.type = type;
    	if (type == TableBase.DIRECTED_GRAPH) isDirected = true;
    	else isDirected = false;
    	
    	VertexPropList  = new HashMappedList();
    	vertexPropCount = 0;
    	EdgePropList    = new HashMappedList();
    	edgePropCount = 0;
    	PathPropList    = new HashMappedList();
    	pathPropCount = 0;
    	
    	//focusVertexes = false;
    	//focusEdges = false;
    	//focusPaths = false;
    	

    }
    
    /*
	 * Adds default properties
	 * Called after adding all other columns from tables 
	 * in order to have column indices from source select statement matched indices of not defailt properties 
	 */
    public void addDefaultProperties(HsqlName schema, boolean isDelimitedIdentifier) {
    	
    	HsqlName fanoutName = database.nameManager.newColumnHsqlName(schema, "FANOUT", isDelimitedIdentifier);
    	ColumnSchema fanOut = new ColumnSchema(fanoutName, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addVertexPropNoCheck(fanOut);  
        
        HsqlName faninName = database.nameManager.newColumnHsqlName(schema, "FANIN", isDelimitedIdentifier);
    	ColumnSchema fanIn = new ColumnSchema(faninName, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addVertexPropNoCheck(fanIn);
    	
    	HsqlName pathLengthName = database.nameManager.newColumnHsqlName(schema, "LENGTH", isDelimitedIdentifier);
    	ColumnSchema pathLength = new ColumnSchema(pathLengthName, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(pathLength);    	
    }
    
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return type;
	}

	@Override
	public HsqlName getName() {
		// TODO Auto-generated method stub
		return GraphName;
	}

	@Override
	public HsqlName getSchemaName() {
		// TODO Auto-generated method stub
		return GraphName.schema;
	}

	@Override
	public HsqlName getCatalogName() {
		// TODO Auto-generated method stub
		return database.getCatalogName();
	}

	@Override
	public Grantee getOwner() {
		// TODO Auto-generated method stub
		return GraphName.schema.owner;
	}

	@Override
	public OrderedHashSet getReferences() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OrderedHashSet getComponents() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void compile(Session session) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSQL() {
		// TODO Auto-generated method stub
		return statement;
	}


	public void setSQL(String sqlString) {
		statement = sqlString;
		
	}
	
    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    VoltXMLElement voltGetGraphXML(Session session)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement graphxml = new VoltXMLElement("graph");
        Map<String, String> autoGenNameMap = new HashMap<String, String>();

        // add graph metadata
        String graphName = getName().name;
        graphxml.attributes.put("name", graphName);
        
        graphxml.attributes.put("Vtable", VTableName.name);
        graphxml.attributes.put("Etable", ETableName.name);
        
        graphxml.attributes.put("Vquery", VSubQuery);
        graphxml.attributes.put("Equery", ESubQuery);

        graphxml.attributes.put("isdirected", String.valueOf(isDirected));
        
        // read all the vertex properties
        //VoltXMLElement vertexes = new VoltXMLElement("vertexes");
        //vertexes.attributes.put("name", "vertexes");
        
        VoltXMLElement vertex = new VoltXMLElement("vertex");
        
        //int j = 0;
        for (int i = 0; i < vertexPropCount; i++) {
            ColumnSchema property = getVertexProp(i);
            VoltXMLElement propChild = property.voltGetColumnXML(session);
            propChild.attributes.put("index", Integer.toString(i));
            vertex.children.add(propChild);
            assert(propChild != null);
            //j = i;
        }
        //j++;
        /*
         * Add default properties: FanOut & FanIn
         */
        /*
        VoltXMLElement fanOut = new VoltXMLElement("column");
        fanOut.attributes.put("name", "FanOut");
        fanOut.attributes.put("index", Integer.toString(j));
        j++;
        String typestring = Types.getTypeName(Types.SQL_INTEGER);
        fanOut.attributes.put("valuetype", typestring);
        fanOut.attributes.put("nullable", String.valueOf(false));
        fanOut.attributes.put("size", String.valueOf(FanPrecision));
        vertex.children.add(fanOut);
        
        VoltXMLElement fanIn = new VoltXMLElement("column");
        fanIn.attributes.put("name", "FanIn");
        fanIn.attributes.put("index", Integer.toString(j));
        typestring = Types.getTypeName(Types.SQL_INTEGER);
        fanIn.attributes.put("valuetype", typestring);
        fanIn.attributes.put("nullable", String.valueOf(false));
        fanIn.attributes.put("size", String.valueOf(FanPrecision));
        vertex.children.add(fanIn);
        */
        //*************************************************/
        
        graphxml.children.add(vertex);
        /*
        HsqlName[] VertexProperties = getVertexProperties();
        for (HsqlName prop : VertexProperties) {
        	VoltXMLElement property = new VoltXMLElement("property");
            property.attributes.put("name", prop.statementName);
            vertex.children.add(property);
        }
        */
        //vertexes.children.add(vertex);

        // read all the edge properties
        //VoltXMLElement edges = new VoltXMLElement("edges");
        //edges.attributes.put("name", "edges");
        
        VoltXMLElement edge = new VoltXMLElement("edge");
        for (int i = 0; i < edgePropCount; i++) {
            ColumnSchema property = getEdgeProp(i);
            VoltXMLElement propChild = property.voltGetColumnXML(session);
            propChild.attributes.put("index", Integer.toString(i));
            edge.children.add(propChild);
            assert(propChild != null);
        }
        graphxml.children.add(edge);
        /*
        HsqlName[] EdgeProperties = getEdgeProperties();
        for (HsqlName prop : EdgeProperties) {
        	VoltXMLElement property = new VoltXMLElement("property");
            property.attributes.put("name", prop.statementName);
            edge.children.add(property);
        }
        */
        //edges.children.add(edge);
        
        return graphxml;
    }

    /*
	private HsqlName[] getEdgeProperties() {
		// TODO Auto-generated method stub
		return EdgeProperties;
	}

	private HsqlName[] getVertexProperties() {
		// TODO Auto-generated method stub
		return VertexProperties;
	}
    */
    
	public ColumnSchema getVertexProp(int i) {
		return (ColumnSchema) VertexPropList.get(i);
	}

    /**
     *  Returns the count of all visible vertex properties.
     */
    public int getVertexPropCount() {
        return vertexPropCount;
    }
    
    void renameVertexProp(ColumnSchema property, HsqlName newName) {

        String oldname = property.getName().name;
        int    i       = getVertexPropIndex(oldname);

        VertexPropList.setKey(i, newName);
        property.getName().rename(newName);
    }
    
    /**
     *  Returns the index of given column name or throws if not found
     */
    public int getVertexPropIndex(String name) {

        int i = findVertexProp(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }
    
    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findVertexProp(String name) {

        int index = VertexPropList.getIndex(name);

        return index;
    }
    
    public void addVertexPropNoCheck(ColumnSchema property) {

        VertexPropList.add(property.getName().name, property);

        vertexPropCount++;
    }
    // EDGES
	public ColumnSchema getEdgeProp(int i) {
		return (ColumnSchema) EdgePropList.get(i);
	}

    // EDGES
	public ColumnSchema getPathProp(int i) {
		return (ColumnSchema) PathPropList.get(i);
	}
	
    /**
     *  Returns the count of all visible vertex properties.
     */
    public int getEdgePropCount() {
        return edgePropCount;
    }
    
    void renameEdgeProp(ColumnSchema property, HsqlName newName) {

        String oldname = property.getName().name;
        int    i       = getEdgePropIndex(oldname);

        EdgePropList.setKey(i, newName);
        property.getName().rename(newName);
    }
    
    /**
     *  Returns the index of given edge property name or throws if not found
     */
    public int getEdgePropIndex(String name) {

        int i = findEdgeProp(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }
    
    /**
     *  Returns the index of given edge property name or -1 if not found.
     */
    public int findEdgeProp(String name) {

        int index = EdgePropList.getIndex(name);

        return index;
    }
    
    public void addEdgePropNoCheck(ColumnSchema property) {

        EdgePropList.add(property.getName().name, property);

        edgePropCount++;
    }
    // Path
    /**
     *  Returns the index of given column name or throws if not found
     */
    public int getPathPropIndex(String name) {

        int i = findPathProp(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }
    
    /**
     *  Returns the count of all visible vertex properties.
     */
    public int getPathPropCount() {
        return pathPropCount;
    }
    
    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findPathProp(String name) {

        int index = PathPropList.getIndex(name);

        return index;
    }
    
    public void addPathPropNoCheck(ColumnSchema property) {

        PathPropList.add(property.getName().name, property);

        pathPropCount++;
    }
}