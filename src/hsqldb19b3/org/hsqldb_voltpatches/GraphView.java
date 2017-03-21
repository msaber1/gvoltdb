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
    
    private Integer EDGE = 1;
    private Integer VERTEX = 2;
    private Integer PATH = 3;

    private ArrayList AllPropList;          // array of properties
    private HashMappedList PropTypeList;         // map index - prop type (edge/vertex/path)
    
    private HashMappedList VertexPropList;          // maps vertex name - id
    private int vertexPropCount;
    
    private HashMappedList EdgePropList;            // maps edge name - id
    private int edgePropCount;
    
    private HashMappedList PathPropList;            // maps path prop name - id
    private int pathPropCount;
    
    private final long DefPrecision = 10;
    
    //HsqlName[] VertexProperties;
    //HsqlName[] VertexColumns;
    
    //HsqlName[] EdgeProperties;
    //HsqlName[] EdgeColumns;
        
    HsqlName VTableName;
    HsqlName ETableName;
    
    String Hint;

	//public boolean focusVertexes;
	//public boolean focusEdges;
	//public boolean focusPaths;
	
    public GraphView(Database database, HsqlName name, int type) {
    	this.database = database;
    	GraphName = name;
    	
    	this.type = type;
    	if (type == TableBase.DIRECTED_GRAPH) isDirected = true;
    	else isDirected = false;
    	
    	AllPropList     = new ArrayList();
    	PropTypeList    = new HashMappedList();
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
	 * Adds default edge Vertex properties
	 * Called after adding all other columns from tables 
	 * in order to have column indices from source select statement matched indices of not defailt properties 
	 */
    public void addDefVertexProps(HsqlName schema, boolean isDelimitedIdentifier) {
    	
    	// VERTEX Def Prop
    	HsqlName Name = database.nameManager.newColumnHsqlName(schema, "FANOUT", isDelimitedIdentifier);
    	ColumnSchema fanOut = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addVertexPropNoCheck(fanOut);  
        
        Name = database.nameManager.newColumnHsqlName(schema, "FANIN", isDelimitedIdentifier);
    	ColumnSchema fanIn = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addVertexPropNoCheck(fanIn);
    	
    }
    
    /*
	 * Adds default Path properties
	 * Called after adding all other columns from tables 
	 * in order to have column indices from source select statement matched indices of not defailt properties 
	 */
    public void addDefPathProps(HsqlName schema, boolean isDelimitedIdentifier) {
    	
    	HsqlName Name;
    	// PATHS Def Prop
    	Name = database.nameManager.newColumnHsqlName(schema, "STARTVERTEXID", isDelimitedIdentifier);
    	ColumnSchema col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "ENDVERTEXID", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "LENGTH", isDelimitedIdentifier);
    	ColumnSchema pathLength = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(pathLength);    	
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "COST", isDelimitedIdentifier);
    	ColumnSchema pathCost = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(pathCost);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PATH", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new CharacterType(Types.SQL_VARCHAR, 1024), false, false, null);
    	addPathPropNoCheck(col);
    	
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
        
        graphxml.attributes.put("DDL", statement);
        
        // read all the vertex properties
        //VoltXMLElement vertexes = new VoltXMLElement("vertexes");
        //vertexes.attributes.put("name", "vertexes");
        
        VoltXMLElement vertex = new VoltXMLElement("vertex");
        
        for (int i = 0; i < getAllPropCount(); i++) {
        	if (PropTypeList.get(i) == VERTEX) {
        		ColumnSchema property = getVertexProp(i);
        		VoltXMLElement propChild = property.voltGetColumnXML(session);
        		propChild.attributes.put("index", Integer.toString(i));
        		vertex.children.add(propChild);
        		assert(propChild != null);
        	}
        }
        
        graphxml.children.add(vertex);
        
        VoltXMLElement path = new VoltXMLElement("path");
        for (int i = 0; i < getAllPropCount(); i++) {
        	if (PropTypeList.get(i) == PATH) {
        		ColumnSchema property = getPathProp(i);
        		VoltXMLElement propChild = property.voltGetColumnXML(session);
        		propChild.attributes.put("index", Integer.toString(i));
        		path.children.add(propChild);
        		assert(propChild != null);
        	}
        }

        graphxml.children.add(path);

        VoltXMLElement edge = new VoltXMLElement("edge");
        for (int i = 0; i < getAllPropCount(); i++) {
        	if (PropTypeList.get(i) == EDGE) {
        		ColumnSchema property = getEdgeProp(i);
        		VoltXMLElement propChild = property.voltGetColumnXML(session);
        		propChild.attributes.put("index", Integer.toString(i));
        		edge.children.add(propChild);
        		assert(propChild != null);
        	}
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
		return (ColumnSchema) AllPropList.get(i);
		
		//return (ColumnSchema) VertexPropList.get(i);
	}

    /**
     *  Returns the count of all visible vertex properties.
     */
    //public int getVertexPropCount() {
    //    return vertexPropCount;
    //}
    
    void renameVertexProp(ColumnSchema property, HsqlName newName) {

        String oldname = property.getName().name;
        int    i       = getVertexPropIndex(oldname);

        ((ColumnSchema)AllPropList.get(i)).getName().rename(newName);//VertexPropList.setKey(i, newName);
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

        int index = (Integer)VertexPropList.get(name);

        return index;
    }
    
    public void addVertexPropNoCheck(ColumnSchema property) {

        AllPropList.add(property);
        int idx = AllPropList.indexOf(property);
    	VertexPropList.add(property.getName().name, idx);
    	PropTypeList.add(idx, VERTEX);
    	

        vertexPropCount++;
    }
    // EDGES
	public ColumnSchema getEdgeProp(int i) {
		return (ColumnSchema) AllPropList.get(i);//EdgePropList.get(i);
	}

    // PATHS
	public ColumnSchema getPathProp(int i) {
		//System.out.println(i);
		return (ColumnSchema) AllPropList.get(i);//PathPropList.get(i);
	}
	
    // All
	public ColumnSchema getAllProp(int i) {
		//System.out.println(i);
		return (ColumnSchema) AllPropList.get(i);//PathPropList.get(i);
	}
	
    /**
     *  Returns the count of all visible vertex properties.
     */
    //public int getEdgePropCount() {
    //    return edgePropCount;
    //}
    
    void renameEdgeProp(ColumnSchema property, HsqlName newName) {

        String oldname = property.getName().name;
        int    i       = getEdgePropIndex(oldname);

        ((ColumnSchema)AllPropList.get(i)).getName().rename(newName);//EdgePropList.setKey(i, newName);
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

        int index = (Integer)EdgePropList.get(name);

        return index;
    }
    
    public void addEdgePropNoCheck(ColumnSchema property) {

        AllPropList.add(property);
        int idx = AllPropList.indexOf(property);
    	EdgePropList.add(property.getName().name, idx);
    	PropTypeList.add(idx, EDGE);
    	

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
    //public int getPathPropCount() {
    //    return pathPropCount;
    //}
    
    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findPathProp(String name) {

        int index = (Integer)PathPropList.get(name);

        return index;
    }
    
    public void addPathPropNoCheck(ColumnSchema property) {

        AllPropList.add(property);
        int idx = AllPropList.indexOf(property);
    	PathPropList.add(property.getName().name, idx);
    	PropTypeList.add(idx, PATH);
    	

        pathPropCount++;
    }
    
    /**
     *  Returns the count of all visible properties.
     */
    public int getAllPropCount() {
        return pathPropCount+edgePropCount+vertexPropCount;
    }
    
    public boolean isVertex(int id) {
    	if (PropTypeList.get(id) == VERTEX)
    		return true;
    	return false;
    }
    
    public boolean isEdge(int id) {
    	if (PropTypeList.get(id) == EDGE)
    		return true;
    	return false;
    }
    
    public boolean isPath(int id) {
    	if (PropTypeList.get(id) == PATH)
    		return true;
    	return false;
    }
}
