#ifndef GRAPHVIEW_H
#define GRAPHVIEW_H

#include <map>
#include <string>
#include "storage/table.h"
#include "storage/temptable.h"
#include "graph/GraphTypes.h"

#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/ValueFactory.hpp"

using namespace std;

namespace voltdb {

//#include "vertex.h"
class Vertex;
class Edge;
class PathIterator;

class GraphView
{
	friend class PathIterator;
	friend class TableIterator;
	friend class GraphViewFactory;

public:
	~GraphView(void);

	/*
	 * Table lifespan can be managed by a reference count. The
	 * reference is trivial to maintain since it is only accessed by
	 * the execution engine thread. Snapshot, Export and the
	 * corresponding CatalogDelegate may be reference count
	 * holders. The table is deleted when the refcount falls to
	 * zero. This allows longer running processes to complete
	 * gracefully after a table has been removed from the catalog.
	 */
	void incrementRefcount() {
		m_refcount += 1;
	}

	void decrementRefcount() {
		m_refcount -= 1;
		if (m_refcount == 0) {
			delete this;
		}
	}


	float shortestPath(int source, int destination, int costColumnId);
	Vertex* getVertex(int id);
	TableTuple* getVertexTuple(int id);
	Edge* getEdge(int id);
	TableTuple* getEdgeTuple(int id);
	void addVertex(int id, Vertex* vertex);
	void addEdge(int id, Edge* edge);
	int numOfVertexes();
	int numOfEdges();
	string name();
	string debug();
	bool isDirected();
	Table* getVertexTable();
	Table* getEdgeTable();
	Table* getPathTable();
	TupleSchema* getVertexSchema();
	TupleSchema* getEdgeSchema();
	TupleSchema* getPathSchema();
	string getPathsTableName() {return m_pathTableName; }
	void setVertexSchema(TupleSchema* s);
	void setEdgeSchema(TupleSchema* s);
	void setPathSchema(TupleSchema* s);

	int getVertexIdColumnIndex();
	int getEdgeIdColumnIndex();
	int getEdgeFromColumnIndex();
	int getEdgeToColumnIndex();
	int getColumnIdInVertexTable(int vertexAttributeId);
	int getColumnIdInEdgeTable(int edgeAttributeId);
	string getVertexAttributeName(int vertexAttributeId);
	string getEdgeAttributeName(int edgeAttributeId);

	//path related members
	//Notice that VoltDB allows one operation or query / one thread per time
	//Hence, we assume that a single path traversal query is active at any point in time
	PathIterator& iteratorDeletingAsWeGo(GraphOperationType opType);

	void expandCurrentPathOperation();


protected:
	void fillGraphFromRelationalTables();
	void constructPathSchema(); //constucts m_pathColumnNames and m_pathSchema
	void constructPathTempTable();
	std::map<int, Vertex* > m_vertexes;
	std::map<int, Edge* > m_edges;
	Table* m_vertexTable;
	Table* m_edgeTable;
	TempTable* m_pathTable;
	TableIterator* m_pathTableIterator;
	PathIterator* m_pathIterator;
	TupleSchema* m_vertexSchema; //will contain fanIn and fanOut as additional attributes
	TupleSchema* m_edgeSchema; //will contain startVertexId and endVertexId as additional attributes
	TupleSchema* m_pathSchema; //will contain startVertexId, endVertexId, and cost for now
	// schema as array of string names
	std::vector<std::string> m_vertexColumnNames;
	std::vector<std::string> m_edgeColumnNames;
	std::vector<std::string> m_pathColumnNames;
	std::vector<int> m_columnIDsInVertexTable;
	std::vector<int> m_columnIDsInEdgeTable;
	int m_vertexIdColumnIndex;
	int m_edgeIdColumnIndex;
	int m_edgeFromColumnIndex;
	int m_edgeToColumnIndex;
	string m_pathTableName = "PATHS_TEMP_TABLE";
	GraphOperationType currentPathOperationType;
	//TODO: this should be removed
	int dummyPathExapansionState = 0;
	// identity information
	CatalogId m_databaseId;
	std::string m_name;
	//SHA-1 of signature string
	char m_signature[20];
	//Mohamed: I think all the below ma not be needed as we will just reference the underlying tables

	/*TableTuple m_tempVertexTuple;
	TableTuple m_tempEdgeTuple;
	boost::scoped_array<char> m_tempVertexTupleMemory;
	boost::scoped_array<char> m_tempEdgeTupleMemory;

	TupleSchema* m_vertexSchema;
	TupleSchema* m_edgeSchema;

	// schema as array of string names
	std::vector<std::string> m_vertexColumnNames;
	std::vector<std::string> m_edgeColumnNames;
	char *m_vertexColumnHeaderData;
	char *m_edgeColumnHeaderData;
	int32_t m_vertexColumnHeaderSize;
	int32_t m_edgeColumnHeaderSize;
	*/

	bool m_isDirected;

	GraphView(void);

private:
    int32_t m_refcount;
    ThreadLocalPool m_tlPool;
    int m_compactionThreshold;
};

}

#endif
