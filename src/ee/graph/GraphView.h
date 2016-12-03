#ifndef GRAPHVIEW_H
#define GRAPHVIEW_H

#include <map>
#include <string>
#include "storage/table.h"
using namespace std;

namespace voltdb {

//#include "vertex.h"
class Vertex;
class Edge;

class GraphView
{
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

	friend class GraphViewFactory;

protected:
	void fillGraphFromRelationalTables();
	std::map<int, Vertex* > m_vertexes;
	std::map<int, Edge* > m_edges;
	Table* m_vertexTable;
	Table* m_edgeTable;
	TupleSchema* m_vertexSchema; //will contain fanIn and fanOut as additional attributes
	TupleSchema* m_edgeSchema; //will contain startVertexId and endVertexId as additional attributes
	TupleSchema* m_pathSchema; //will contain startVertexId, endVertexId, and cost for now (this should change dynamically per query and should not be placed here at this level)
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
