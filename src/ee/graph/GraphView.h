#ifndef GRAPHVIEW_H
#define GRAPHVIEW_H

#include <map>
#include <string>
using namespace std;

namespace voltdb {

//#include "vertex.h"
class Vertex;
class Edge;

class GraphView
{
protected:
	std::map<int, Vertex* > m_vertexes;
	std::map<int, Edge* > m_edges;
	string m_name;
	bool m_isDirected;
	GraphView(void);

public:
	~GraphView(void);

	Vertex* getVertex(int id);
	Edge* getEdge(int id);
	void addVertex(int id, Vertex* vertex);
	void addEdge(int id, Edge* edge);
	int numOfVertexes();
	int numOfEdges();
	string getName();
	bool isDirected();

	friend class GraphViewFactory;
};

}

#endif
