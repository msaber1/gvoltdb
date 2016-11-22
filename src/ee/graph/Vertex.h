#ifndef VERTEX_H
#define VERTEX_H

#include "GraphElement.h"
#include "Edge.h"
#include <vector>
using namespace std;

namespace voltdb {

class Vertex 
	: public GraphElement
{
protected:
	//mohamed: we are using ids instead of pointers for two reasons
	// 1) this will abstract referencing the graph elements from thier memory locations (e.g., we can easily relocate the edges in the memory)
	// 2) graph elements may be hosted by other remote machines
	vector<int> m_outEdgeIds;
	vector<int> m_inEdgeIds;

public:
	Vertex(void);
	~Vertex(void);

	int fanOut();
	int fanIn();
	int getOutEdgeId(int index);
	int getInEdgeId(int index);
	Edge* getOutEdge(int index);
	Edge* getInEdge(int index);
	void addOutEdge(Edge* edge);
	void addInEdge(Edge* edge);
	string toString();
};

}

#endif
