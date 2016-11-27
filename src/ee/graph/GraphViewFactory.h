#ifndef GRAPHVIEWFACTORY_H
#define GRAPHVIEWFACTORY_H

//#include "GraphView.h"
#include "Edge.h"
#include "Vertex.h"
#include <string>

namespace catalog {
class GraphView;
}

namespace voltdb {

class GraphView;

class GraphViewFactory
{
public:
	GraphViewFactory(void);
	~GraphViewFactory(void);

	static GraphView* createGraphView(string graphViewName, bool isDirected);
	static GraphView* createGraphView(const catalog::GraphView &catalogGraphView,
	           voltdb::CatalogId databaseId, Table* vTable, Table* eTable, char *signature);
	static void loadGraph(GraphView* vw, vector<Vertex* > vertexes, vector<Edge* > edges);
	static void printGraphView(GraphView* gview);
};

}

#endif
