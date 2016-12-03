#include "GraphView.h"
#include "storage/tableiterator.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "logging/LogManager.h"
#include "Vertex.h"
#include "Edge.h"
#include <string>
using namespace std;

namespace voltdb
{

GraphView::GraphView(void)
{
}

Vertex* GraphView::getVertex(int id)
{
	return this->m_vertexes[id];
}

TableTuple* GraphView::getVertexTuple(int id)
{
	Vertex* v = this->getVertex(id);
	return new TableTuple(v->getTupleData(), this->m_vertexTable->schema());
}

Edge* GraphView::getEdge(int id)
{
	return this->m_edges[id];
}

TableTuple* GraphView::getEdgeTuple(int id)
{
	Edge* e = this->getEdge(id);
	return new TableTuple(e->getTupleData(), this->m_edgeTable->schema());
}

void GraphView::addVertex(int id, Vertex* vertex)
{
	this->m_vertexes[id] = vertex;
}
	
void GraphView::addEdge(int id, Edge* edge)
{
	this->m_edges[id] = edge;
}

Table* GraphView::getVertexTable()
{
	return this->m_vertexTable;
}

Table* GraphView::getEdgeTable()
{
	return this->m_edgeTable;
}

int GraphView::numOfVertexes()
{
	return this->m_vertexes.size();
}
	
int GraphView::numOfEdges()
{
	return this->m_edges.size();
}

string GraphView::name()
{
	return m_name;
}
	
bool GraphView::isDirected()
{
	return m_isDirected;
}

void GraphView::fillGraphFromRelationalTables()
{
	this->m_vertexes.clear();
	this->m_edges.clear();
	//fill the vertex collection
	TableIterator iter = this->m_vertexTable->iterator();
	const TupleSchema* schema = this->m_vertexTable->schema();
	TableTuple tuple(schema);
	int id, from, to;
	from = -1;
	to = -1;
	Vertex* vertex = NULL;
	Edge* edge = NULL;

	if (this->m_vertexTable->activeTupleCount() != 0)
	{
		while (iter.next(tuple))
		{
			if (tuple.isActive())
			{
				id = ValuePeeker::peekInteger(tuple.getNValue(0));
				vertex = new Vertex();
				vertex->setGraphView(this);
				vertex->setId(id);
				vertex->setTupleData(tuple.address());
				this->addVertex(id, vertex);
				//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 77, "vertex: " + vertex->toString());
			}
		}
	}
	//fill the edge collection
	iter = this->m_edgeTable->iterator();
	schema = this->m_edgeTable->schema();
	TableTuple edgeTuple(schema);
	Vertex* vFrom = NULL;
	Vertex* vTo = NULL;
	if (this->m_edgeTable->activeTupleCount() != 0)
	{
		while (iter.next(edgeTuple))
		{
			if (edgeTuple.isActive())
			{
				id = ValuePeeker::peekInteger(edgeTuple.getNValue(0));
				from = ValuePeeker::peekInteger(edgeTuple.getNValue(1));
				to = ValuePeeker::peekInteger(edgeTuple.getNValue(2));
				edge = new Edge();
				edge->setGraphView(this);
				edge->setId(id);
				edge->setTupleData(edgeTuple.address());
				edge->setStartVertexId(from);
				edge->setEndVertexId(to);
				//update the endpoint vertexes in and out lists
				vFrom = edge->getStartVertex();
				vTo = edge->getEndVertex();
				vFrom->addOutEdge(edge);
				vTo->addInEdge(edge);
				if(!this->isDirected())
				{
					vTo->addOutEdge(edge);
					vFrom->addInEdge(edge);
				}
				this->addEdge(id, edge);
			}
		}
	}
	LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 159, "graph: " + this->debug());
	//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 73, "vTable: " + this->m_vertexTable->debug());
	//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 73, "eTable: " + this->m_edgeTable->debug());

}

string GraphView::debug()
{
	std::stringstream output;
	output << "Name: " << this->name() << endl;
	output << "Is directed? = " << this->isDirected() << endl;
	int vCount, eCount;
	vCount = this->numOfVertexes();
	eCount = this->numOfEdges();
	output << "#Vertexes = " << vCount << endl;
	output << "#Edges = " << eCount << endl;
	output << "Vertexes" << endl;
	Vertex* currentVertex;
	for (std::map<int,Vertex* >::iterator it= this->m_vertexes.begin(); it != this->m_vertexes.end(); ++it)
	{
		currentVertex = it->second;
		output << "\t" << currentVertex->toString() << endl;
		output << "\t\t" << "out: " << endl;
		for(int j = 0; j < currentVertex->fanOut(); j++)
		{
			output << "\t\t\t" << currentVertex->getOutEdge(j)->toString() << endl;
		}
		output << "\t\t" << "in: " << endl;
		for(int j = 0; j < currentVertex->fanIn(); j++)
		{
			output << "\t\t\t" << currentVertex->getInEdge(j)->toString() << endl;
		}
	}
	return output.str();
}

GraphView::~GraphView(void)
{
}

}
