#include "GraphView.h"

namespace voltdb
{

GraphView::GraphView(void)
{
}

Vertex* GraphView::getVertex(int id)
{
	return this->m_vertexes[id];
}

Edge* GraphView::getEdge(int id)
{
	return this->m_edges[id];
}

void GraphView::addVertex(int id, Vertex* vertex)
{
	this->m_vertexes[id] = vertex;
}
	
void GraphView::addEdge(int id, Edge* edge)
{
	this->m_edges[id] = edge;
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

GraphView::~GraphView(void)
{
}

}
