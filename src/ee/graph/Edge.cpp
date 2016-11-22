#include "Edge.h"
#include <string>
#include <sstream>

using namespace std;

namespace voltdb {


Edge::Edge(void)
{
}

int Edge::getStartVertexId()
{
	return m_startVertexId;
}
	
int Edge::getEndVertexId()
{
	return m_endVertexId;
}
	
Vertex* Edge::getStartVertex()
{
	return this->m_gview->getVertex(this->m_startVertexId);
}
	
Vertex* Edge::getEndVertex()
{
	return this->m_gview->getVertex(this->m_endVertexId);
}

string Edge::toString()
{
	std::ostringstream stream;
	stream << "(id = " << this->getId() << ", from = " << this->m_startVertexId << ", to = " <<
		this->m_endVertexId << ")";
	return stream.str();
}

void Edge::setStartVertexId(int id)
{
	this->m_startVertexId = id;
}

void Edge::setEndVertexId(int id)
{
	this->m_endVertexId = id;
}

Edge::~Edge(void)
{
}

}
