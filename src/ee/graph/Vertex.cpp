#include "Vertex.h"
#include <sstream>
#include <algorithm>

namespace voltdb {


Vertex::Vertex(void)
{
}

int Vertex::fanOut()
{
	return this->m_outEdgeIds.size();
}

int Vertex::fanIn()
{
	return this->m_inEdgeIds.size();
}

int Vertex::getOutEdgeId(int index)
{
	return this->m_outEdgeIds[index];
}

int Vertex::getInEdgeId(int index)
{
	return this->m_inEdgeIds[index];
}

Edge* Vertex::getOutEdge(int index)
{
	return this->m_gview->getEdge(this->m_outEdgeIds[index]);
}

void Vertex::addOutEdge(Edge* edge)
{
	//add it if it does not exist
	if(std::find(this->m_outEdgeIds.begin(), this->m_outEdgeIds.end(), edge->getId()) == this->m_outEdgeIds.end()) 
	{
		this->m_outEdgeIds.push_back(edge->getId());
	}
}
	
void Vertex::addInEdge(Edge* edge)
{
	//add it if it does not exist
	if(std::find(this->m_inEdgeIds.begin(), this->m_inEdgeIds.end(), edge->getId()) == this->m_inEdgeIds.end()) 
	{
		this->m_inEdgeIds.push_back(edge->getId());
	}
}

Edge* Vertex::getInEdge(int index)
{
	return this->m_gview->getEdge(this->m_inEdgeIds[index]);
}

string Vertex::toString()
{
	std::ostringstream stream;
	stream << "(id = " << this->getId() << ", fanOut = " << this->fanOut() << ", fanIn = " << this->fanIn() << ")"; 
	return stream.str();
}

Vertex::~Vertex(void)
{
}

}
