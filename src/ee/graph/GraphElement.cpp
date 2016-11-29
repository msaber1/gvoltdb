#include "GraphElement.h"
//#include "common/tabletuple.h"

namespace voltdb {

GraphElement::GraphElement(int id, char* tupleData, GraphView* graphView, bool remote)
{
	this->m_id = id;
	this->m_tupleData = tupleData;
	this->m_gview = graphView;
	this->m_isRemote = remote;
}

GraphElement::GraphElement(void)
{
	this->m_id = -1;
	this->m_tupleData = NULL;
	this->m_gview = NULL;
	this->m_isRemote = false;
}


bool GraphElement::isRemote()
{
	return m_isRemote;
}


void GraphElement::setId(int id)
{
	this->m_id = id;
}

int GraphElement::getId()
{
	return this->m_id;
}

void GraphElement::setTupleData(char* tupleData)
{
	this->m_tupleData = tupleData;
}

char* GraphElement::getTupleData()
{
	return this->m_tupleData;
}

GraphView* GraphElement::getGraphView()
{
	return this->m_gview;
}


void GraphElement::setGraphView(GraphView* gView)
{
	this->m_gview = gView;
}

GraphElement::~GraphElement(void)
{
}

}
