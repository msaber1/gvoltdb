#include "GraphViewFactory.h"
#include "catalog/graphview.h"
#include "GraphView.h"
#include "logging/LogManager.h"
#include <iostream>

using namespace std;

namespace voltdb {

GraphView* GraphViewFactory::createGraphView(string graphViewName, bool isDirected)
{
	GraphView* vw = new GraphView();
	vw->m_name = graphViewName;
	vw->m_isDirected = isDirected;
	//vw->constructPathSchema();
	return vw;
}

GraphView* GraphViewFactory::createGraphView(const std::string &graphViewName, const bool isDirected,
		   Table* vTable, Table* eTable, Table* pTable, TupleSchema* vSchema, TupleSchema* eSchema,
		   vector<std::string> vertexColumnNames, vector<std::string> edgeColumnNames,
		   vector<int> columnIdsInVertexTable, vector<int> columnIdsInEdgeTable,
           voltdb::CatalogId databaseId, char *signature)
{
	GraphView* vw = new GraphView();
	vw->m_name = graphViewName;
	vw->m_isDirected = isDirected;
	vw->m_vertexTable = vTable;
	vw->m_edgeTable = eTable;
	//construct the path schema
	vw->constructPathSchema();
	//construct the path temp table
	vw->constructPathTempTable();

	//set the vertex column names
	int vColumnCount = vSchema->columnCount();
	vw->m_vertexColumnNames.resize(vColumnCount);

	for(int i = 0; i < vColumnCount; i++)
	{
		vw->m_vertexColumnNames[i] = vertexColumnNames[i];
	}

	int colCountInVTable = columnIdsInVertexTable.size();
	vw->m_columnIDsInVertexTable.resize(colCountInVTable);

	for(int i = 0; i < colCountInVTable; i++)
	{
		vw->m_columnIDsInVertexTable[i] = columnIdsInVertexTable[i];
	}

	//set the edges columns
	int eColumnCount = eSchema->columnCount();
	vw->m_edgeColumnNames.resize(eColumnCount);

	for(int i = 0; i < eColumnCount; i++)
	{
		vw->m_edgeColumnNames[i] = edgeColumnNames[i];
	}

	int colCountInETable = columnIdsInEdgeTable.size();
	vw->m_columnIDsInEdgeTable.resize(colCountInETable);

	for(int i = 0; i < colCountInETable; i++)
	{
		vw->m_columnIDsInEdgeTable[i] = columnIdsInEdgeTable[i];
	}

	for(int i = 0; i < vw->m_vertexColumnNames.size(); i++)
	{
		if (vw->m_vertexColumnNames[i] == "ID")
		{
			vw->m_vertexIdColumnIndex = vw->m_columnIDsInVertexTable[i];
			break;
		}
	}

	for(int i = 0; i < vw->m_edgeColumnNames.size(); i++)
	{
		if (vw->m_edgeColumnNames[i] == "ID")
		{
			vw->m_edgeIdColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
		else if (vw->m_edgeColumnNames[i] == "FROM")
		{
			vw->m_edgeFromColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
		else if (vw->m_edgeColumnNames[i] == "TO")
		{
			vw->m_edgeToColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
	}

	vw->m_databaseId = databaseId;
	::memcpy(&(vw->m_signature), signature, 20);

	vw->fillGraphFromRelationalTables();

	return vw;
}

/*
GraphView* GraphViewFactory::createGraphView(const catalog::GraphView &catalogGraphView,
           voltdb::CatalogId databaseId, Table* vTable, Table* eTable, char *signature)
{
	GraphView* vw = new GraphView();
	vw->m_name = catalogGraphView.name();
	//TODO: msaber should check this with tatiana, the directed attribute is not communicated write from the FE
	vw->m_isDirected = catalogGraphView.isDirected();
	//vw->m_isDirected = true;
	vw->m_vertexTable = vTable;
	vw->m_edgeTable = eTable;
	vw->m_databaseId = databaseId;
	::memcpy(&(vw->m_signature), signature, 20);

	vw->fillGraphFromRelationalTables();

	return vw;
}
*/


void GraphViewFactory::loadGraph(GraphView* vw, vector<Vertex* > vertexes, vector<Edge* > edges)
{
	int vCount = vertexes.size();
	int eCount = edges.size();

	for(int i = 0; i < vCount; i++)
	{
		vertexes[i]->setGraphView(vw);
		vw->addVertex( vertexes[i]->getId(), vertexes[i]);
	}

	Vertex* from;
	Vertex* to;

	for(int i = 0; i < eCount; i++)
	{
		edges[i]->setGraphView(vw);
		vw->addEdge(edges[i]->getId(), edges[i]);
		//update the endpoint vertexes in and out lists
		from = edges[i]->getStartVertex();
		to = edges[i]->getEndVertex();
		from->addOutEdge(edges[i]);
		to->addInEdge(edges[i]);
	}
}

void GraphViewFactory::printGraphView(GraphView* gview)
{
	cout << "Name: " << gview->name() << endl;
	int vCount, eCount;
	vCount = gview->numOfVertexes();
	eCount = gview->numOfEdges();
	cout << "#Vertexes = " << vCount << endl;
	cout << "#Edges = " << eCount << endl;
	cout << "Vertexes" << endl;
	Vertex* currentVertex;
	for(int i = 0; i < vCount; i++)
	{
		currentVertex = gview->getVertex(i);
		cout << "\t" << currentVertex->toString() << endl;
		cout << "\t\t" << "out: " << endl;
		for(int j = 0; j < currentVertex->fanOut(); j++)
		{
			cout << "\t\t\t" << currentVertex->getOutEdge(j)->toString() << endl;
		}
		cout << "\t\t" << "in: " << endl;
		for(int j = 0; j < currentVertex->fanIn(); j++)
		{
			cout << "\t\t\t" << currentVertex->getInEdge(j)->toString() << endl;
		}
	}
}


GraphViewFactory::~GraphViewFactory(void)
{
}

}
