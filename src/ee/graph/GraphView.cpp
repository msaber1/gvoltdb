#include "GraphView.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchemaBuilder.h"
#include "logging/LogManager.h"
#include "PathIterator.h"
#include "Vertex.h"
#include "Edge.h"
#include <string>
using namespace std;

namespace voltdb
{

GraphView::GraphView(void) //: m_pathIterator(this)
{
	m_pathIterator = new PathIterator(this);
}

float GraphView::shortestPath(int source, int destination, int costColumnId)
{
	//TODO: write real shortest path code that consults the edges table using costColumnId
	return (float)source * destination;
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

Table* GraphView::getPathTable()
{
	return this->m_pathTable;
}

int GraphView::numOfVertexes()
{
	return this->m_vertexes.size();
}

TupleSchema* GraphView::getVertexSchema()
{
	return m_vertexSchema;
}

TupleSchema* GraphView::getEdgeSchema()
{
	return m_edgeSchema;
}

TupleSchema* GraphView::getPathSchema()
{
	return m_pathSchema;
}

void GraphView::setVertexSchema(TupleSchema* s)
{
	m_vertexSchema = s;
}

void GraphView::setEdgeSchema(TupleSchema* s)
{
	m_edgeSchema = s;
}

void GraphView::setPathSchema(TupleSchema* s)
{
	m_pathSchema = s;
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

int GraphView::getVertexIdColumnIndex()
{
	return m_vertexIdColumnIndex;
}

int GraphView::getEdgeIdColumnIndex()
{
	return m_edgeIdColumnIndex;
}

int GraphView::getEdgeFromColumnIndex()
{
	return m_edgeFromColumnIndex;
}

int GraphView::getEdgeToColumnIndex()
{
	return m_edgeToColumnIndex;
}

int GraphView::getColumnIdInVertexTable(int vertexAttributeId)
{
	// -1 means FanOut
	// -2 means FanIn
	// -3 invalid
	// >= 0 means columnIndex
	//int numOfVertexTableColumns = this->m_vertexTable->columnCount();
	//if(vertexAttributeId >= numOfVertexTableColumns)
	return m_columnIDsInVertexTable[vertexAttributeId];
}

int GraphView::getColumnIdInEdgeTable(int edgeAttributeId)
{
	return m_columnIDsInEdgeTable[edgeAttributeId];
}

string GraphView::getVertexAttributeName(int vertexAttributeId)
{
	return m_vertexColumnNames[vertexAttributeId];
}

string GraphView::getEdgeAttributeName(int edgeAttributeId)
{
	return m_edgeColumnNames[edgeAttributeId];
}

void GraphView::constructPathSchema()
{
	//
	//Path tuple will contain 5 attributes
	//0: StartVertex, Integer
	//1: EndVertex, Integer
	//2: Length, Integer
	//3: Cost, Float
	//4: Path: Varchar(256)
	//add the column names
	m_pathColumnNames.clear();
	m_pathColumnNames.push_back("StartVertex");
	m_pathColumnNames.push_back("EndVertex");
	m_pathColumnNames.push_back("Length");
	m_pathColumnNames.push_back("Cost");
	m_pathColumnNames.push_back("Path");
	int numOfPathColumns = m_pathColumnNames.size();

	bool needsDRTimestamp = false; //TODO: we might revisit this
	TupleSchemaBuilder schemaBuilder(numOfPathColumns,
									 needsDRTimestamp ? 1 : 0); // number of hidden columns

	schemaBuilder.setColumnAtIndex (0, ValueType::VALUE_TYPE_INTEGER, 4, false, false); //StartVertex
	schemaBuilder.setColumnAtIndex(1, ValueType::VALUE_TYPE_INTEGER, 4, false, false); //EndVertex
	schemaBuilder.setColumnAtIndex(2, ValueType::VALUE_TYPE_INTEGER, 4, false, false); //Length
	schemaBuilder.setColumnAtIndex(3, ValueType::VALUE_TYPE_DOUBLE, 8, true, false); //Cost
	schemaBuilder.setColumnAtIndex(4, ValueType::VALUE_TYPE_VARCHAR, 1024, true, false); //Path

	m_pathSchema = schemaBuilder.build();
}

void GraphView::constructPathTempTable()
{
	m_pathTable = TableFactory::buildTempTable(m_pathTableName, m_pathSchema, m_pathColumnNames, NULL);
}

PathIterator& GraphView::iteratorDeletingAsWeGo(GraphOperationType opType)
{
	//empty the paths table, which is the staging memory for the paths to be explored
	dummyPathExapansionState = 0;
	m_pathTable->deleteAllTempTupleDeepCopies();
	//set the iterator for the temp table
	//create new tuple in the paths temp table
	//m_pathTableIterator = &(m_pathTable->iteratorDeletingAsWeGo());
	m_pathTableIterator = NULL;
	return *m_pathIterator;
}

void GraphView::expandCurrentPathOperation()
{
	//Check the current path operation type, and
	//advance the paths exploration accordingly
	//new entries should be added to the paths temp table
	//adding no new entries means that the exploration is completely done
	//and the iterator will have hasNext evaluated to false
	std::stringstream paramsToPrint;

	if(dummyPathExapansionState < 6)
	{
		//create new tuple in the paths temp table
		TableTuple temp_tuple = m_pathTable->tempTuple();
		//start vertex, end vertex, length, cost, path
		temp_tuple.setNValue(0, ValueFactory::getIntegerValue(dummyPathExapansionState + 6));
		temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dummyPathExapansionState + 11));
		temp_tuple.setNValue(2, ValueFactory::getIntegerValue(dummyPathExapansionState + 16));
		temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(dummyPathExapansionState + 21)));

		m_pathTable->insertTempTuple(temp_tuple);

		dummyPathExapansionState++;
	}
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


	std::stringstream paramsToPrint;
	paramsToPrint << " vertex column names = ";
	for(int i = 0; i < m_vertexColumnNames.size(); i++)
	{
		paramsToPrint << m_vertexColumnNames[i] << ", ";
	}
	paramsToPrint << " ### vertexTable ColIDs= ";
	for(int i = 0; i < m_columnIDsInVertexTable.size(); i++)
	{
		paramsToPrint << m_columnIDsInVertexTable[i] << ", ";
	}

	paramsToPrint << " ### edge column names = ";
	for(int i = 0; i < m_edgeColumnNames.size(); i++)
	{
		paramsToPrint << m_edgeColumnNames[i] << ", ";
	}
	paramsToPrint << " ### edgeTable ColIDs= ";
	for(int i = 0; i < m_columnIDsInEdgeTable.size(); i++)
	{
		paramsToPrint << m_columnIDsInEdgeTable[i] << ", ";
	}

	paramsToPrint << " ##### vertexId= " << m_vertexIdColumnIndex << ", edgeId= " << m_edgeIdColumnIndex
			<< "from = " << m_edgeFromColumnIndex << ", to = " << m_edgeToColumnIndex;

	LogManager::GLog("GraphView", "fill", 180, paramsToPrint.str());

	assert(m_vertexIdColumnIndex >= 0 && m_edgeIdColumnIndex >= 0 && m_edgeFromColumnIndex >= 0 && m_edgeToColumnIndex >=0);

	if (this->m_vertexTable->activeTupleCount() != 0)
	{
		while (iter.next(tuple))
		{
			if (tuple.isActive())
			{
				id = ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColumnIndex));
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
				id = ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeIdColumnIndex));
				from = ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeFromColumnIndex));
				to = ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeToColumnIndex));
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
