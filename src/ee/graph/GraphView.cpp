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

#include <queue>
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
	m_pathColumnNames.push_back("STARTVERTEXID");
	m_pathColumnNames.push_back("ENDVERTEXID");
	m_pathColumnNames.push_back("LENGTH");
	m_pathColumnNames.push_back("COST");
	m_pathColumnNames.push_back("PATH");
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
	executeTraversal = true;
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
	//std::stringstream paramsToPrint;
	/*
	if(dummyPathExapansionState < 6)
	{
		//create new tuple in the paths temp table
		TableTuple temp_tuple = m_pathTable->tempTuple();
		//start vertex, end vertex, length, cost, path
		temp_tuple.setNValue(0, ValueFactory::getIntegerValue(dummyPathExapansionState + 6));
		temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dummyPathExapansionState + 11));
		temp_tuple.setNValue(2, ValueFactory::getIntegerValue(dummyPathExapansionState + 16));
		temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(dummyPathExapansionState + 21)));
		temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
		m_pathTable->insertTempTuple(temp_tuple);
		paramsToPrint << "current tuple count in m_pathTable = " << m_pathTable->activeTupleCount();
		LogManager::GLog("GraphView", "expandCurrentPathOperation", 242,
									paramsToPrint.str());
		paramsToPrint.clear();
		dummyPathExapansionState++;
	}
	*/
	if(executeTraversal)
	{
		switch(this->queryType)
		{
		//reachability, BFS,...
		case 1: //reachability BFS without selectivity
			this->BFS_Reachability_ByDepth(this->fromVertexId, this->pathLength);
			break;
		case 2: //reachaility BFS with edge selectivity
			this->BFS_Reachability_ByDepth_eSelectivity(this->fromVertexId, this->pathLength, this->eSelectivity);
			break;
		case 3: //reachability BFS with start and end
			this->BFS_Reachability_ByDestination(this->fromVertexId, this->toVertexId);
			break;
		//topological queries
		case 11: //vOnly selectivity
			this->SubGraphLoop(this->pathLength, this->vSelectivity, 100);
			break;
		case 12: //eOnly selectivity
			this->SubGraphLoop(this->pathLength, 100, this->eSelectivity);
					break;
		case 13: //vertex and edge selectivity
			this->SubGraphLoop(this->pathLength, this->vSelectivity, this->eSelectivity);
			break;
		//shortest paths
		case 21: //top k shortest paths
			this->SP_TopK(this->fromVertexId, this->toVertexId, this->topK);
			break;
		case 22: //top 1 shortest path with edge selectivity
			this->SP_EdgeSelectivity(this->fromVertexId, this->toVertexId, this->eSelectivity);
			break;
		}
		executeTraversal = false;
	}
}

void GraphView::SP_TopK(int src, int dest, int k)
{
	int minCost = INT_MAX;
	int foundPathsSoFar = 0;
	priority_queue<PQEntry, vector<PQEntry>, std::greater<PQEntry>> pq;
	//map<int, int> costMap;
	//costMap[src] = 0;
	pq.push(make_pair(0, src)); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	int currVId, fanOut = -1, candVertexId = -1;

	while(!pq.empty())
	{
		//select next vertex to explore
		currVId = pq.top().second;
		minCost = pq.top().first;
		if(currVId == dest)
		{
			foundPathsSoFar++;

			//add a tuple here
			TableTuple temp_tuple = m_pathTable->tempTuple();
			//start vertex, end vertex, length, cost, path
			temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src));
			temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest));
			temp_tuple.setNValue(2, ValueFactory::getIntegerValue(minCost));
			temp_tuple.setNValue(3, ValueFactory::getDoubleValue(minCost));
			//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
			m_pathTable->insertTempTuple(temp_tuple);
		}
		if(foundPathsSoFar == k)
		{
			break;
		}
		pq.pop();

		//explore the outgoing vertexes
		v = this->getVertex(currVId);
		fanOut = v->fanOut();
		for(int i = 0; i < fanOut; i++)
		{
			e = v->getOutEdge(i);
			candVertexId = e->getEndVertex()->getId();

			//these lines are commented to allow top k search
			//if ( (costMap.find(candVertexId) == costMap.end()) ||
			//	 (costMap[candVertexId] > minCost + 1) )
			{
				//costMap[candVertexId] = minCost + 1;
				pq.push(make_pair(minCost + 1, candVertexId));
			}

		}
	}

	std::stringstream paramsToPrint;
	paramsToPrint << "TopK SP: from = " << src << ", to = " << dest
			<< ", k = " << k
			<< ", foundPaths = " << foundPathsSoFar
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "TopK_SP", 334, paramsToPrint.str());
}

void GraphView::SP_EdgeSelectivity(int src, int dest, int edgeSelectivity)
{
	int minCost = INT_MAX;
	priority_queue<PQEntry, vector<PQEntry>, std::greater<PQEntry>> pq;
	map<int, int> costMap;
	costMap[src] = 0;
	pq.push(make_pair(0, src)); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	int currVId, fanOut = -1, candVertexId = -1;

	while(!pq.empty())
	{
		//select next vertex to explore
		currVId = pq.top().second;
		minCost = pq.top().first;
		if(currVId == dest)
		{
			//add a tuple here
			TableTuple temp_tuple = m_pathTable->tempTuple();
			//start vertex, end vertex, length, cost, path
			temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src));
			temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest));
			temp_tuple.setNValue(2, ValueFactory::getIntegerValue(minCost));
			temp_tuple.setNValue(3, ValueFactory::getDoubleValue(minCost));
			//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
			m_pathTable->insertTempTuple(temp_tuple);
			break;
		}
		pq.pop();

		//explore the outgoing vertexes
		v = this->getVertex(currVId);
		fanOut = v->fanOut();
		for(int i = 0; i < fanOut; i++)
		{
			e = v->getOutEdge(i);
			candVertexId = e->getEndVertex()->getId();

			if(e->eProp > eSelectivity)
			{
				continue;
			}

			if ( (costMap.find(candVertexId) == costMap.end()) ||
				 (costMap[candVertexId] > minCost + 1) )
			{
				costMap[candVertexId] = minCost + 1;
				pq.push(make_pair(minCost + 1, candVertexId));
			}

		}
	}

	std::stringstream paramsToPrint;
	paramsToPrint << "SP with eSelectivity: from = " << src << ", to = " << dest
			<< ", eSelectivity = " << edgeSelectivity
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "SP_eSelectivity", 398, paramsToPrint.str());
}

void GraphView::BFS_Reachability_ByDepth_eSelectivity(int startVertexId, int depth, int eSelectivity)
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	if(NULL != currentVertex)
	{
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < depth)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				if(outEdge->eProp > eSelectivity)
				{
					continue;
				}
				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if( (depth > 0 && outVertex->Level == depth))
				{
					//Now, we reached the destination vertexes, where we should add tuples into the output table
					TableTuple temp_tuple = m_pathTable->tempTuple();
					//start vertex, end vertex, length, cost, path
					temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
					temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
					temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(outVertex->Level + 1)));
					//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
					m_pathTable->insertTempTuple(temp_tuple);
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}

	std::stringstream paramsToPrint;
	paramsToPrint << "BFS_ByDepth: from = " << startVertexId << ", depth = " << depth << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS_ByDepth", 463, paramsToPrint.str());
}

void GraphView::BFS_Reachability_ByDepth(int startVertexId, int depth)
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	if(NULL != currentVertex)
	{
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < depth)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if( (depth > 0 && outVertex->Level == depth))
				{
					//Now, we reached the destination vertexes, where we should add tuples into the output table
					TableTuple temp_tuple = m_pathTable->tempTuple();
					//start vertex, end vertex, length, cost, path
					temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
					temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
					temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(outVertex->Level + 1)));
					//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
					m_pathTable->insertTempTuple(temp_tuple);
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}

	std::stringstream paramsToPrint;
	paramsToPrint << "BFS_ByDepth: from = " << startVertexId << ", depth = " << depth << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS_ByDepth", 463, paramsToPrint.str());
}

void GraphView::BFS_Reachability_ByDestination(int startVertexId, int destVerexId)
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	if(NULL != currentVertex)
	{
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		bool found = false;
		while(!q.empty() && !found)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				outVertex = outEdge->getEndVertex();
				if(outVertex->getId() == destVerexId)
				{
					//Now, we reached the destination vertexes, where we should add tuples into the output table
					TableTuple temp_tuple = m_pathTable->tempTuple();
					//start vertex, end vertex, length, cost, path
					temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
					temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
					temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(outVertex->Level + 1)));
					temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
					m_pathTable->insertTempTuple(temp_tuple);

					found = true;
					break;
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}
	executeTraversal = false;

	std::stringstream paramsToPrint;
	paramsToPrint << "BFS_Reachability_ByDestination: from = " << startVertexId << ", to = " << destVerexId << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS_Reachability", 513, paramsToPrint.str());
}

void GraphView::SubGraphLoop(int length, int vSelectivity, int eSelectivity)
{
	queue<Vertex*> q;
	Vertex* currentVertex = NULL;
	int startVertexId = -1;
	for(std::map<int, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
	{
		currentVertex = it->second;
		if(currentVertex->vProp > vSelectivity)
		{
			cout << "skipping vertex " << currentVertex->getId() << endl;
			continue;
		}
		startVertexId = currentVertex->getId();
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < length)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);

				if(outEdge->eProp > eSelectivity)
				{
					cout << "skipping edge " << outEdge->getId() << endl;
					continue;
				}

				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if(outVertex->Level == length)
				{
					//we found a loop of the desired length
					if(outVertex->getId() == startVertexId)
					{
						//Now, we reached the destination vertexes, where we should add tuples into the output table
						TableTuple temp_tuple = m_pathTable->tempTuple();
						//start vertex, end vertex, length, cost, path
						//set the start vertex to the vertex having an edge that closes the loop (for debugging purposes)
						temp_tuple.setNValue(0, ValueFactory::getIntegerValue(outEdge->getStartVertex()->getId()));
						temp_tuple.setNValue(1, ValueFactory::getIntegerValue(startVertexId));
						temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
						temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
						//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
						m_pathTable->insertTempTuple(temp_tuple);
					}
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}

		//empty the queue
		while(!q.empty())
		{
			q.pop();
		}
	}

	std::stringstream paramsToPrint;
	paramsToPrint << "SubGraphLoop: length = " << length << ", vSelectivity = " << vSelectivity
			<< ", eSelectivity = " << eSelectivity
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "SubGraphLoop", 625, paramsToPrint.str());
}

void GraphView::SubGraphLoop(int startVertexId, int length)
{
	queue<Vertex*> q;
	Vertex* currentVertex = NULL;
	if(startVertexId >= 0)
	{
		currentVertex = this->m_vertexes[startVertexId];
	}
	if(NULL != currentVertex)
	{
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < length)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if(outVertex->Level == length)
				{
					//we found a loop of the desired length
					if(outVertex->getId() == startVertexId)
					{
						//Now, we reached the destination vertexes, where we should add tuples into the output table
						TableTuple temp_tuple = m_pathTable->tempTuple();
						//start vertex, end vertex, length, cost, path
						temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
						temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
						temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
						temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
						//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
						m_pathTable->insertTempTuple(temp_tuple);
					}
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}
	else
	{
		for (std::map<int, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
		{
			currentVertex = it->second;
			currentVertex->Level = 0;
			q.push(currentVertex);
			int fanOut;
			Edge* outEdge = NULL;
			Vertex* outVertex = NULL;
			while(!q.empty() && currentVertex->Level < length)
			{
				currentVertex = q.front();
				q.pop();
				fanOut = currentVertex->fanOut();
				for(int i = 0; i < fanOut; i++)
				{
					outEdge = currentVertex->getOutEdge(i);
					outVertex = outEdge->getEndVertex();
					outVertex->Level = currentVertex->Level + 1;
					if(outVertex->Level == length)
					{
						//we found a loop of the desired length
						if(outVertex->getId() == startVertexId)
						{
							//Now, we reached the destination vertexes, where we should add tuples into the output table
							TableTuple temp_tuple = m_pathTable->tempTuple();
							//start vertex, end vertex, length, cost, path
							temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
							temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
							temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
							temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
							//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
							m_pathTable->insertTempTuple(temp_tuple);
						}
					}
					else
					{
						//add to the queue, as currentDepth is less than depth
						q.push(outVertex);
					}
				}
			}
			//empty the queue
			while(!q.empty())
			{
				q.pop();
			}
		}
	}
	executeTraversal = false;

	std::stringstream paramsToPrint;
	paramsToPrint << "SubGraphLoop: from = " << startVertexId << ", length = " << length << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS", 302, paramsToPrint.str());
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
			<< "from = " << m_edgeFromColumnIndex << ", to = " << m_edgeToColumnIndex
			<< "vPropColIndex = " << m_vPropColumnIndex << ", ePropColIndex = " << m_ePropColumnIndex;

	LogManager::GLog("GraphView", "fill", 785, paramsToPrint.str());

	assert(m_vertexIdColumnIndex >= 0 && m_edgeIdColumnIndex >= 0 && m_edgeFromColumnIndex >= 0 && m_edgeToColumnIndex >=0);

	bool vPropExists = (m_vPropColumnIndex >= 0);
	bool ePropExists = (m_ePropColumnIndex >= 0);

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
				if(vPropExists)
				{
					vertex->vProp = ValuePeeker::peekInteger(tuple.getNValue(m_vPropColumnIndex));
				}
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
				if(ePropExists)
				{
					edge->eProp = ValuePeeker::peekInteger(edgeTuple.getNValue(m_ePropColumnIndex));
				}
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
