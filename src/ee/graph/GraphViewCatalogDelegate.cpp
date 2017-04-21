/*
 * GraphViewCatalogDelegate.cpp
 *
 *  Created on: Nov 23, 2016
 *      Author: msaberab
 */

#include "GraphViewCatalogDelegate.h"

#include "catalog/catalog.h"
#include "catalog/database.h"


#include "GraphView.h"
#include "GraphViewFactory.h"
#include "catalog/graphview.h"
#include "sha1/sha1.h"
#include "logging/LogManager.h"
#include "common/TupleSchemaBuilder.h"

#include "catalog/table.h"
#include "catalog/column.h"
#include "catalog/columnref.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

#include <string>
#include <vector>
#include <map>

using namespace std;

namespace voltdb {

GraphViewCatalogDelegate::~GraphViewCatalogDelegate()
{
	if (m_graphView)
	{
		m_graphView->decrementRefcount();
	}
}

GraphView *GraphViewCatalogDelegate::getGraphView() const {
	return m_graphView;
}

void GraphViewCatalogDelegate::init(catalog::Database const &catalogDatabase,
	            catalog::GraphView const &catalogGraphView, Table* vTable, Table* eTable, Table* pTable)
{
	m_graphView = constructGraphViewFromCatalog(catalogDatabase,
	                                        catalogGraphView, vTable, eTable, pTable);
	if (!m_graphView) {
	        return;
	}

	/*
	evaluateExport(catalogDatabase, catalogTable);

	// configure for stats tables
	PersistentTable* persistenttable = dynamic_cast<PersistentTable*>(m_table);
	if (persistenttable) {
	      persistenttable->configureIndexStats();
	}
	*/
	m_graphView->incrementRefcount();
}

GraphView *GraphViewCatalogDelegate::constructGraphViewFromCatalog(catalog::Database const &catalogDatabase,
	                                     catalog::GraphView const &catalogGraphView,
	                                     Table* vTable, Table* eTable, Table* pTable)
{
	LogManager::GLog("GraphViewCatalogDelegate", "constructGraphViewFromCatalog", 71, "graphViewName = " + catalogGraphView.name());
	// Create a persistent graph view for this table in our catalog
	int32_t graphView_id = catalogGraphView.relativeIndex();
	std::stringstream params;
	params << " graphView_id (relative index) = " << graphView_id;
	LogManager::GLog("GraphViewCatalogDelegate", "GraphViewCatalogDelegate", 68, params.str());

	// get an array of the vertex column names
	int numColumns = catalogGraphView.VertexProps().size();
	map<string, catalog::Column*>::const_iterator col_iterator;
	vector<string> columnNamesVertex(numColumns);
	vector<int> columnIdsInVertexTable(catalogGraphView.VTable()->columns().size());
	int colIndex = 0;
	for (col_iterator = catalogGraphView.VertexProps().begin();
		 col_iterator != catalogGraphView.VertexProps().end();
		 col_iterator++)
	{
		const catalog::Column *catalog_column = col_iterator->second;
		if(catalog_column->matviewsource())
		{
			//colIndex = catalog_column->index();
			columnNamesVertex[colIndex] = catalog_column->name();

			std::stringstream params;
			params << "Graph vCol Index = " << colIndex
					<< ", Graph vCol Name = " << catalog_column->name();



			columnIdsInVertexTable[colIndex] = catalog_column->matviewsource()->index();

			params << ", VertexTable Column Index = " << catalog_column->matviewsource()->index();


			LogManager::GLog("GraphViewCatalogDelegate", "constructGraphViewFromCatalog", 108, params.str());
			colIndex++;
		}
	}


	colIndex = 0;
	numColumns = catalogGraphView.EdgeProps().size();
	vector<string> columnNamesEdge(numColumns);
	vector<int> columnIdsInEdgeTable(catalogGraphView.ETable()->columns().size());
	for (col_iterator = catalogGraphView.EdgeProps().begin();
			 col_iterator != catalogGraphView.EdgeProps().end();
			 col_iterator++)
	{
		const catalog::Column *catalog_column = col_iterator->second;
		if(catalog_column->matviewsource())
		{
			//colIndex = catalog_column->index();
			columnNamesEdge[colIndex] = catalog_column->name();

			std::stringstream params;
			params << "Graph eCol Index = " << colIndex
					<< ", Graph eCol Name = " << catalog_column->name();



			columnIdsInEdgeTable[colIndex] = catalog_column->matviewsource()->index();

			params << ", EdgeTable Column Index = " << catalog_column->matviewsource()->index();


			LogManager::GLog("GraphViewCatalogDelegate", "constructGraphViewFromCatalog", 139, params.str());
			colIndex++;
		}
	}


	// get the schema
	//TODO: we will uncomment the code below when the graph attributes indexes are fixed
	//TupleSchema *vSchema = createOutputVertexTupleSchema(catalogDatabase, catalogGraphView);
	//TupleSchema *eSchema = createOutputEdgeTupleSchema(catalogDatabase, catalogGraphView);
	TupleSchema *vSchema = NULL;
	TupleSchema *eSchema = NULL;

	//const string& graphViewName = catalogGraphView.name();
	int32_t databaseId = catalogDatabase.relativeIndex();
	SHA1_CTX shaCTX;
	SHA1Init(&shaCTX);
	SHA1Update(&shaCTX, reinterpret_cast<const uint8_t *>(catalogGraphView.signature().c_str()), (uint32_t )::strlen(catalogGraphView.signature().c_str()));
	SHA1Final(reinterpret_cast<unsigned char *>(m_signatureHash), &shaCTX);
	// Persistent table will use default size (2MB) if tableAllocationTargetSize is zero.

	GraphView *graphView = GraphViewFactory::createGraphView(catalogGraphView.name(), catalogGraphView.isDirected(),
			vTable, eTable, pTable, vSchema, eSchema, columnNamesVertex, columnNamesEdge, columnIdsInVertexTable,
			columnIdsInEdgeTable, databaseId, m_signatureHash);

	return graphView;
}

void GraphViewCatalogDelegate::processSchemaChanges(catalog::Database const &catalogDatabase,
	                             catalog::GraphView const &catalogGraphView,
	                             std::map<std::string, GraphViewCatalogDelegate*> const &graphViewsByName)
{

}

void GraphViewCatalogDelegate::initVertexTupleWithDefaultValues(Pool* pool,
	    								catalog::GraphView const *catalogGraphView,
	                                    const std::set<int>& fieldsExplicitlySet,
	                                    TableTuple& tbTuple,
	                                    std::vector<int>& nowFields)
{

}

void GraphViewCatalogDelegate::initEdgeTupleWithDefaultValues(Pool* pool,
	    	                                    catalog::GraphView const *catalogGraphView,
	    	                                    const std::set<int>& fieldsExplicitlySet,
	    	                                    TableTuple& tbTuple,
	    	                                    std::vector<int>& nowFields)
{

}

TupleSchema *GraphViewCatalogDelegate::createOutputVertexTupleSchema(catalog::Database const &catalogDatabase,
	                                          catalog::GraphView const &catalogGraphView)
{
	// Columns:
	// Column is stored as map<String, Column*> in Catalog. We have to
	// sort it by Column index to preserve column order.
	const catalog::CatalogMap<catalog::Column> &cols = catalogGraphView.VertexProps();
	const int numColumns = cols.size();
	bool needsDRTimestamp = false;
	TupleSchemaBuilder schemaBuilder(numColumns,
									 needsDRTimestamp ? 1 : 0); // number of hidden columns

	map<string, catalog::Column*>::const_iterator col_iterator;
	int colIndex = 0;
	for (col_iterator = cols.begin();
		 col_iterator != cols.end(); col_iterator++) {

		const catalog::Column *catalog_column = col_iterator->second;
		colIndex = catalog_column->index();
		schemaBuilder.setColumnAtIndex(colIndex,
									   static_cast<ValueType>(catalog_column->type()),
									   static_cast<int32_t>(catalog_column->size()),
									   catalog_column->nullable(),
									   catalog_column->inbytes());
	}

	//msaber: the column names should be kept in a separate array per VoltDB design
	//The FanOut and FaIn attributes are added in the previous loop as they are part of  the VertexProps collection,
	//so no need to manuallly add them

	//FANOUT
	/*
	schemaBuilder.setColumnAtIndex(++colIndex,
								   ValueType::VALUE_TYPE_INTEGER,
								   4,
								   false,
								   false); //not in bytes, msaber needs to check this
	//FANIN
	schemaBuilder.setColumnAtIndex(++colIndex,
									   ValueType::VALUE_TYPE_INTEGER,
									   4,
									   false,
									   false); //not in bytes, msaber needs to check this
	*/

	if (needsDRTimestamp) {
		// Create a hidden timestamp column for a DRed table in an
		// active-active context.
		//
		// Column will be marked as not nullable in TupleSchema,
		// because we never expect a null value here, but this is not
		// actually enforced at runtime.
		schemaBuilder.setHiddenColumnAtIndex(0,
											 VALUE_TYPE_BIGINT,
											 8,      // field size in bytes
											 false); // nulls not allowed
	}

	return schemaBuilder.build();
}
TupleSchema *GraphViewCatalogDelegate::createOutputEdgeTupleSchema(catalog::Database const &catalogDatabase,
	    									  catalog::GraphView const &catalogGraphView)
{
	// Columns:
	// Column is stored as map<String, Column*> in Catalog. We have to
	// sort it by Column index to preserve column order.
	const catalog::CatalogMap<catalog::Column> &cols = catalogGraphView.EdgeProps();
	const int numColumns = cols.size();
	bool needsDRTimestamp = false;
	TupleSchemaBuilder schemaBuilder(numColumns,
									 needsDRTimestamp ? 1 : 0); // number of hidden columns

	map<string, catalog::Column*>::const_iterator col_iterator;
	int colIndex = 0;
	int numOfVertexProps = catalogGraphView.VertexProps().size();
	for (col_iterator = cols.begin();
		 col_iterator != cols.end(); col_iterator++) {

		const catalog::Column *catalog_column = col_iterator->second;
		//TODO: the following line depends on having the FF to start the first edge prop index
		//starting from n, where n-1 is the last index of a vertex property (also n is the # vertex properties)
		colIndex = catalog_column->index() - numOfVertexProps;
		schemaBuilder.setColumnAtIndex(colIndex,
									   static_cast<ValueType>(catalog_column->type()),
									   static_cast<int32_t>(catalog_column->size()),
									   catalog_column->nullable(),
									   catalog_column->inbytes());
	}

	if (needsDRTimestamp) {
		// Create a hidden timestamp column for a DRed table in an
		// active-active context.
		//
		// Column will be marked as not nullable in TupleSchema,
		// because we never expect a null value here, but this is not
		// actually enforced at runtime.
		schemaBuilder.setHiddenColumnAtIndex(0,
											 VALUE_TYPE_BIGINT,
											 8,      // field size in bytes
											 false); // nulls not allowed
	}

	return schemaBuilder.build();

}

}
