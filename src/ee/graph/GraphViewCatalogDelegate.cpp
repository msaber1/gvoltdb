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
	            catalog::GraphView const &catalogGraphView, Table* vTable, Table* eTable)
{
	m_graphView = constructGraphViewFromCatalog(catalogDatabase,
	                                        catalogGraphView, vTable, eTable);
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
	                                     Table* vTable, Table* eTable)
{
	LogManager::GLog("GraphViewCatalogDelegate", "constructGraphViewFromCatalog", 71, "graphViewName = " + catalogGraphView.name());
	// Create a persistent graph view for this table in our catalog
	int32_t graphView_id = catalogGraphView.relativeIndex();
	std::stringstream params;
	params << " graphView_id (relative index) = " << graphView_id;
	LogManager::GLog("GraphViewCatalogDelegate", "GraphViewCatalogDelegate", 68, params.str());

	// get an array of the vertex column names
	const int numColumns = static_cast<int>(catalogGraphView.VTable()->columns().size());
	map<string, catalog::Column*>::const_iterator col_iterator;
	vector<string> columnNamesVertex(numColumns + 2);
	int colIndex = 0;
	for (col_iterator = catalogGraphView.VTable()->columns().begin();
		 col_iterator != catalogGraphView.VTable()->columns().end();
		 col_iterator++)
	{
		const catalog::Column *catalog_column = col_iterator->second;
		colIndex = catalog_column->index();
		columnNamesVertex[colIndex] = catalog_column->name();
	}
	columnNamesVertex[++colIndex] = "fanOut";
	columnNamesVertex[++colIndex] = "fanIn";


	// get the schema for the table
	//TupleSchema *schema = createVertexTupleSchema(catalogDatabase, catalogGraphView);

	//const string& graphViewName = catalogGraphView.name();
	int32_t databaseId = catalogDatabase.relativeIndex();
	SHA1_CTX shaCTX;
	SHA1Init(&shaCTX);
	SHA1Update(&shaCTX, reinterpret_cast<const uint8_t *>(catalogGraphView.signature().c_str()), (uint32_t )::strlen(catalogGraphView.signature().c_str()));
	SHA1Final(reinterpret_cast<unsigned char *>(m_signatureHash), &shaCTX);
	// Persistent table will use default size (2MB) if tableAllocationTargetSize is zero.

	GraphView *graphView = GraphViewFactory::createGraphView(catalogGraphView,
			databaseId, vTable, eTable, m_signatureHash);

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

TupleSchema *GraphViewCatalogDelegate::createVertexTupleSchema(catalog::Database const &catalogDatabase,
	                                          catalog::GraphView const &catalogGraphView)
{
	// Columns:
	// Column is stored as map<String, Column*> in Catalog. We have to
	// sort it by Column index to preserve column order.
	const catalog::Table *catalogTable = catalogGraphView.VTable();
	const int numColumns = static_cast<int>(catalogTable->columns().size()) + 2;
	bool needsDRTimestamp = catalogDatabase.isActiveActiveDRed() && catalogTable->isDRed();
	TupleSchemaBuilder schemaBuilder(numColumns,
									 needsDRTimestamp ? 1 : 0); // number of hidden columns

	map<string, catalog::Column*>::const_iterator col_iterator;
	int colIndex = 0;
	for (col_iterator = catalogTable->columns().begin();
		 col_iterator != catalogTable->columns().end(); col_iterator++) {

		const catalog::Column *catalog_column = col_iterator->second;
		colIndex = catalog_column->index();
		schemaBuilder.setColumnAtIndex(colIndex,
									   static_cast<ValueType>(catalog_column->type()),
									   static_cast<int32_t>(catalog_column->size()),
									   catalog_column->nullable(),
									   catalog_column->inbytes());
	}

	//msaber: need to check how the name is set

	schemaBuilder.setColumnAtIndex(++colIndex,
								   ValueType::VALUE_TYPE_INTEGER,
								   4,
								   false,
								   false); //not in bytes, msaber needs to check this
	schemaBuilder.setColumnAtIndex(++colIndex,
									   ValueType::VALUE_TYPE_INTEGER,
									   4,
									   false,
									   false); //not in bytes, msaber needs to check this

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
TupleSchema *GraphViewCatalogDelegate::createEdgeTupleSchema(catalog::Database const &catalogDatabase,
	    									  catalog::GraphView const &catalogGraphView)
{
	return NULL;
}

}
