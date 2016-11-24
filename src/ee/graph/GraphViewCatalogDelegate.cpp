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
	            catalog::GraphView const &catalogGraphView)
{

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
	return NULL;
}
TupleSchema *GraphViewCatalogDelegate::createEdgeTupleSchema(catalog::Database const &catalogDatabase,
	    									  catalog::GraphView const &catalogGraphView)
{
	return NULL;
}

}
