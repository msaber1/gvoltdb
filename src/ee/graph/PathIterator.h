/*
 * PathIterator.h
 *
 *  Created on: Mar 14, 2017
 *      Author: msaberab
 */
#ifndef PATHITERATOR_H_
#define PATHITERATOR_H_

#include <cassert>
#include "boost/shared_ptr.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/TupleIterator.h"
#include "storage/temptable.h"
#include "graph/GraphView.h"
#include "logging/LogManager.h"

namespace voltdb {

//class TempTable;
//class GraphView;

class PathIterator : public TupleIterator {

    friend class TempTable;
    friend class GraphView;

public:
	PathIterator(GraphView* gv) { this->graphView = gv; }
	bool next(TableTuple &out);
	//virtual ~PathIterator();
protected:
	GraphView* graphView;
};


inline bool PathIterator::next(TableTuple &out) {

	LogManager::GLog("PathIterator", "next", 40,
					"calling next() before expanding");
	//call graph function that might add data to the temp table
	graphView->expandCurrentPathOperation();

	if(graphView->m_pathTableIterator == NULL)
	{
		LogManager::GLog("PathIterator", "next", 46,
							"m_pathTableIterator is initialized after we found it null.");
		graphView->m_pathTableIterator = &(graphView->m_pathTable->iteratorDeletingAsWeGo());
	}
	//bool hasNext = graphView->m_pathTableIterator->hasNext();
	//if new tuples were added, update the iterator about the #active tuples
	if(graphView->m_pathTableIterator->m_activeTuples < (int)graphView->m_pathTable->activeTupleCount())
	{
		//std::stringstream paramsToPrint;
		//paramsToPrint << "hasNext is false, activeTupleCount before = " << graphView->m_pathTableIterator->m_activeTuples;

		graphView->m_pathTableIterator->m_activeTuples = (int)graphView->m_pathTable->activeTupleCount();

		//paramsToPrint << ", activeTupleCount after = " << graphView->m_pathTableIterator->m_activeTuples;
		//LogManager::GLog("PathIterator", "next", 61, paramsToPrint.str());
	}


	/*
	bool hasNext = graphView->m_pathTableIterator->hasNext();
	if(!hasNext)
	{
		graphView->m_pathTableIterator = &(graphView->m_pathTable->iteratorDeletingAsWeGo());
		hasNext = graphView->m_pathTableIterator->hasNext();
		if(!isValidTuple && hasNext)
		{
			LogManager::GLog("PathIterator", "next", 59,
										"isValidTuple is false and hasNext is true, calling next again.");
			isValidTuple = graphView->m_pathTableIterator->next(out);
		}
	}

	if(graphView->dummyPathExapansionState == 6)
		isValidTuple = false;
	*/
	//call next on the iterator of the temp table
	return graphView->m_pathTableIterator->next(out);
}

}
#endif /* PATHITERATOR_H_ */
