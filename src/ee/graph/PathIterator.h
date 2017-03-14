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

	//call graph function that might add data to the temp table
	graphView->expandCurrentPathOperation();
	//call next on the iterator of the temp table
	return graphView->m_pathTableIterator->next(out);
}

}
#endif /* PATHITERATOR_H_ */
