#ifndef GRAPHELEMENT_H
#define GRAPHELEMENT_H

/*
#include <cstdlib>
#include <sstream>
#include <cassert>
#include "common/tabletuple.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/FatalException.hpp"
*/
//#include "TableTuple.h"
#include "GraphView.h"

#ifndef NDEBUG
//#include "debuglog.h"
#endif /* !define(NDEBUG) */
namespace voltdb {

class GraphElement
{
protected:
	int m_id;
	char* m_tupleData;
	GraphView* m_gview;
	bool m_isRemote;
public:
	GraphElement(void);
	GraphElement(int id, char* tupleData, GraphView* graphView, bool remote);
	~GraphElement(void);

	void setId(int id);
	int getId();
	void setTupleData(char* tupleData);
	void setGraphView(GraphView* gView);
	char* getTupleData();
	GraphView* getGraphView();
	bool isRemote();
};

}

#endif
