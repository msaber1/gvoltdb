#ifndef GRAPHELEMENT_H
#define GRAPHELEMENT_H

//#include "common/tabletuple.h"
#include "TableTuple.h"
#include "GraphView.h"

#ifndef NDEBUG
//#include "debuglog.h"
#endif /* !define(NDEBUG) */
namespace voltdb {
class GraphElement
{
protected:
	int m_id;
	TableTuple* m_tuple;
	GraphView* m_gview;
	bool m_isRemote;
public:
	GraphElement(void);
	GraphElement(int id, TableTuple* t, GraphView* graphView, bool remote);
	~GraphElement(void);

	void setId(int id);
	int getId();
	void setTuple(TableTuple* t);
	void setGraphView(GraphView* gView);
	TableTuple* getTuple();
	GraphView* getGraphView();
	bool isRemote();
};

}

#endif
