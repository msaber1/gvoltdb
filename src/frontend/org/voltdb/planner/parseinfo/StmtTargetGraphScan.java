package org.voltdb.planner.parseinfo;

import java.util.ArrayList;
import java.util.List;

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.GraphView;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.utils.CatalogUtil;

public class StmtTargetGraphScan extends StmtTableScan {
    // Catalog table
    private final GraphView m_graph;
    private final String m_graphElement;
    private List<Index> m_indexes;
    private List<Column> m_columns;

    public StmtTargetGraphScan(GraphView graph, String tableAlias, int stmtId, String object) {
        super(tableAlias, stmtId);
        assert (graph != null);
        m_graph = graph;
        m_graphElement = object;

        //findPartitioningColumns();
    }

    public StmtTargetGraphScan(GraphView graph, String tableAlias) {
        this(graph, tableAlias, 0, null);
    }

    @Override
    public String getTableName() {
        return m_graph.getTypeName();
    }

    public GraphView getTargetGraph() {
        assert(m_graph != null);
        return m_graph;
    }

    public String getGraphElementName() {
        return m_graphElement;
    }
    
    @Override
    public boolean getIsReplicated() {
        return m_graph.getIsreplicated();
    }


    @Override
    public List<Index> getIndexes() {
        if (m_indexes == null) {
            m_indexes = new ArrayList<Index>();
            for (Index index : m_graph.getIndexes()) {
                m_indexes.add(index);
            }
        }
        return m_indexes;
    }

    @Override
    public String getColumnName(int columnIndex) {
    	throw new PlanningErrorException("Unsupported getColumnName operation for graph");
        //return null;
    }
    
    public String getVertexPropName(int columnIndex) {
        if (m_columns == null) {
        	m_columns = CatalogUtil.getSortedCatalogItems(m_graph.getVertexprops(), "index");
        }
        return m_columns.get(columnIndex).getTypeName();
    }
    
    public String getEdgePropName(int columnIndex) {
        if (m_columns == null) {
        	m_columns = CatalogUtil.getSortedCatalogItems(m_graph.getEdgeprops(), "index");
        }
        return m_columns.get(columnIndex).getTypeName();
    }

    @Override
    public void processTVE(TupleValueExpression expr, String properytype) {
    	//throw new PlanningErrorException("Unsupported processTVE operation for graph");
    	expr.resolveForGraph(m_graph, properytype);
    }

    public void resolveTVE(TupleValueExpression expr, String properytype) {
        String columnName = expr.getColumnName();
        processTVE(expr, properytype);
        expr.setOrigStmtId(m_stmtId);

        Pair<String, Integer> setItem = Pair.of(columnName, expr.getDifferentiator());
        if ( ! m_scanColumnNameSet.contains(setItem)) {
            SchemaColumn scol = new SchemaColumn(getTableName(), m_tableAlias,
                    columnName, columnName, (TupleValueExpression) expr.clone());
            m_scanColumnNameSet.add(setItem);
            m_scanColumnsList.add(scol);
        }
    }

}

