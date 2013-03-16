/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.CatalogUtil;

public abstract class AbstractScanPlanNode extends AbstractPlanNode {

    public enum Members {
        PREDICATE,
        TARGET_TABLE_NAME;
    }

    // Store the columns from the table as an internal NodeSchema
    // for consistency of interface
    protected NodeSchema m_tableSchema = new NodeSchema();
    // Store the columns we use from this table indexed by name and alias.
    protected HashMap<Pair<String, String>, TupleValueExpression> m_usedColumns =
          new HashMap<Pair<String, String>, TupleValueExpression>();
    protected AbstractExpression m_predicate;

    // The target table is the table that the plannode wants to perform some operation on.
    protected String m_targetTableName = "";
    protected String m_targetTableAlias = null;

    protected AbstractScanPlanNode() {
        super();
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // TargetTableId
        //
        if (m_targetTableName == null) {
            throw new Exception("ERROR: TargetTableName is null for PlanNode '" + toString() + "'");
        }
        //
        // Filter Expression
        // It is allowed to be null, but we need to check that it's valid
        //
        if (m_predicate != null) {
            m_predicate.validate();
        }
        // All the schema columns better reference this table
        for (TupleValueExpression col : m_usedColumns.values()) {
            if (!m_targetTableName.equals(col.getTableName())) {
                throw new Exception("ERROR: The scan column: " + col.getColumnName() +
                                    " in table: " + m_targetTableName + " refers to " +
                                    " table: " + col.getTableName());
            }
        }
    }

    /**
     * Does the plan guarantee an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return true unless the scan has an inline limit and no particular order.
     */
    @Override
    public boolean isContentDeterministic() {
        AbstractPlanNode limit = this.getInlinePlanNode(PlanNodeType.LIMIT);
        if ((limit == null) || isOrderDeterministic()) {
            return true;
        } else {
            m_nondeterminismDetail = "a limit on an unordered scan may return different rows";
            return false;
        }
    }

    /**
     * @return the target_table_name
     */
    public String getTargetTableName() {
        return m_targetTableName;
    }

    /**
     * @param name
     */
    public void setTargetTableName(String name) {
        m_targetTableName = name;
    }

    /**
     * @return the target_table_alias
     */
    public String getTargetTableAlias() {
        return m_targetTableAlias;
    }

    /**
     * @param alias
     */
    public void setTargetTableAlias(String alias) {
        m_targetTableAlias = alias;
    }

    /**
     * @return the predicate
     */
    public AbstractExpression getPredicate() {
        return m_predicate;
    }

    /**
     * @param predicate the predicate to set
     *
     */
    public void setPredicate(AbstractExpression predicate) {
        if (predicate != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_predicate = (AbstractExpression) predicate.clone();
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    public void addScanColumns(Map< Pair<String, String>, TupleValueExpression > map)
    {
        for (Pair<String, String> name_n_alias : map.keySet()) {
            TupleValueExpression col = map.get(name_n_alias);
            m_usedColumns.put(name_n_alias, col);
        }
    }

    @Override
    public NodeSchema generateOutputSchema(Database db)
    {
        // If any columns are referenced in the query, get the catalog schema.
        // It's rare but not impossible for a query to scan a table but not reference any of its columns.
        if (m_usedColumns.size() != 0) {
            CatalogMap<Column> cols = db.getTables().getIgnoreCase(m_targetTableName).getColumns();
            // you don't strictly need to sort this, but it makes diff-ing easier
            for (Column col : CatalogUtil.getSortedCatalogItems(cols, "index")) {
                // must produce a tuple value expression for this column.
                TupleValueExpression tve = new TupleValueExpression();
                tve.setValueType(VoltType.get((byte)col.getType()));
                tve.setValueSize(col.getSize());
                tve.setColumnIndex(col.getIndex());
                tve.setTableName(m_targetTableName);
                tve.setColumnAlias(col.getTypeName());
                tve.setColumnName(col.getTypeName());
                m_tableSchema.addColumn(tve);
            }
        }

        // A scan's inline projection defines its output schema.
        // If it lacks an inline projection, a projection can be constructed from the set of referenced columns,
        // arranged for convenience in table-schema order.

        ProjectionPlanNode proj = (ProjectionPlanNode)getInlinePlanNode(PlanNodeType.PROJECTION);
        if (proj == null) {
            // If the set of columns is essentially "select *",
            // including the fact that the columns are not aliased,
            // a projection would just be extra overhead (vs. operating directly on the persistent table).
            // The table schema IS the output schema.
            // Assuming that the parser caught any invalid column names, all that needs to be checked here
            // is that the number of referenced unique column names matches the number of columns and there
            // are no aliases. It seems a pity to have to do a projection just to get aliases, but the real
            // target here is "select *" which gives no way of aliasing.
            if (m_usedColumns.size() == m_tableSchema.size() &&  ! columnsUseAliases()) {
                m_outputSchema = m_tableSchema;
            } else {
                // Build an inline projection consisting of all the referenced columns.
                m_outputSchema = new NodeSchema();
                for (TupleValueExpression tve : m_usedColumns.values()) {
                    // Update all columns' indexes against the table schema
                    int index = m_tableSchema.getIndexOfTve(tve);
                    tve.setColumnIndex(index);
                    m_outputSchema.addColumn(tve);
                }
                // Order the scan columns according to the table schema
                // before we stick them in the projection output.
                // It PROBABLY is OK for different aliases for the same column to "tie" on index in this sort.
                // The result should be deterministic according to the order that the aliases come out of the hash.
                m_outputSchema.sortByTveIndex();
                // Create an inline projection to map table outputs to scan outputs
                proj = new ProjectionPlanNode();
                proj.setOutputSchema(m_outputSchema);
                addInlinePlanNode(proj);
            }
        } else {
            // Only the pre-existing inline projections (vs. the simple one just built above)
            // still need index resolution.
            proj.resolveColumnIndexesUsingSchema(m_tableSchema);
            m_outputSchema = proj.getOutputSchema();
        }

        // Resolve column indexes in the predicate expression
        // This applies to both seq and index scan.  Though, index scan has
        // additional expressions to resolve in its own generateOutputSchema refinement.
        for (TupleValueExpression tve : ExpressionUtil.getTupleValueExpressions(m_predicate)) {
            int index = m_tableSchema.getIndexOfTve(tve);
            tve.setColumnIndex(index);
        }
        m_outputSchema = proj.getOutputSchema();
        return m_outputSchema;
    }

    private boolean columnsUseAliases()
    {
        for (Pair<String, String> name_n_alias : m_usedColumns.keySet()) {
            if (name_n_alias.getFirst().equals(name_n_alias.getSecond())) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key(Members.PREDICATE.name());
        stringer.value(m_predicate);
        stringer.key(Members.TARGET_TABLE_NAME.name()).value(m_targetTableName);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);

        if(!jobj.isNull(Members.PREDICATE.name())) {
            m_predicate = AbstractExpression.fromJSONObject(jobj.getJSONObject(Members.PREDICATE.name()), db);
        }
        this.m_targetTableName = jobj.getString( Members.TARGET_TABLE_NAME.name() );

    }

    @Override
    public void getScanNodeList_recurse(ArrayList<AbstractScanPlanNode> collected,
            HashSet<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        collected.add(this);
    }
}
