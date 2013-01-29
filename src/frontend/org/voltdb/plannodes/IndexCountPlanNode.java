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
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.StatsField;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

public class IndexCountPlanNode extends AbstractScanPlanNode {

    public enum Members {
        START_TYPE,
        END_TYPE,
        TARGET_INDEX_NAME,
        START_KEYS,
        END_KEYS;
    }

    /**
     * Attributes
     */

    // The lower bound index lookup operation type
    // -- controls whether bound should be considered inclusive (GTE/EQ) or exclusive (GT).
    protected IndexLookupType m_startType = IndexLookupType.EQ;

    // The upper bound index lookup operation type
    // -- controls whether bound should be considered inclusive (LTE/EQ) or exclusive (LT).
    protected IndexLookupType m_endType = IndexLookupType.EQ;

    // The index to use in the scan operation
    protected String m_targetIndexName;

    // This list of expressions corresponds to the values that we will use
    // at runtime in the lower bound lookup on the index
    protected List<AbstractExpression> m_startKeys = new ArrayList<AbstractExpression>();

    // This list of expressions corresponds to the values that we will use
    // at runtime in the upper bound lookup on the index
    final protected List<AbstractExpression> m_endKeys = new ArrayList<AbstractExpression>();

    private ArrayList<AbstractExpression> m_bindings;

    public IndexCountPlanNode() {
        super();
    }

    private IndexCountPlanNode(IndexScanPlanNode isp, AggregatePlanNode apn,
                               IndexLookupType endType, List<AbstractExpression> endKeys)
    {
        super();
        m_targetTableName = isp.getTargetTableName();
        m_targetIndexName = isp.getTargetIndexName();

        m_startType = isp.getLookupType();
        m_startKeys = isp.getSearchKeyExpressions();

        m_estimatedOutputTupleCount = 1;
        m_predicate = null;
        m_bindings = isp.getBindings();

        m_outputSchema = apn.getOutputSchema().clone();
        m_endType = endType;
        m_endKeys.addAll(endKeys);
    }

    // Create an IndexCountPlanNode that replaces the parent aggregate and child indexscan
    // UNLESS the indexscan's end expressions aren't a form that can be modeled with an end key.
    // The supported forms for end expression are:
    //   - null
    //   - one filter expression per index key component (ANDed together) as "combined" for the IndexScan.
    //   - fewer filter expressions than index key components with one of them (the last) being a LT comparison.
    // The LT restriction comes because when index key prefixes are identical to the prefix-only end key,
    // the entire index key sorts greater than the prefix-only end-key, because it is always longer.
    // These prefix-equal cases would be missed in an EQ or LTE filter, causing undercounts.
    // A prefix-only LT filter is intended to discard prefix-equal cases, so it is allowed.
    // @return the IndexCountPlanNode or null if one is not possible.
    public static IndexCountPlanNode createOrNull(IndexScanPlanNode isp, AggregatePlanNode apn)
    {
        List<AbstractExpression> endKeys = new ArrayList<AbstractExpression>();
        // Initially assume that there will be an equality filter on all key components.
        IndexLookupType endType = IndexLookupType.EQ;
        List<AbstractExpression> endComparisons = ExpressionUtil.uncombine(isp.getEndExpression());
        for (AbstractExpression ae: endComparisons) {
            // There should be no more end expressions after an LT or LTE has reset the end type.
            assert(endType == IndexLookupType.EQ);

            if (ae.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) {
                endType = IndexLookupType.LT;
            }
            else if (ae.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO) {
                endType = IndexLookupType.LTE;
            } else {
                assert(ae.getExpressionType() == ExpressionType.COMPARE_EQUAL);
            }

            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                endKeys.add((AbstractExpression)ae.getRight().clone());
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.toString());
            }
        }

        // Avoid the cases that would cause undercounts for prefix matches.
        // A prefix-only key exists and does not use LT.
        if ((endType != IndexLookupType.LT) &&
            (endKeys.size() > 0) &&
            (endKeys.size() < isp.getCatalogIndex().getColumns().size())) {
            return null;
        }
        return new IndexCountPlanNode(isp, apn, endType, endKeys);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.INDEXCOUNT;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        for (AbstractExpression exp : m_startKeys) {
            exp.validate();
        }
        for (AbstractExpression exp : m_endKeys) {
            exp.validate();
        }
    }

    /**
     * Should just return true -- there's only one order for a single row
     * @return true
     */
    @Override
    public boolean isOrderDeterministic() {
        return true;
    }

    @Override
    public void generateOutputSchema(Database db){}

    @Override
    public void resolveColumnIndexes(){}

    @Override
    public boolean computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        // HOW WE COST INDEX COUNTS
        // Cost out the index traversals. This is mostly a formality.
        // The point is to come up with a relatively small cost so that
        // index counts are preferable to index scans.
        Table target = db.getTables().getIgnoreCase(m_targetTableName);
        assert(target != null);
        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());
        stats.incrementStatistic(0, StatsField.TREE_INDEX_LEVELS_TRAVERSED, (long)(Math.log(tableEstimates.maxTuples)));

        stats.incrementStatistic(0, StatsField.TUPLES_READ, 1);
        m_estimatedOutputTupleCount = 1;
        return true;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.START_TYPE.name()).value(m_startType.toString());
        stringer.key(Members.END_TYPE.name()).value(m_endType.toString());
        stringer.key(Members.TARGET_INDEX_NAME.name()).value(m_targetIndexName);

        if ( ! m_startKeys.isEmpty()) {
            listExpressionsToJSONArray(stringer, Members.START_KEYS.name(),  m_startKeys);
        }
        if ( ! m_endKeys.isEmpty()) {
            listExpressionsToJSONArray(stringer, Members.END_KEYS.name(), m_endKeys);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);

        m_startType = IndexLookupType.get( jobj.getString( Members.START_TYPE.name() ) );
        m_endType = IndexLookupType.get( jobj.getString( Members.END_TYPE.name() ) );
        m_targetIndexName = jobj.getString(Members.TARGET_INDEX_NAME.name());
        loadExpressionsFromJSONArray(jobj, db, m_startKeys, Members.START_KEYS.name());
        loadExpressionsFromJSONArray(jobj, db, m_endKeys, Members.END_KEYS.name());
    }

    @Override
    protected String explainPlanForNode(String indent) {
        int startSize = m_startKeys.size();
        int endSize = m_endKeys.size();
        String usageInfo = String.format("(%d start keys, %d end keys)", startSize, endSize);

        String retval = "INDEX COUNT of \"" + m_targetTableName + "\"";
        retval += " using \"" + m_targetIndexName + "\"";
        retval += " " + usageInfo;
        return retval;
    }

    public ArrayList<AbstractExpression> getBindings() {
        return m_bindings;
    }
}
