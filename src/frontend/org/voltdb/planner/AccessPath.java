/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class AccessPath {
    Index index = null;
    IndexUseType use = IndexUseType.COVERING_UNIQUE_EQUALITY;
    boolean nestLoopIndexJoin = false;
    boolean requiresSendReceive = false;
    boolean keyIterate = false;
    IndexLookupType lookupType = IndexLookupType.EQ;
    SortDirectionType sortDirection = SortDirectionType.INVALID;
    ArrayList<AbstractExpression> indexExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> endExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> otherExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> joinExprs = new ArrayList<AbstractExpression>();

    // Generally enable/disable index rank processing
    boolean m_track_rank;
    // Configure the rank range -- how it is bounded inclusively/exclusively/not at each end.
    IndexLookupType m_rank_range_type;
    // The high and low end rank values of a rank range -- both optional
    AbstractExpression m_rank_range_min;
    AbstractExpression m_rank_range_max;
    // An optional "search key" representing the location of "rank = 1" if not ranking over the entire index.
    List<AbstractExpression> m_rank_offset_key_expressions;

    public void enableIndexRank() {
        m_track_rank = true;
    }

    public void setIndexRankOffsetOptions(List<AbstractExpression> rank_offset_key_expressions) {
        m_track_rank = true;
        m_rank_offset_key_expressions = rank_offset_key_expressions;
    }

    public void setIndexRankRangeOptions(IndexLookupType rank_range_type, AbstractExpression rank_range_min, AbstractExpression rank_range_max) {
        m_track_rank = true;
        m_rank_range_type = rank_range_type;
        m_rank_range_min = rank_range_min;
        m_rank_range_max = rank_range_max;
    }

    @Override
    public String toString() {
        String retval = "";

        retval += "INDEX: " + ((index == null) ? "NULL" : (index.getParent().getTypeName() + "." + index.getTypeName())) + "\n";
        retval += "USE:   " + use.toString() + "\n";
        retval += "TYPE:  " + lookupType.toString() + "\n";
        retval += "DIR:   " + sortDirection.toString() + "\n";
        retval += "ITER?: " + String.valueOf(keyIterate) + "\n";
        retval += "NLIJ?: " + String.valueOf(nestLoopIndexJoin) + "\n";

        retval += "IDX EXPRS:\n";
        int i = 0;
        for (AbstractExpression expr : indexExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "END EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : endExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "OTHER EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : otherExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "JOIN EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : joinExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        return retval;
    }
}
