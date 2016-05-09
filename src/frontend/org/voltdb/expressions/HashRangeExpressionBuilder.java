/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.expressions;

import java.util.Map;

import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Ordering;

/**
 * Convenience class to piece together a HashRangeExpression so its
 * ranges can be immutable post-construction.
 * Currently used only in org.voltdb.jni.TestExecutionEngine
 * in voltdb/tests/frontend
 * TODO: move this class into that package and test directory to
 * limit its exposure.
 */
public class HashRangeExpressionBuilder {
    /** Builder object that produces an immutable map. */
    private ImmutableSortedMap.Builder<Integer, Integer> m_builder =
            new ImmutableSortedMap.Builder<Integer, Integer>(Ordering.natural());

    public HashRangeExpressionBuilder() { }

    /**
     * Add a value pair representing a range of hash values.
     * @param start
     * @param end
     */
    public HashRangeExpressionBuilder put(Integer start, Integer end) {
        m_builder.put(start, end);
        return this;
    }

    /**
     * Generate a hash range expression for elastic partitioning of a table.
     * @param hashColumnIndex 0-based index of the partition column within its table
     * @return  hash range expression
     */
    public HashRangeExpression build(int hashColumnIndex) {
        Map<Integer, Integer> ranges = m_builder.build();
        return new HashRangeExpression(ranges, hashColumnIndex);
    }
}
