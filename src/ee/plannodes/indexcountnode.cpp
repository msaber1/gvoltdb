/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "indexcountnode.h"

#include <sstream>

namespace voltdb {

PlanNodeType IndexCountPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_INDEXCOUNT; }

std::string IndexCountPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << m_target_index_name << "]\n";
    buffer << spacer << "IndexLookupType[" << m_lookup_type << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (int ctr = 0, cnt = (int)m_search_key_expressions.size(); ctr < cnt; ctr++) {
        buffer << m_search_key_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "EndKey Expressions:\n";
    for (int ctr = 0, cnt = (int)m_end_key_expressions.size(); ctr < cnt; ctr++) {
        buffer << m_end_key_expressions[ctr]->debug(spacer);
    }

    return buffer.str();
}

void IndexCountPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string endTypeString = obj.valueForKey("END_TYPE").asStr();
    m_end_type = stringToIndexLookup(endTypeString);

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    m_lookup_type = stringToIndexLookup(lookupTypeString);

    m_target_index_name = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    loadExpressionsFromJSONObject(m_search_key_expressions, "SEARCHKEY_EXPRESSIONS", obj);
    loadExpressionsFromJSONObject(m_end_key_expressions, "ENDKEY_EXPRESSIONS", obj);

    m_skip_null_predicate = loadExpressionFromJSONObject("SKIP_NULL_PREDICATE", obj);
}

}
