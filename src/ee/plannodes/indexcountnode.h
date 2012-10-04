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


#ifndef HSTOREINDEXCOUNTNODE_H
#define HSTOREINDEXCOUNTNODE_H

#include "abstractscannode.h"

namespace voltdb {
/**
 *
 */
class IndexCountPlanNode : public AbstractScanPlanNode {
public:
    IndexCountPlanNode() : lookup_type(INDEX_LOOKUP_TYPE_EQ), end_type(INDEX_LOOKUP_TYPE_EQ) { }
    ~IndexCountPlanNode();
    virtual PlanNodeType getPlanNodeType() const { return (PLAN_NODE_TYPE_INDEXCOUNT); }

    IndexLookupType getLookupType() const { return lookup_type; }
    IndexLookupType getEndType() const { return end_type; }
    const std::string& getTargetIndexName() const { return target_index_name; }
    const std::vector<AbstractExpression*>& getEndKeyExpressions() const { return endkey_expressions; }
    const std::vector<AbstractExpression*>& getSearchKeyExpressions() const { return searchkey_expressions; }

    std::string debugInfo(const std::string &spacer) const;

private:
    virtual void loadFromJSONObject(json_spirit::Object &obj);

    // The index to reference during execution
    std::string target_index_name;
    // Optional indexed value(s) -- possibly just a prefix -- indicating the lower bound of the counted range
    std::vector<AbstractExpression*> searchkey_expressions;
    // Optional indexed value(s) -- possibly just a prefix -- indicating the upper bound of the counted range
    std::vector<AbstractExpression*> endkey_expressions;
    // Distinguish random access lookups from range scans and indicate inclusiveness of the lower bound
    IndexLookupType lookup_type;
    // Indicate inclusiveness of the upper bound
    IndexLookupType end_type;
};

}

#endif
