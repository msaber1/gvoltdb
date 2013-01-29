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


#ifndef HSTOREINDEXCOUNTNODE_H
#define HSTOREINDEXCOUNTNODE_H

#include "abstractscannode.h"

namespace voltdb {
/**
 *
 */
class IndexCountPlanNode : public AbstractScanPlanNode {
public:
    IndexCountPlanNode() : m_startType(INDEX_LOOKUP_TYPE_EQ), m_endType(INDEX_LOOKUP_TYPE_EQ) { }
    ~IndexCountPlanNode();
    virtual PlanNodeType getPlanNodeType() const { return PLAN_NODE_TYPE_INDEXCOUNT; }

    IndexLookupType getStartType() const { return m_startType; }
    IndexLookupType getEndType() const { return m_endType; }
    const std::string& getTargetIndexName() const { return m_targetIndexName; }
    const std::vector<AbstractExpression*>& getEndKeys() const { return m_endKeys; }
    const std::vector<AbstractExpression*>& getStartKeys() const { return m_startKeys; }

    std::string debugInfo(const std::string &spacer) const;

private:
    virtual void loadFromJSONObject(json_spirit::Object &obj);

    // The index to reference during execution
    std::string m_targetIndexName;
    // Optional indexed value(s) -- possibly just a prefix -- indicating the lower bound of the counted range
    std::vector<AbstractExpression*> m_startKeys;
    // Optional indexed value(s) -- possibly just a prefix -- indicating the upper bound of the counted range
    std::vector<AbstractExpression*> m_endKeys;
    // Distinguish random access lookups from range scans and indicate inclusiveness of the lower bound
    IndexLookupType m_startType;
    // Indicate inclusiveness of the upper bound
    IndexLookupType m_endType;
};

}

#endif
