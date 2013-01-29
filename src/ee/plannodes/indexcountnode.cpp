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

#include "indexcountnode.h"

#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "expressions/abstractexpression.h"

namespace voltdb {

IndexCountPlanNode::~IndexCountPlanNode() {
    for (int ii = 0; ii < m_startKeys.size(); ii++) {
        delete m_startKeys[ii];
    }
    for (int ii = 0; ii < m_endKeys.size(); ii++) {
        delete m_endKeys[ii];
    }
}

std::string IndexCountPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << m_targetIndexName << "]\n";
    buffer << spacer << "IndexLookupType[" << m_startType << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (size_t ctr = 0, cnt = m_startKeys.size(); ctr < cnt; ctr++) {
        buffer << m_startKeys[ctr]->debug(spacer);
    }

    buffer << spacer << "EndKey Expressions:\n";
    for (size_t ctr = 0, cnt = m_endKeys.size(); ctr < cnt; ctr++) {
        buffer << m_endKeys[ctr]->debug(spacer);
    }

    buffer << spacer << "Post-Scan Expression: ";
    if (m_predicate != NULL) {
        buffer << "\n" << m_predicate->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }
    return (buffer.str());
}

void IndexCountPlanNode::loadFromJSONObject(json_spirit::Object &obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string startTypeString = loadStringFromJSON(obj, "START_TYPE");
    if (startTypeString.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
                                      " Can't find START_TYPE");
    }
    m_startType = stringToIndexLookup(startTypeString);

    std::string endTypeString = loadStringFromJSON(obj, "END_TYPE");
    if (endTypeString.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
                                      " Can't find END_TYPE");
    }
    m_endType = stringToIndexLookup(endTypeString);

    m_targetIndexName = loadStringFromJSON(obj, "TARGET_INDEX_NAME");
    if (m_targetIndexName.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
                                      " Can't find TARGET_INDEX_NAME");
    }

    m_startKeys = loadExpressionsFromJSONArray(obj, "STARTKEYS");
    m_endKeys = loadExpressionsFromJSONArray(obj, "ENDKEYS");
}

}
