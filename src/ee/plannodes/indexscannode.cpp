/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "indexscannode.h"

#include <sstream>

#include "common/debuglog.h"

#include "common/SerializableEEException.h"
#include "expressions/abstractexpression.h"

namespace voltdb {

IndexScanPlanNode::~IndexScanPlanNode() {
    for (int ii = 0; ii < searchkey_expressions.size(); ii++) {
        delete searchkey_expressions[ii];
    }
    delete end_expression;
}

std::string IndexScanPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << this->AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << this->target_index_name << "]\n";
    buffer << spacer << "IndexLookupType[" << this->lookup_type << "]\n";
    buffer << spacer << "SortDirection[" << this->sort_direction << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (int ctr = 0, cnt = (int)this->searchkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << this->searchkey_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "End Expression: ";
    if (this->end_expression != NULL) {
        buffer << "\n" << this->end_expression->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }

    buffer << spacer << "Post-Scan Expression: ";
    if (m_predicate != NULL) {
        buffer << "\n" << m_predicate->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }
    return (buffer.str());
}

void IndexScanPlanNode::loadFromJSONObject(json_spirit::Object &obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string lookupTypeString = loadStringFromJSON(obj, "LOOKUP_TYPE");
    if (lookupTypeString.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find LOOKUP_TYPE");
    }
    lookup_type = stringToIndexLookup(lookupTypeString);

    std::string sortDirectionString = loadStringFromJSON(obj, "SORT_DIRECTION");
    if (sortDirectionString.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find SORT_DIRECTION");
    }
    sort_direction = stringToSortDirection(sortDirectionString);

    target_index_name = loadStringFromJSON(obj, "TARGET_INDEX_NAME");
    if (target_index_name.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find TARGET_INDEX_NAME");
    }

    searchkey_expressions = loadExpressionsFromJSONArray(obj, "SEARCH_KEYS");
    if (searchkey_expressions.empty()) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find SEARCH_KEYS");
    }

    end_expression = loadExpressionFromJSON(obj, "END_EXPRESSION");
}

}
