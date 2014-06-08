/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#include "abstractscanexecutor.h"

#include "plannodes/abstractscannode.h"
#include "plannodes/limitnode.h"

namespace voltdb {

bool AbstractScanExecutor::p_init(TempTableLimits* limits)
{
    VOLT_TRACE("init scan Executor");

    AbstractScanPlanNode* node = dynamic_cast<AbstractScanPlanNode*>(m_abstractNode);
    assert(node);
    m_predicate = node->getPredicate();

    ProjectionPlanNode* projection_node =
        static_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
    m_inlineProjection.initProjectionState(projection_node);

    if (node->isSubQuery()) {
        m_target_tcd = NULL;
    } else {
        m_target_tcd = m_engine->getTableDelegate(node->getTargetTableName());
        assert(m_target_tcd);
    }

    LimitPlanNode* limit_node =
        static_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    if (limit_node) {
        m_inlineLimitOffset = limit_node->getState();
    }

    p_initMore(limits);
    return true;
}

Table* AbstractScanExecutor::getTargetTable()
{
    return m_target_tcd->getTable();
}

}
