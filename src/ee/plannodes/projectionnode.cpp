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

#include "projectionnode.h"

#include "common/executorcontext.hpp"
#include "expressions/parametervalueexpression.h"
#include "expressions/tuplevalueexpression.h"

using namespace std;
using namespace voltdb;

string ProjectionPlanNode::debugInfo(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "Projection Output["
           << getOutputSchema().size() << "]:\n";
    for (int ctr = 0, cnt = (int)getOutputSchema().size(); ctr < cnt; ctr++)
    {
        buffer << spacer << "  [" << ctr << "] ";
        buffer << "name=" << getOutputSchema()[ctr]->getColumnName() << " : ";
        if (m_outputColumnExpressions[ctr] != NULL)
        {
            buffer << m_outputColumnExpressions[ctr]->debug(spacer + "   ");
        }
        else
        {
            buffer << spacer << "  " << "<NULL>" << "\n";
        }
    }
    return buffer.str();
}

void ProjectionPlanNode::loadFromJSONObject(json_spirit::Object& obj)
{
    // XXX-IZZY move this to init at some point
    bool paramsBusted = false;
    bool columnsBusted = false;
    for (int ii = 0; ii < getOutputSchema().size(); ii++)
    {
        SchemaColumn* outputColumn = getOutputSchema()[ii];
        AbstractExpression* ae = outputColumn->getExpression();
        m_outputColumnExpressions.push_back(ae);
        if ( ! columnsBusted ) {
            voltdb::TupleValueExpression* tve = dynamic_cast<TupleValueExpression*>(ae);
            if (tve == NULL) {
                columnsBusted = true;
                m_outputIfAllTupleValues.resize(0);
            } else {
                m_outputIfAllTupleValues[ii] = tve->getColumnId();
            }
        }
        if ( ! paramsBusted ) {
            voltdb::ParameterValueExpression* pve = dynamic_cast<ParameterValueExpression*>(ae);
            if (pve == NULL) {
                paramsBusted = true;
                m_outputIfAllParameterValues.resize(0);
            } else {
                m_outputIfAllParameterValues[ii] = &(ExecutorContext::getParams()[pve->getParameterId()]);
            }
        }
    }
}
