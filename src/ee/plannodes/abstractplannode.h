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

#ifndef HSTOREPLANNODE_H
#define HSTOREPLANNODE_H

#include "catalog/database.h"
#include "common/ids.h"
#include "common/types.h"
#include "common/PlannerDomValue.h"
#include "expressions/abstractexpression.h"

#include "boost/scoped_array.hpp"

#include <map>
#include <string>
#include <vector>
#include <utility>

namespace voltdb {

class AbstractExecutor;
class SchemaColumn;
class Table;
class TupleSchema;

class AbstractPlanNode
{
public:
    virtual ~AbstractPlanNode();

    // ------------------------------------------------------------------
    // CHILDREN + PARENTS METHODS
    // ------------------------------------------------------------------
    void addChild(AbstractPlanNode* child) { m_children.push_back(child); }
    const std::vector<int32_t>& getChildIds() { return m_childIds; }
    const std::vector<AbstractPlanNode*>& getChildren() const { return m_children; }

    // ------------------------------------------------------------------
    // INLINE PLANNODE METHODS
    // ------------------------------------------------------------------
    void addInlinePlanNode(AbstractPlanNode* inline_node);
    AbstractPlanNode* getInlinePlanNode(PlanNodeType type) const;
    const std::map<PlanNodeType, AbstractPlanNode*>& getInlinePlanNodes() const { return m_inlineNodes; }

    bool isInline() const { return m_isInline; }

    // ------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ------------------------------------------------------------------
    int32_t getPlanNodeId() const { return m_planNodeId; }
    void setPlanNodeIdForTest(int32_t id) { m_planNodeId = id; }

    // currently a hack needed to initialize the executors.
    CatalogId databaseId() const { return 1; }

    void setExecutor(AbstractExecutor* executor) { m_executor = executor; }
    inline AbstractExecutor* getExecutor() const { return m_executor; }

    // Each sub-class will have to implement this function to return their type
    // This is better than having to store redundant types in all the objects
    virtual PlanNodeType getPlanNodeType() const = 0;

    /** return this or the child node that originally defined the output schema for this plan node. */
    const AbstractPlanNode* getSchemaDefiner() const;

    /**
     * Get the output columns that make up the output schema for this plan node.
     * The column order is implicit in their order in this vector.
     */
    const std::vector<std::string>& getOutputColumnNames() const { return m_outputColumnNames; }
    const AbstractExpression* const* getOutputExpressionArray() const
    { return m_outputExpressionArray.get(); }

    /**
     * Get the output number of columns -- strictly for use with plannode
     * classes that "project" a new output schema (vs. passing one up from a child).
     */
    int getValidOutputColumnCount() const
    {
        // Assert that this plan node defined (derialized in) its own output schema.
        assert(m_validOutputColumnCount >= 0);
        return m_validOutputColumnCount;
    }

    /**
     * Convenience method:
     * Generate a TupleSchema based on the contents of the output schema
     * from the plan and fetch the corresponding column names.
     */
    std::pair<TupleSchema*, const std::vector<std::string>*> generateTupleSchema() const;

    /**
     * Convenience method:
     * Generate a TupleSchema based on the expected format for DML results.
     */
    static TupleSchema* generateDMLCountTupleSchema();

    // ------------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------------
    static AbstractPlanNode* fromJSONObject(PlannerDomValue obj);

    // Debugging convenience methods
    std::string debug() const;
    std::string debugTree() const;
    //std::string debug(bool traverse) const;
    std::string debug(const std::string& spacer) const;
    virtual std::string debugInfo(const std::string& spacer) const = 0;

    // Replacement/wrapper for AbtractExpression* that saves the trouble of defining a
    // non-trivial destructor in the plannode class just to propagate deletion to
    // data member expression objects. The memory management only applies to the final value
    // of the expression member.
    // It is not intended to support multiple resets with implied deletes like scoped_ptr, etc.
    // Assign it a value only once or take responsibility for deleting a value before overwriting it.
    struct OwnedExpression {
        // The destructor implements the critical behavior of this class.
        ~OwnedExpression() { delete m_expression; }

        // The rest is just for compatibility/dressing.
        OwnedExpression(AbstractExpression* in = NULL) : m_expression(in) { }
        void operator=(AbstractExpression* in) { assert( ! m_expression); m_expression = in; }
        operator AbstractExpression*() const { return m_expression; }
        AbstractExpression* operator->() const { assert(m_expression); return m_expression; }
    private:
        AbstractExpression* m_expression;
    };

    // Replacement/wrapper for vector<AbtractExpression* that saves the trouble of defining a destructor
    // just to propagate deletion to the elements of the member vector (expression objects).
    // The memory management operates only on the final state of the member vector.
    // Assign/push each element only once or take responsibility for deleting an element
    // before overwriting it.
    struct VectorOfOwnedExpression : public std::vector<AbstractExpression*> {
        // The destructor implements the critical behavior of this class.
        // All other behavior is exactly as implemented in std::vector.
        ~VectorOfOwnedExpression()
        {
            size_t ii = size();
            while (ii--) {
                delete (*this)[ii];
            }
        }
    };

protected:
    virtual void loadFromJSONObject(PlannerDomValue obj) = 0;

    AbstractPlanNode()
        : m_planNodeId(-1)
        , m_executor(NULL)
        , m_isInline(false)
    { }

    void loadOutputSchemaFromJSONObject(PlannerDomValue obj);
    static AbstractExpression* loadExpressionFromJSONObject(const char* label, PlannerDomValue obj);
    static void loadExpressionsFromJSONObject(std::vector<AbstractExpression*>& arrayOut,
                                              const char* label, PlannerDomValue obj);



    //
    // Every PlanNode will have a unique id assigned to it at compile time
    //
    int32_t m_planNodeId;
    //
    // A node can have multiple children
    //
    std::vector<AbstractPlanNode*> m_children;
    std::vector<int32_t> m_childIds;
    //
    // We also keep a pointer to this node's executor so that we can
    // reference it quickly
    // at runtime without having to look-up a map
    //
    AbstractExecutor* m_executor; // volatile
    //
    // Some Executors can take advantage of multiple internal PlanNodes
    // to perform tasks inline. This can be a big speed increase
    //
    std::map<PlanNodeType, AbstractPlanNode*> m_inlineNodes;
    bool m_isInline;

private:
    static const int SCHEMA_UNDEFINED_SO_GET_FROM_INLINE_PROJECTION = -1;
    static const int SCHEMA_UNDEFINED_SO_GET_FROM_CHILD = -2;

    // This is used as a column count but also to hold one of the SCHEMA_UNDEFINED_SO_GET_FROM_ flags
    // when the output schema is not defined at this level.
    int m_validOutputColumnCount;

    std::vector<std::string> m_outputColumnNames;
    // The same "select" expressions are stored in the vector of owned expressions for memory management
    // and in the scoped array for quick run-time iteration.
    // ProjectionPlanNode and MaterializePlanNode further optimize the special cases of all
    // TupleValueExpressions and all ParameterValueExpressions, respectively.
    VectorOfOwnedExpression m_outputColumnExpressions;
    boost::scoped_array<AbstractExpression*> m_outputExpressionArray;
};

}

#endif
