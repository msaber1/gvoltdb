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

package org.voltdb.planner;

import java.util.ArrayList;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;

public class ParsedUnionStmt extends AbstractParsedStmt {

    public enum UnionType {
        NOUNION,
        UNION,
        UNION_ALL,
        INTERSECT,
        INTERSECT_ALL,
        EXCEPT_ALL,
        EXCEPT
    };

    public ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    public UnionType m_unionType = UnionType.NOUNION;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedUnionStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        m_unionType = UnionType.valueOf(type);
        // The join order map must contain an entry for each child plus one for the whole statement
        if (joinOrderMap != null && joinOrderMap.size() != m_children.size() + 1) {
            throw new RuntimeException("The specified join order doesn not match the set operation statement.");
        }
        int i = 0;
        for (VoltXMLElement selectSQL : stmtNode.children) {
            AbstractParsedStmt nextStmt = m_children.get(i++);
            if (joinOrderMap != null) {
                String joinOrder = joinOrderMap.get("sql" + i);
                nextStmt.parseJoinOrder(joinOrder);
                if (joinOrder == null) {
                    throw new RuntimeException("The specified join order doesn not match the set operation statement.");
                }
            }
            nextStmt.parse(selectSQL);
        }
    }

    /**Parse tables and parameters
     *
     * @param root
     * @param db
     */
    @Override
    void parseTablesAndParams(VoltXMLElement stmtNode) {

        assert(stmtNode.children.size() > 1);
        tableList.clear();
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedSelectStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);

                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                ParsedUnionStmt childStmt = new ParsedUnionStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);
                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
            } else {
                throw new PlanningErrorException("Unexpected Element in UNION statement: " + childSQL.name);
            }
        }
    }

    @Override
    protected String extractJoinOrderAlias(String joinOrder, int idx, int stmtCnt) {
        return "sql" + stmtCnt;
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param joinOrder
     */
    @Override
    void postParse(String sqlStmt) {
        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sqlStmt);
        }

        sql = sqlStmt;
    }

}
