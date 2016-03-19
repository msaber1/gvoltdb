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
/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#ifndef SRC_EE_EXPRESSIONS_USERDEFINEDFUNCTIONDESCRIPTOR_H_
#define SRC_EE_EXPRESSIONS_USERDEFINEDFUNCTIONDESCRIPTOR_H_

namespace voltdb {

class UserDefinedFunctionDescriptor {
    int32_t m_fid;
    int32_t m_returnType;
    std::vector<int32_t> m_paramTypes;
public:
    UserDefinedFunctionDescriptor(int32_t fid, int32_t returnType, const std::vector<int32_t> &paramTypes)
     : m_fid(fid),
       m_returnType(returnType),
       m_paramTypes(paramTypes.size()) {
        for (int idx = 0; idx < paramTypes.size(); idx += 1) {
            m_paramTypes[idx] = paramTypes[idx];
        }
    }
    inline int32_t getReturnType() const {
        return m_returnType;
    }
    inline int32_t getFid() const {
        return m_fid;
    }
    const std::vector<int32_t> &getParamTypes() const {
        return m_paramTypes;
    }
};

} /* namespace voltdb */

#endif /* SRC_EE_EXPRESSIONS_USERDEFINEDFUNCTIONDESCRIPTOR_H_ */
