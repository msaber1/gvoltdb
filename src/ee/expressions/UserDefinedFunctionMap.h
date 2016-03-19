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
#ifndef SRC_EE_EXPRESSIONS_USERDEFINEDFUNCTIONMAP_H_
#define SRC_EE_EXPRESSIONS_USERDEFINEDFUNCTIONMAP_H_

#include <stdexcept>
#include <boost/unordered_map.hpp>
#include <boost/shared_ptr.hpp>
#include "UserDefinedFunctionDescriptor.h"
#include "common/debuglog.h"

namespace voltdb {

class UserDefinedFunctionMap {
    typedef boost::unordered_map<int32_t, boost::shared_ptr<UserDefinedFunctionDescriptor>> maptype;
    typedef maptype::const_iterator cmapiterator;
    maptype m_udfMap;
public:
    const UserDefinedFunctionDescriptor *getUDF(int32_t fid) {
        try {
            const boost::shared_ptr<UserDefinedFunctionDescriptor> &ptr = m_udfMap.at(fid);
            return ptr.get();
        } catch (const std::out_of_range &ex) {
            return NULL;
        }
    }

    inline void clear() {
        m_udfMap.clear();
    }

    inline void addUDF(int32_t fid, int32_t returnType, const std::vector<int32_t> &paramTypes) {
        for (int idx = 0; idx < paramTypes.size(); idx += 1) {
            VOLT_DEBUG("param[%d] = %d", idx, paramTypes[idx]);
        }
        UserDefinedFunctionDescriptor *udfd = new UserDefinedFunctionDescriptor(fid, returnType, paramTypes);
        m_udfMap[fid].reset(udfd);
    }

    void dump() const;
};

} /* namespace voltdb */

#endif /* SRC_EE_EXPRESSIONS_USERDEFINEDFUNCTIONSET_H_ */
