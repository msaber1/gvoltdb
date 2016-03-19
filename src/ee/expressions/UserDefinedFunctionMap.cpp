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

#include <iostream>
#include "expressions/UserDefinedFunctionMap.h"
#include "catalog/udfparameter.h"

namespace voltdb {
void UserDefinedFunctionMap::dump() const {
    for (cmapiterator it = m_udfMap.begin(); it != m_udfMap.end(); it++) {
        const UserDefinedFunctionDescriptor *udf = it->second.get();
        std::cout << "User Defined Function: fid = "
                  << udf->getFid()
                  << ", return type = "
                  << udf->getReturnType()
                  << ", params = [";
        std::string sep("");
        const std::vector<int32_t> &paramlist = udf->getParamTypes();
        for (int idx = 0; idx < paramlist.size(); idx += 1) {
            int32_t type_id = paramlist[idx];
            std::cout << sep << type_id;
            sep = ", ";
        }
        std::cout << "]\n";
    }
}

}
