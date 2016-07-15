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
#include <sstream>
#include <string>
#include <limits.h>
#include "common/NValue.hpp"

namespace voltdb {

template<> inline NValue NValue::callUnary<FUNC_VOLT_BITNOT>() const {
    if (getValueType() != VALUE_TYPE_BIGINT) {
        // The parser should enforce this for us, but just in case...
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-BigInt type for SQL BITNOT function");
    }

    if (isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    }

    int64_t result = ~(getBigInt());
    if (result == INT64_NULL) {
        throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                           "Application of bitwise function BITNOT would produce INT64_MIN, "
                           "which is reserved for SQL NULL values.");
    }

    return getBigIntValue(result);
}

} // namespace voltdb