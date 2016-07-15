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

#ifndef UDF_H
#define UDF_H

#include "common/NValue.hpp"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"

namespace voltdb {

class UserDefinedFunction {
public:
    UserDefinedFunction() : m_returnType(VALUE_TYPE_INVALID) {}
    virtual ~UserDefinedFunction() {}

    ValueType getReturnType() { return m_returnType; }

    void setReturnType(ValueType returnType) {
        m_returnType = returnType;
    }

    const std::vector<ValueType>& getArgumentTypes() const {
        return m_argumentTypes;
    }

    void addArgumentOfType(ValueType parameterType) {
        m_argumentTypes.push_back(parameterType);
    }

    virtual UDFType getFunctionType() = 0;

private:
    std::vector<ValueType> m_argumentTypes;
    ValueType m_returnType;
};

class ScalarFunction : public UserDefinedFunction {
public:

    UDFType getFunctionType() {
        return UDF_TYPE_SCALAR;
    }

    NValue execute(const std::vector<NValue>& arguments) {
        m_arguments = arguments;
        return p_execute();
    }

    virtual NValue p_execute() = 0;

protected:
    double getDoubleArgument(int index) {
        return ValuePeeker::peekDouble(m_arguments[index]);
    }

    int8_t getTinyIntArgument(int index) {
        return ValuePeeker::peekTinyInt(m_arguments[index]);
    }

    int16_t getSmallIntArgument(int index) {
        return ValuePeeker::peekSmallInt(m_arguments[index]);
    }

    int32_t getIntegerArgument(int index) {
        return ValuePeeker::peekInteger(m_arguments[index]);
    }

    bool getBooleanArgument(int index) {
        return ValuePeeker::peekBoolean(m_arguments[index]);
    }

private:
    std::vector<NValue> m_arguments;
};

} // namespace voltdb

#endif // UDF_H
