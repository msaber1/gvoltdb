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

#include <sstream>
#include <cstdio>
#include "common/TupleSchema.h"
#include "common/NValue.hpp"

namespace voltdb {

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType>& columnTypes,
                                            const std::vector<int32_t>& columnSizes,
                                            const std::vector<bool>& allowNull)
{
    const uint16_t uninlineableObjectColumnCount =
            TupleSchema::countUninlineableObjectColumns(columnTypes, columnSizes);
    const uint16_t columnCount = static_cast<uint16_t>(columnTypes.size());
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    // Also allocate space for an int16_t for each uninlineable object column so that
    // the indices of uninlineable columns can be stored at the front and aid in iteration
    int memSize = (int)(sizeof(TupleSchema) +
                        (sizeof(ColumnInfo) * (columnCount + 1)) +
                        (uninlineableObjectColumnCount * sizeof(int16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memset(retval, 0, memSize);
    retval->m_columnCount = columnCount;
    retval->m_uninlinedObjectColumnCount = uninlineableObjectColumnCount;

    uint16_t uninlinedObjectColumnIndex = 0;
    for (uint16_t ii = 0; ii < columnCount; ii++) {
        const ValueType type = columnTypes[ii];
        const uint32_t length = columnSizes[ii];
        const bool columnAllowNull = allowNull[ii];
        retval->setColumnMetaData(ii, type, length, columnAllowNull, uninlinedObjectColumnIndex);
    }

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType>& columnTypes,
                                            const std::vector<int32_t>& columnSizes)
{
    const std::vector<bool> allowNull(columnTypes.size(), true);
    return createTupleSchema(columnTypes, columnSizes, allowNull);
}

TupleSchema* TupleSchema::cloneSchema() const
{
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    int memSize =
            (int)(sizeof(TupleSchema) +
                    (sizeof(ColumnInfo) * (m_columnCount + 1)) +
                    (m_uninlinedObjectColumnCount * sizeof(uint16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // copy all the member values
    memcpy(retval, this, memSize);

    return retval;
}

TupleSchema* TupleSchema::createTestUniformTupleSchema(int column_count, bool allowNull,
                                                       ValueType vt, int value_length)
{
    std::vector<bool> columnAllowNull(column_count, allowNull);
    std::vector<voltdb::ValueType> columnTypes(column_count, vt);
    if (value_length == -1) {
        value_length = getTupleStorageSize(vt);
    }
    std::vector<int32_t> columnLengths(column_count, value_length);
    return createTupleSchema(columnTypes, columnLengths, columnAllowNull);
}

TupleSchema* TupleSchema::createTestFullCombinedTupleSchema(const TupleSchema *first,
                                                            const TupleSchema *second)
{
    assert(first);
    assert(second);

    std::vector<uint16_t> firstSet;
    std::vector<uint16_t> secondSet;

    for (uint16_t i = 0; i < first->columnCount(); i++) {
        firstSet.push_back(i);
    }
    for (uint16_t i = 0; i < second->columnCount(); i++) {
        secondSet.push_back(i);
    }

    return createTestCombinedTupleSchema(first, firstSet, second, secondSet);
}

TupleSchema*
TupleSchema::createTestCombinedTupleSchema(const TupleSchema *first,
        const std::vector<uint16_t>& firstSet,
        const TupleSchema *second,
        const std::vector<uint16_t>& secondSet)
{
    assert(first);

    const std::vector<uint16_t>::size_type combinedColumnCount = firstSet.size() + secondSet.size();
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull;
    std::vector<uint16_t>::const_iterator iter;
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        columnTypes.push_back(first->columnType(*iter));
        columnLengths.push_back(first->columnLength(*iter));
        columnAllowNull.push_back(first->columnAllowNull(*iter));
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        columnTypes.push_back(second->columnType(*iter));
        columnLengths.push_back(second->columnLength(*iter));
        columnAllowNull.push_back(second->columnAllowNull(*iter));
    }

    TupleSchema *schema = createTupleSchema(columnTypes, columnLengths, columnAllowNull);
    return schema;
}

void TupleSchema::freeTupleSchema(TupleSchema *schema) {
    delete[] reinterpret_cast<char*>(schema);
}

/**
 * Get the amount of storage necessary to store a value of the specified type
 * in a tuple
 */
uint16_t TupleSchema::getTupleStorageSize(ValueType type)
{
    switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        return sizeof(int64_t);
    case VALUE_TYPE_TINYINT:
        return sizeof(int8_t);
    case VALUE_TYPE_SMALLINT:
        return sizeof(int16_t);
    case VALUE_TYPE_INTEGER:
        return sizeof(int32_t);
    case VALUE_TYPE_DOUBLE:
        return sizeof(double);
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
        return sizeof(char*);
    case VALUE_TYPE_DECIMAL:
        return sizeof(TTInt);
    default: {
        char message[128];
        snprintf(message, 128, "TupleSchema::getTupleStorageSize() unsupported type '%s'",
                 getTypeName(type).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    }
}

void TupleSchema::setColumnMetaData(uint16_t index, ValueType type, const int32_t length, bool allowNull,
                                    uint16_t &uninlinedObjectColumnIndex)
{
    assert(length <= COLUMN_MAX_VALUE_LENGTH);
    uint32_t offset = 0;

    // set the type
    ColumnInfo *columnInfo = getColumnInfo(index);
    columnInfo->type = static_cast<char>(type);
    columnInfo->allowNull = (char)(allowNull ? 1 : 0);
    columnInfo->length = length;
    if (isObjectType(type)) {
        if (length < UNINLINEABLE_OBJECT_LENGTH) {
            if (length == 0) {
                throwFatalLogicErrorStreamed("Zero length for object type " << valueToString((ValueType)type));
            }
            /*
             * Inline the string if it is less then UNINLINEABLE_OBJECT_LENGTH bytes.
             */
            columnInfo->inlined = true;
            // One byte to store the size
            offset = static_cast<uint32_t>(length + SHORT_OBJECT_LENGTHLENGTH);
        } else {
            /*
             * Set the length to the size of a String pointer since it won't be inlined.
             */
            offset = static_cast<uint32_t>(getTupleStorageSize(type));
            columnInfo->inlined = false;
            setUninlinedObjectColumnInfoIndex(uninlinedObjectColumnIndex++, index);
        }
    } else {
        // All values are inlined if they aren't strings/varbinary.
        columnInfo->inlined = true;
        // don't trust the planner since it can be avoided
        offset = static_cast<uint32_t>(getTupleStorageSize(type));
    }
    // make the column offsets right for all columns past this one
    int oldsize = columnLengthPrivate(index);
    ColumnInfo *nextColumnInfo = NULL;
    for (int i = index + 1; i <= m_columnCount; i++) {
        nextColumnInfo = getColumnInfo(i);
        nextColumnInfo->offset = static_cast<uint32_t>(nextColumnInfo->offset + offset - oldsize);
    }
    assert((index == 0) == (columnInfo->offset == 0));
}

std::string TupleSchema::debug() const {
    std::ostringstream buffer;

    buffer << "Schema has " << columnCount() << " columns, length = "
           << tupleLength() <<  ", uninlinedObjectColumns "  << m_uninlinedObjectColumnCount
           << std::endl;

    for (uint16_t i = 0; i < columnCount(); i++) {
        buffer << " column " << i << ": type = " << getTypeName(columnType(i));
        buffer << ", length = " << columnLength(i) << ", nullable = ";
        buffer << (columnAllowNull(i) ? "true" : "false") << ", isInlined = " << columnIsInlined(i) <<  std::endl;
    }

    std::string ret(buffer.str());
    return ret;
}

bool TupleSchema::equals(const TupleSchema *other) const {
    if (other->m_columnCount != m_columnCount ||
        other->m_uninlinedObjectColumnCount != m_uninlinedObjectColumnCount) {
        return false;
    }

    for (int ii = 0; ii < m_columnCount; ii++) {
        const ColumnInfo *columnInfo = getColumnInfo(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfo(ii);
        if (columnInfo->allowNull != ocolumnInfo->allowNull ||
                columnInfo->offset != ocolumnInfo->offset ||
                columnInfo->type != ocolumnInfo->type) {
            return false;
        }
    }

    return true;
}

/*
 * Returns the number of string columns that can't be inlined.
 */
uint16_t TupleSchema::countUninlineableObjectColumns(
        const std::vector<ValueType> columnTypes,
        const std::vector<int32_t> columnSizes) {
    const uint16_t numColumns = static_cast<uint16_t>(columnTypes.size());
    uint16_t numUninlineableObjects = 0;
    for (int ii = 0; ii < numColumns; ii++) {
        if (isObjectType(columnTypes[ii])) {
            if (columnSizes[ii] >= UNINLINEABLE_OBJECT_LENGTH) {
                numUninlineableObjects++;
            }
        }
    }
    return numUninlineableObjects;
}

} // namespace voltdb
