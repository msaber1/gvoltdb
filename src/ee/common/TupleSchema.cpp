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

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType> columnTypes,
                                            const std::vector<int32_t> columnSizes,
                                            const std::vector<bool> allowNull,
                                            bool allowInlinedObjects)
{
    const uint16_t uninlineableObjectColumnCount =
      TupleSchema::countUninlineableObjectColumns(columnTypes, columnSizes, allowInlinedObjects);
    const uint16_t columnCount = static_cast<uint16_t>(columnTypes.size());
    // big enough for any data members plus big enough for tupleCount "ColumnInfo" fields.
    // Also allocate space for an int16_t for each uninlineable object column so that
    // the indices of uninlineable columns can be stored at the front and aid in iteration
    int memSize = (int)(sizeof(TupleSchema) +
                        (sizeof(ColumnInfo) * (columnCount + 1)) +
                        (uninlineableObjectColumnCount * sizeof(int16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memset(retval, 0, memSize);
    retval->m_allowInlinedObjects = allowInlinedObjects;
    retval->m_columnCount = columnCount;
    retval->m_uninlinedObjectColumnCount = uninlineableObjectColumnCount;

    uint16_t uninlinedObjectColumnIndex = 0;
    uint32_t offset = 0;
    for (uint16_t ii = 0; ii < columnCount; ii++) {
        const ValueType type = columnTypes[ii];
        const uint32_t length = columnSizes[ii];
        const bool columnAllowNull = allowNull[ii];
        // TODO: ENG-5876 declaredUnitIsBytes will need to be overridable from an input vector
        // to support "COL VARCHAR(30 BYTES)"
        const bool declaredUnitIsBytes = (type == VALUE_TYPE_VARCHAR);
        retval->setColumnMetaData(ii, type, length, declaredUnitIsBytes, columnAllowNull,
                                  &offset, &uninlinedObjectColumnIndex);
    }
    retval->m_tupleLength = offset;
    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema) {
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    int memSize =
            (int)(sizeof(TupleSchema) +
                    (sizeof(ColumnInfo) * (schema->m_columnCount + 1)) +
                    (schema->m_uninlinedObjectColumnCount * sizeof(uint16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memcpy(retval, schema, memSize);

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema,
                                            const std::vector<uint16_t> set) {
    return createTupleSchema(schema, set, NULL, std::vector<uint16_t>());
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *first,
                                            const TupleSchema *second) {
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

    return createTupleSchema(first, firstSet, second, secondSet);
}

TupleSchema*
TupleSchema::createTupleSchema(const TupleSchema *first,
                               const std::vector<uint16_t> firstSet,
                               const TupleSchema *second,
                               const std::vector<uint16_t> secondSet) {
    assert(first);

    const std::vector<uint16_t>::size_type offset = firstSet.size();
    const std::vector<uint16_t>::size_type combinedColumnCount = firstSet.size()
        + secondSet.size();
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(combinedColumnCount, true);
    std::vector<uint16_t>::const_iterator iter;
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        columnTypes.push_back(first->columnType(*iter));
        columnLengths.push_back(first->columnDeclaredLength(*iter));
        columnAllowNull[*iter] = first->columnAllowNull(*iter);
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        columnTypes.push_back(second->columnType(*iter));
        columnLengths.push_back(second->columnDeclaredLength(*iter));
        columnAllowNull[offset + *iter] = second->columnAllowNull(*iter);
    }

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes,
                                                         columnLengths,
                                                         columnAllowNull,
                                                         true);

    // Remember to set the inlineability of each column correctly.
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        ColumnInfo *info = schema->getColumnInfo(*iter);
        info->inlined = first->columnIsInlined(*iter);
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        ColumnInfo *info = schema->getColumnInfo((int)offset + *iter);
        info->inlined = second->columnIsInlined(*iter);
    }

    return schema;
}

void TupleSchema::freeTupleSchema(TupleSchema *schema) {
    delete[] reinterpret_cast<char*>(schema);
}

void TupleSchema::setColumnMetaData(uint16_t index, ValueType type, int32_t length,
        bool lengthIsBytes, bool allowNull, uint32_t *offset, uint16_t *uninlinedObjectColumnIndex)
{
    assert(length <= COLUMN_MAX_VALUE_LENGTH);

    // set the type
    ColumnInfo *columnInfo = getColumnInfo(index);
    columnInfo->offset = *offset;
    columnInfo->type = static_cast<char>(type);
    columnInfo->allowNull = allowNull;
    if (isObjectType(type)) {
        columnInfo->declaredLength = length;
        columnInfo->declaredUnitIsBytes = lengthIsBytes;
        if ( ! lengthIsBytes) {
            length *= MAX_UTF8_BYTES_PER_CHARACTER;
        }
        if (length < UNINLINEABLE_OBJECT_LENGTH && m_allowInlinedObjects) {
            if (length == 0) {
                throwFatalLogicErrorStreamed("Zero length for object type " << valueToString((ValueType)type));
            }
            /*
             * Inline the string if it is less then UNINLINEABLE_OBJECT_LENGTH bytes.
             */
            columnInfo->inlined = true;
            // One byte to store the size
            *offset += static_cast<uint32_t>(length + SHORT_OBJECT_LENGTHLENGTH);
        } else {
            columnInfo->inlined = false;
            setUninlinedObjectColumnInfoIndex((*uninlinedObjectColumnIndex)++, index);
            /*
             * Set the length to the size of a String pointer since it won't be inlined.
             */
            *offset += static_cast<uint32_t>(NValue::getTupleStorageSize(type));
        }
    } else {
        // All values are inlined if they aren't strings/varbinary.
        columnInfo->inlined = true;
        // don't trust the planner since it can be avoided
        *offset += static_cast<uint32_t>(NValue::getTupleStorageSize(type));
    }
}

std::string TupleSchema::debug() const {
    std::ostringstream buffer;

    buffer << "Schema has " << columnCount() << " columns, allowInlinedObjects = " << allowInlinedObjects()
           << ", length = " << tupleLength() <<  ", uninlinedObjectColumns "  << m_uninlinedObjectColumnCount
           << std::endl;

    for (uint16_t i = 0; i < columnCount(); i++) {
        buffer << " column " << i << ": type = " << getTypeName(columnType(i));
        if (isObjectType((ValueType)columnType(i))) {
            buffer << ", length = " << columnDeclaredLength(i);
            buffer << (columnDeclaredUnitIsBytes(i) ? " bytes" : " characters");
            buffer << ", isInlined = " << columnIsInlined(i);
        }
        buffer << ", nullable = " << (columnAllowNull(i) ? "true" : "false") <<  std::endl;
    }

    std::string ret(buffer.str());
    return ret;
}

bool TupleSchema::equals(const TupleSchema *other) const {
    if (this == other) {
        return true;
    }
    if (other->m_columnCount != m_columnCount ||
        other->m_uninlinedObjectColumnCount != m_uninlinedObjectColumnCount ||
        other->m_allowInlinedObjects != m_allowInlinedObjects) {
        return false;
    }

    // columns should have identical scalar members (and zero'd out pad bytes, if any).
    const char* columnInfo = reinterpret_cast<const char*>(getColumnInfo(0));
    const char* endColumnInfo = reinterpret_cast<const char*>(getColumnInfo(m_columnCount));
    const char* oColumnInfo = reinterpret_cast<const char*>(other->getColumnInfo(0));

    return (0 == std::memcmp(columnInfo, oColumnInfo, endColumnInfo - columnInfo));
}

bool TupleSchema::isCompatibleForCopy(const TupleSchema *other) const {
    if (equals(other)) {
        return true;
    }
    const ColumnInfo* columnInfo = getColumnInfo(0);
    const ColumnInfo* endColumnInfo = getColumnInfo(m_columnCount);
    const ColumnInfo* oColumnInfo = other->getColumnInfo(0);

    for ( ; columnInfo < endColumnInfo; ++columnInfo, ++oColumnInfo) {
        if (columnInfo->type != oColumnInfo->type) {
            return false;
        }
        if ( ! isObjectType((ValueType)columnInfo->type)) {
            continue;
        }
        if (columnInfo->declaredLength != oColumnInfo->declaredLength) {
            return false;
        }
        if (columnInfo->declaredUnitIsBytes != oColumnInfo->declaredUnitIsBytes) {
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
        const std::vector<int32_t> columnSizes,
        bool allowInlineObjects) {
    const uint16_t numColumns = static_cast<uint16_t>(columnTypes.size());
    uint16_t numUninlineableObjects = 0;
    for (int ii = 0; ii < numColumns; ii++) {
        if ((columnTypes[ii] == VALUE_TYPE_VARCHAR) || ((columnTypes[ii] == VALUE_TYPE_VARBINARY))) {
            if (!allowInlineObjects) {
                numUninlineableObjects++;
            } else if (columnSizes[ii] >= UNINLINEABLE_OBJECT_LENGTH) {
                numUninlineableObjects++;
            }
        }
    }
    return numUninlineableObjects;
}

} // namespace voltdb
