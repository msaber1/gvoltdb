/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#ifndef TUPLESTORAGEINDEXKEY_H_
#define TUPLESTORAGEINDEXKEY_H_

template <std::size_t tupleSize>
class TupleStorageIndexKey {
public:
    inline TupleKey() {
        m_columnIndices = NULL;
        m_keyTuple = NULL;
        m_keyTupleSchema = NULL;
    }

    // Set a key from a key-schema tuple.
    inline void setFromKey(const TableTuple *tuple) {
        assert(tuple);
        m_columnIndices = NULL;
        m_keyTuple = tuple->address();
        m_keyTupleSchema = tuple->getSchema();
    }

    // Set a key from a table-schema tuple.
    inline void setFromTuple(const TableTuple *tuple, const int *indices, const TupleSchema *keySchema) {
        assert(tuple);
        assert(indices);
        m_columnIndices = indices;
        m_keyTuple = tuple->address();
        m_keyTupleSchema = tuple->getSchema();
    }

    // Return true if the TupleKey references an ephemeral index key.
    bool isKeySchema() const {
        return m_columnIndices == NULL;
    }

    // Return a table tuple that is valid for comparison
    TableTuple getTupleForComparison() const {
        return TableTuple(m_keyTuple, m_keyTupleSchema);
    }

    // Return the indexColumn'th key-schema column.
    int columnForIndexColumn(int indexColumn) const {
        if (isKeySchema())
            return indexColumn;
        else
            return m_columnIndices[indexColumn];
    }

    size_t getKeySize() const
    {
        return sizeof(int*) + sizeof(char*) + sizeof(TupleSchema*);
    }

private:
    // TableIndex owns this array - NULL if an ephemeral key
    const int* m_columnIndices;

    // Pointer a persistent tuple in non-ephemeral case.
    char *m_keyTuple;
    const TupleSchema *m_keyTupleSchema;
};

class TupleStorageKeyLessComparator {
public:
    TupleStorageKeyLessComparator(TupleSchema *keySchema) : m_schema(keySchema) {
    }

    // return true if lhs < rhs
    inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
        TableTuple lhTuple = lhs.getTupleForComparison();
        TableTuple rhTuple = rhs.getTupleForComparison();
        NValue lhValue, rhValue;

        for (int ii=0; ii < m_schema->columnCount(); ++ii) {
            lhValue = lhTuple.getNValue(lhs.columnForIndexColumn(ii));
            rhValue = rhTuple.getNValue(rhs.columnForIndexColumn(ii));

            int comparison = lhValue.compare(rhValue);

            if (comparison == VALUE_COMPARE_LESSTHAN) {
                return true;
            }
            else if (comparison == VALUE_COMPARE_GREATERTHAN) {
                return false;
            }
        }
        return false;
    }

    TupleSchema *m_schema;
};

class TupleStorageIndexKeyComparator {
public:
    TupleKeyComparator(TupleSchema *keySchema) : m_schema(keySchema) {
    }

    // return true if lhs < rhs
    inline int operator()(const TupleKey &lhs, const TupleKey &rhs) const {
        TableTuple lhTuple = lhs.getTupleForComparison();
        TableTuple rhTuple = rhs.getTupleForComparison();
        NValue lhValue, rhValue;

        for (int ii=0; ii < m_schema->columnCount(); ++ii) {
            lhValue = lhTuple.getNValue(lhs.columnForIndexColumn(ii));
            rhValue = rhTuple.getNValue(rhs.columnForIndexColumn(ii));

            int comparison = lhValue.compare(rhValue);

            if (comparison == VALUE_COMPARE_LESSTHAN) return -1;
            else if (comparison == VALUE_COMPARE_GREATERTHAN) return 1;
        }
        return 0;
    }

    TupleSchema *m_schema;
};

class TupleStorageIndexKeyEqualityChecker {
public:
    TupleKeyEqualityChecker(TupleSchema *keySchema) : m_schema(keySchema) {
    }

    // return true if lhs == rhs
    inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
        TableTuple lhTuple = lhs.getTupleForComparison();
        TableTuple rhTuple = rhs.getTupleForComparison();
        NValue lhValue, rhValue;

        //         std::cout << std::endl << "TupleKeyEqualityChecker: " <<
        //         std::endl << lhTuple.debugNoHeader() <<
        //         std::endl << rhTuple.debugNoHeader() <<
        //         std::endl;

        for (int ii=0; ii < m_schema->columnCount(); ++ii) {
            lhValue = lhTuple.getNValue(lhs.columnForIndexColumn(ii));
            rhValue = rhTuple.getNValue(rhs.columnForIndexColumn(ii));

            if (lhValue.compare(rhValue) != VALUE_COMPARE_EQUAL) {
                return false;
            }
        }
        return true;
    }

    TupleSchema *m_schema;
};

#endif // TUPLESTORAGEINDEXKEY_H_
