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

#ifndef TUPLESTORAGEINDEX_H_
#define TUPLESTORAGEINDEX_H_

#include <iostream>
#include <cassert>

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "indexes/TupleStorageIndexKey.h"
#include "structures/CompactingMap.h"

namespace voltdb {

    /**
     * Index implemented as a Binary Unique Map.
     * @see TableIndex
     */
    template <std::size_t tupleSize>
    class TupleStorageIndex : public TableIndex
    {
        friend class IndexBasedPersistentTable;

        typedef CompactingMap<
            TupleStorageIndexKey<tupleSize>, char, TupleStorageIndexKeyComparitor
        > MapType;

    public:

        ~CompactingTreeUniqueIndex() {};

        bool addEntry(const TableTuple* tuple)
        {
            m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
            return addEntryPrivate(tuple, m_tmp1);
        }

        bool deleteEntry(const TableTuple* tuple)
        {
            m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
            return deleteEntryPrivate(m_tmp1);
        }

        bool replaceEntry(const TableTuple* oldTupleValue,
                          const TableTuple* newTupleValue)
        {
            // this can probably be optimized
            m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
            m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);

            bool deleted = deleteEntryPrivate(m_tmp1);
            //TODO: addEntry COULD be used here instead of setting and using m_tmp2.
            bool inserted = addEntryPrivate(newTupleValue, m_tmp2);
            --m_deletes;
            --m_inserts;
            ++m_updates;
            return (deleted && inserted);
        }

        /**
         * Update in place an index entry with a new tuple address
         */
        bool replaceEntryNoKeyChange(const TableTuple *oldTupleValue,
                                     const TableTuple *newTupleValue) {
            assert(oldTupleValue->address() != newTupleValue->address());
            m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
            typename MapType::iterator mapiter = m_entries.find(m_tmp1);
            assert(!mapiter.isEnd());
            if (mapiter.isEnd()) {
                return false;
            }
            mapiter.setValue(newTupleValue->address());

            m_updates++;
            return true;
        }

        bool checkForIndexChange(const TableTuple* lhs, const TableTuple* rhs)
        {
            m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
            m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
            return !(m_eq(m_tmp1, m_tmp2));
        }

        bool exists(const TableTuple* values)
        {
            ++m_lookups;
            m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
            return (!m_entries.find(m_tmp1).isEnd());
        }

        bool moveToKey(const TableTuple* searchKey)
        {
            ++m_lookups;
            m_begin = true;
            m_tmp1.setFromKey(searchKey);
            m_keyIter = m_entries.find(m_tmp1);
            if (m_keyIter.isEnd()) {
                m_match.move(NULL);
                return false;
            }
            m_match.move(const_cast<void*>(m_keyIter.value()));
            return !m_match.isNullTuple();
        }

        bool moveToTuple(const TableTuple* searchTuple)
        {
            ++m_lookups;
            m_begin = true;
            m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
            m_keyIter = m_entries.find(m_tmp1);
            if (m_keyIter.isEnd()) {
                m_match.move(NULL);
                return false;
            }
            m_match.move(const_cast<void*>(m_keyIter.value()));
            return !m_match.isNullTuple();
        }

        void moveToKeyOrGreater(const TableTuple* searchKey)
        {
            ++m_lookups;
            m_begin = true;
            m_tmp1.setFromKey(searchKey);
            m_keyIter = m_entries.lowerBound(m_tmp1);
        }

        void moveToGreaterThanKey(const TableTuple* searchKey)
        {
            ++m_lookups;
            m_begin = true;
            m_tmp1.setFromKey(searchKey);
            m_keyIter = m_entries.upperBound(m_tmp1);
        }

        void moveToEnd(bool begin)
        {
            ++m_lookups;
            m_begin = begin;
            if (begin)
                m_keyIter = m_entries.begin();
            else
                m_keyIter = m_entries.rbegin();
        }

        TableTuple nextValue()
        {
            TableTuple retval(m_tupleSchema);

            if (m_begin) {
                if (m_keyIter.isEnd())
                    return TableTuple();
                retval.move(const_cast<void*>(m_keyIter.value()));
                m_keyIter.moveNext();
            } else {
                if (m_keyIter.isEnd())
                    return TableTuple();
                retval.move(const_cast<void*>(m_keyIter.value()));
                m_keyIter.movePrev();
            }

            return retval;
        }

        TableTuple nextValueAtKey()
        {
            TableTuple retval = m_match;
            m_match.move(NULL);
            return retval;
        }

        bool advanceToNextKey()
        {
            if (m_begin) {
                m_keyIter.moveNext();
                if (m_keyIter.isEnd())
                {
                    m_match.move(NULL);
                    return false;
                }
                m_match.move(const_cast<void*>(m_keyIter.value()));
            } else {
                m_keyIter.movePrev();
                if (m_keyIter.isEnd())
                {
                    m_match.move(NULL);
                    return false;
                }
                m_match.move(const_cast<void*>(m_keyIter.value()));
            }

            return !m_match.isNullTuple();
        }

        size_t getSize() const { return static_cast<size_t>(m_entries.size()); }

        int64_t getMemoryEstimate() const
        {
            return m_entries.bytesAllocated();
        }

        std::string getTypeName() const { return "CompactingTreeUniqueIndex"; };
        std::string debug() const
        {
            std::ostringstream buffer;
            buffer << TableIndex::debug() << std::endl;

            typename MapType::iterator i = m_entries.begin();
            while (!i.isEnd()) {
                TableTuple retval(m_tupleSchema);
                retval.move(const_cast<void*>(i.value()));
                buffer << retval.debugNoHeader() << std::endl;
                i.moveNext();
            }
            std::string ret(buffer.str());
            return (ret);
        }
    protected:
        CompactingTreeUniqueIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_entries(true, KeyComparator(m_keySchema)),
        m_begin(true),
        m_eq(m_keySchema)
        {
            m_match = TableTuple(m_tupleSchema);
        }

        inline bool addEntryPrivate(const TableTuple* tuple, const KeyType &key)
        {
            ++m_inserts;
            return m_entries.insert(std::pair<KeyType, const void*>(key, tuple->address()));
        }

        inline bool deleteEntryPrivate(const KeyType &key)
        {
            ++m_deletes;
            return m_entries.erase(key);
        }

        MapType m_entries;
        KeyType m_tmp1;
        KeyType m_tmp2;

        // iteration stuff
        bool m_begin;
        typename MapType::iterator m_keyIter;
        TableTuple m_match;

        // comparison stuff
        TupleStorageIndexKeyEqualityChecker m_eq;
    };

}

#endif // TUPLESTORAGEINDEX_H_
