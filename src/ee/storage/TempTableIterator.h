/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef TEMPTABLEITERATOR_H
#define TEMPTABLEITERATOR_H

#include <cassert>

#include "storage/TupleBlock.h"
#include "storage/TupleIterator.h"

namespace voltdb {

    class TempTable;

    /**
     * Iterator for table which neglects deleted tuples.
     * TableIterator is a small and copiable object.
     * You can copy it, not passing a pointer of it.
     *
     * This class should be a virtual interface or should
     * be templated on the underlying table data iterator.
     * Either change requires some updating of the iterators
     * that are persvasively stack allocated...
     *
     */
    class TempTableIterator : public TupleIterator {

        friend class TempTable;

    public:

        /**
         * Updates the given tuple so that it points to the next tuple in the table.
         * @param out the tuple will point to the retrieved tuple if this method returns true.
         * @return true if succeeded. false if no more active tuple is there.
         */
        virtual bool next(TableTuple &out);
        virtual bool hasNext();

    protected:
        // Get an iterator via table->iterator()
        TempTableIterator(std::vector<TBPtr>::iterator start,
                          uint32_t tupleCount,
                          uint32_t tuplesPerBlock,
                          uint32_t tupleLength);

        void reset(std::vector<TBPtr>::iterator start,
                   uint32_t tupleCount,
                   uint32_t tuplesPerBlock,
                   uint32_t tupleLength);

        inline bool tempNext(TableTuple &out);

        /*
         * Configuration parameter that controls whether the table iterator
         * stops when it has found the expected number of tuples or when it has iterated
         * all the blocks. The former is able to stop sooner without having to read to the end of
         * of the block. The latter is useful when the table will be modified after the creation of
         * the iterator. It is assumed that the code invoking this iterator is handling
         * the modifications that occur after the iterator is created.
         *
         * When set to false the counting of found tuples method is used. When set to true
         * all blocks are scanned.
         */
        char *m_dataPtr;
        uint32_t m_blockOffset;
        uint32_t m_activeTuples;
        uint32_t m_foundTuples;
        uint32_t m_tupleLength;
        uint32_t m_tuplesPerBlock;
        TBPtr m_currentBlock;
        std::vector<TBPtr>::iterator m_tempBlockIterator;
    };

    inline TempTableIterator::TempTableIterator(std::vector<TBPtr>::iterator start,
                                                uint32_t tupleCount,
                                                uint32_t tuplesPerBlock,
                                                uint32_t tupleLength) {
        reset(start, tupleCount, tuplesPerBlock, tupleLength);
    }

    inline void TempTableIterator::reset(std::vector<TBPtr>::iterator start,
                                         uint32_t tupleCount,
                                         uint32_t tuplesPerBlock,
                                         uint32_t tupleLength) {
        m_tempBlockIterator = start;
        m_dataPtr = NULL;
        m_blockOffset = 0;
        m_activeTuples = tupleCount;
        m_foundTuples = 0;
        m_tupleLength = tupleLength;
        m_tuplesPerBlock = tuplesPerBlock;
        m_currentBlock = NULL;
    }

    inline bool TempTableIterator::hasNext() {
        return m_foundTuples < m_activeTuples;
    }

    inline bool TempTableIterator::next(TableTuple &out) {
        return tempNext(out);
    }

    inline bool TempTableIterator::tempNext(TableTuple &out) {
        if (m_foundTuples < m_activeTuples) {
            if (m_currentBlock == NULL ||
                m_blockOffset >= m_currentBlock->unusedTupleBoundry())
            {
                m_currentBlock = *m_tempBlockIterator;
                m_dataPtr = m_currentBlock->address();
                m_blockOffset = 0;
                m_tempBlockIterator++;
            } else {
                m_dataPtr += m_tupleLength;
            }
            out.move(m_dataPtr);

            ++m_blockOffset;
            ++m_foundTuples;

            return true;
        }

        return false;
    }
}

#endif // TEMPTABLEITERATOR_H
