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

#ifndef UNDOQUANTUM_H_
#define UNDOQUANTUM_H_

#include <vector>
#include <stdint.h>
#include <cassert>
#include <string.h>

#include "common/Pool.hpp"
#include "common/UndoAction.h"
#include "common/UndoQuantumReleaseInterest.h"
#include "boost/unordered_set.hpp"

namespace voltdb {

class UndoQuantum {
public:
    inline UndoQuantum(int64_t undoToken, Pool *dataPool)
        : m_undoToken(undoToken), m_numInterests(0), m_interestsCapacity(0), m_interests(NULL), m_dataPool(dataPool) {}
    inline virtual ~UndoQuantum() {}

    virtual void registerUndoAction(UndoAction *undoAction) {
        assert(undoAction);
        m_undoActions.push_back(undoAction);
    }

    virtual void registerInterest(UndoQuantumReleaseInterest *interest) {
        if (m_interests == NULL) {
            m_interests = reinterpret_cast<UndoQuantumReleaseInterest**>(m_dataPool->allocate(sizeof(void*) * 16));
            m_interestsCapacity = 16;
            m_interests[0] = interest;
            m_numInterests = 1;
            return;
        }
        for (int ii = 0; ii < m_numInterests; ii++) {
            if (m_interests[ii] == interest) {
                return;
            }
        }
        if (m_numInterests == m_interestsCapacity) {
            UndoQuantumReleaseInterest **newStorage =
                    reinterpret_cast<UndoQuantumReleaseInterest**>(m_dataPool->allocate(sizeof(void*) * m_interestsCapacity * 2));
            ::memcpy(newStorage, m_interests, sizeof(void*) * m_interestsCapacity);
            m_interests = newStorage;
            m_interestsCapacity *= 2;
        }
        m_interests[m_numInterests++] = interest;
    }

    /*
     * Invoke all the undo actions for this UndoQuantum. UndoActions
     * must have released all memory after undo() is called. Their
     * destructor will never be called because they are allocated out
     * of the data pool which will be purged in one go.
     */
    inline void undo() {
        for (std::vector<UndoAction*>::reverse_iterator i = m_undoActions.rbegin();
             i != m_undoActions.rend(); i++) {
            (*i)->undo();
            (*i)->~UndoAction();
        }
        this->~UndoQuantum();
    }

    /*
     * Call the destructors of all the UndoActions for this
     * UndoQuantum so they will release any resources they still hold.
     * Also call own destructor to ensure that the vector is released.
     */
    inline void release() {
        for (std::vector<UndoAction*>::reverse_iterator i = m_undoActions.rbegin();
             i != m_undoActions.rend(); i++) {
            (*i)->release();
            (*i)->~UndoAction();
        }
        if (m_interests != NULL) {
            for (int ii = 0; ii < m_numInterests; ii++) {
                m_interests[ii]->notifyQuantumRelease();
            }
        }
        this->~UndoQuantum();
    }

    inline int64_t getUndoToken() const {
        return m_undoToken;
    }

    virtual inline Pool* getDataPool() {
        return m_dataPool;
    }

    inline int64_t getAllocatedMemory() const
    {
        return m_dataPool->getAllocatedMemory();
    }

    static void* operator new(std::size_t size, Pool &pool) throw()
    {
        return pool.allocate(size);
    }
    static void operator delete(void *pMemory, Pool &pool) throw() { /* does nothing on failed allocate */ }
    static void operator delete(void *pMemory) throw() { /* does nothing */ }

private:
    const int64_t m_undoToken;
    std::vector<UndoAction*> m_undoActions;
    uint32_t m_numInterests;
    uint32_t m_interestsCapacity;
    UndoQuantumReleaseInterest **m_interests;
protected:
    Pool *const m_dataPool;
};

inline static void* UndoAction::operator new(std::size_t size, Pool &pool) throw()
{
    return pool.allocate(size);
}

}

#endif /* UNDOQUANTUM_H_ */
