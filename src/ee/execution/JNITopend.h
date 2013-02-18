/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef JNITOPEND_H_
#define JNITOPEND_H_
#include "common/Topend.h"
#include "logging/JNILogProxy.h"
#include <jni.h>

namespace voltdb {

class JNITopend : public Topend {
public:
    JNITopend(JNIEnv *env, jobject caller, JavaVM *vm, JNILogProxy* jniLogProxy);
    ~JNITopend();

    void updateJNIEnv(JNIEnv *env)
    {
        m_jniEnv = env;
        m_logProxy->setJNIEnv(env);
    }

    void setParameterBuffer(const char* parameterBuffer, int parameterBufferCapacity)
    {
        m_parameterBuffer = parameterBuffer;
        m_parameterBufferCapacity = parameterBufferCapacity;
    }

    const char* getParameterBuffer(int *parameterBufferCapacity)
    {
        *parameterBufferCapacity = m_parameterBufferCapacity;
        return m_parameterBuffer;
    }

    int loadNextDependency(int32_t dependencyId, Pool *stringPool, voltdb::Table* destination);
    void crashVoltDB(const voltdb::FatalException& e);
    int64_t getQueuedExportBytes(int32_t partitionId, const std::string &signature);
    void pushExportBuffer(
            int64_t exportGeneration,
            int32_t partitionId,
            const std::string &signature,
            voltdb::StreamBlock *block,
            bool sync,
            bool endOfStream);
    void fallbackToEEAllocatedBuffer(char *buffer, size_t length);

private:
    JNIEnv *m_jniEnv;

    /**
     * JNI object corresponding to this engine. for callback functions.
     * if this is NULL, VoltDBEngine will fail to call sendDependency().
    */
    jobject m_javaExecutionEngine;
    jmethodID m_fallbackToEEAllocatedBufferMID;
    jmethodID m_nextDependencyMID;
    jmethodID m_crashVoltDBMID;
    jmethodID m_pushExportBufferMID;
    jmethodID m_getQueuedExportBytesMID;
    jclass m_exportManagerClass;
    JNILogProxy* m_logProxy;
    /** buffer object to pass parameters to EE. */
    const char* m_parameterBuffer;
    /** size of parameter_buffer. */
    int m_parameterBufferCapacity;


};

}
#endif /* JNITOPEND_H_ */
