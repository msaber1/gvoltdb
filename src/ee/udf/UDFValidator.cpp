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

#ifndef UDFVALIDATOR_H
#define UDFVALIDATOR_H
#include <dlfcn.h>
#include <jni.h>
#include <vector>
#include <cstdio>

#include "common/FatalException.hpp"
#include "common/types.h"
#include "udf/UDF.h"

using namespace voltdb;
using namespace std;

typedef UserDefinedFunction *(*CreateFunction)();

extern "C" JNIEXPORT jintArray JNICALL Java_org_voltdb_compiler_UDFCompiler_getFunctionPrototype
        (JNIEnv *env, jobject obj, jstring libFilePath, jstring entryName) {
    char *error;
    const char *nativeLibFilePath = env->GetStringUTFChars(libFilePath, 0);
    const char *nativeEntryName = env->GetStringUTFChars(entryName, 0);
    void *libHandle = dlopen(nativeLibFilePath, RTLD_NOW);
    if (! libHandle) {
        fprintf(stderr, "Failed to load shared library file %s\n", nativeLibFilePath);
        throwFatalException("Failed to load shared library file %s", nativeLibFilePath);
    }
    char createFunctionName[50];
    sprintf(createFunctionName, "createFunction%s", nativeEntryName);
    CreateFunction createFunction = (CreateFunction)(dlsym(libHandle, createFunctionName));
    if ((error = dlerror()) != NULL)  {
        fprintf(stderr, "%s\n", error);
        throwFatalException("%s\n", error);
    }
    UserDefinedFunction *udf = createFunction();
    vector<ValueType> argumentTypes = udf->getArgumentTypes();
    ValueType returnType = udf->getReturnType();
    jintArray ret = env->NewIntArray(argumentTypes.size()+1);
    jint *retarr = env->GetIntArrayElements(ret, NULL);
    retarr[0] = returnType;
    for (int i=0; i<argumentTypes.size(); i++) {
        retarr[i+1] = argumentTypes[i];
    }
    env->ReleaseIntArrayElements(ret, retarr, 0);
    env->ReleaseStringUTFChars(libFilePath, nativeLibFilePath);
    env->ReleaseStringUTFChars(entryName, nativeEntryName);
    delete udf;
    return ret;
}

#endif // UDFVALIDATOR_H
