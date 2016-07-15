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

#ifndef UDFLIBRARY_H
#define UDFLIBRARY_H

#include <dlfcn.h>
#include <map>
#include <cstdio>

#include "common/FatalException.hpp"
#include "udf/UDF.h"

namespace voltdb {

typedef UserDefinedFunction *(*CreateFunction)();

class UDFLibrary {
public:
    UDFLibrary(const string &libraryPath) {
        m_libHandle = dlopen(libraryPath.c_str(), RTLD_NOW);
        if (! m_libHandle) {
            throwFatalException("Failed to load shared library file %s", libraryPath.c_str());
        }
    }
    ~UDFLibrary() {
        dlclose(m_libHandle);
    }

    ScalarFunction *loadScalarFunction(const string &functionName, const string &entryName) {
        char *error;
        char createFunctionName[50];
        sprintf(createFunctionName, "createFunction%s", entryName.c_str());
        CreateFunction createFunction = (CreateFunction)(dlsym(m_libHandle, createFunctionName));
        if ((error = dlerror()) != NULL)  {
            throwFatalException("%s\n", error);
        }
        return static_cast<ScalarFunction*>(createFunction());
    }
private:
    void *m_libHandle;
};

} // namespace voltdb

#endif // UDFLIBRARY_H
