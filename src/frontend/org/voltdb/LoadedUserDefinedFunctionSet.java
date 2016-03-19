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
package org.voltdb;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.UDFParameter;
import org.voltdb.catalog.UserDefinedFunction;
import org.voltdb.compiler.Language;

import com.google_voltpatches.common.collect.ImmutableMap;

public class LoadedUserDefinedFunctionSet {

    ImmutableMap<Integer, UserDefinedFunctionCaller> m_UDFs;
    public LoadedUserDefinedFunctionSet() {
    }

    public void loadUserDefinedFunctions(CatalogContext catalogContext) {
        ImmutableMap.Builder<Integer, UserDefinedFunctionCaller> builder
            = loadUserDefinedFunctionsFromCatalog(catalogContext);
        m_UDFs = builder.build();
    }

    private ImmutableMap.Builder<Integer, UserDefinedFunctionCaller> loadUserDefinedFunctionsFromCatalog(CatalogContext catalogContext) {
        final CatalogMap<UserDefinedFunction> catalogUDFs = catalogContext.database.getUdfs();
        final Map<String, Method> sqlNameToMethodCache = new HashMap<String, Method>();

        final Map<String, Class<?>> classCache = new HashMap<String, Class<?>>();

        ImmutableMap.Builder<Integer, UserDefinedFunctionCaller> builder
               = ImmutableMap.<Integer, UserDefinedFunctionCaller>builder();
        classCache.clear();

        for (UserDefinedFunction udf : catalogUDFs) {
            String langString = udf.getLanguage().toUpperCase();
            if (langString == null) {
                langString = "JAVA";
            }
            Language lang = Language.valueOf(langString);
            // Just ignore all UDFs which are not in java.
            // Not possibile now, but perhaps in the future.
            if (lang == Language.JAVA) {
                final String className = udf.getClassname();
                final String sqlName = udf.getSqlname();
                final int    retType = udf.getReturntype();
                int fid = udf.getFuncidx();
                CatalogMap<UDFParameter> params = udf.getParameters();
                Class<?> udfClass = classCache.get(className);
                if (udfClass == null) {
                    try {
                        udfClass = catalogContext.classForProcedure(className);
                        classCache.put(className, udfClass);
                    } catch (ClassNotFoundException e) {
                        final String methodName = udf.getMethodname();
                        VoltDB.crashLocalVoltDB("VoltDB was unable to find the class \""
                                                + className
                                                + "\" for the user defined function \""
                                                + sqlName
                                                + "\".  This should have been defined in the method named \""
                                                + methodName
                                                + "\"");
                    }
                }
                Method method = findMethod(udfClass, sqlName, sqlNameToMethodCache);
                assert(method != null);
                builder.put(fid, new UserDefinedFunctionCaller(method, fid, params, retType));
            }
        }
        return builder;
    }

    private Method findMethod(Class<?> udfClass, String sqlName, Map<String, Method> sqlNameCache) {
        // First, look in the cache.
        Method retval = sqlNameCache.get(sqlName);
        if (retval != null) {
            return retval;
        }
        for (Method meth : udfClass.getMethods()) {
            if (meth.isAnnotationPresent(VoltUserDefinedFunction.class)) {
                int mod = meth.getModifiers();
                if (Modifier.isPublic(mod) && Modifier.isStatic(mod)) {
                    VoltUserDefinedFunction udf = meth.getAnnotation(VoltUserDefinedFunction.class);
                    String callName = udf.CallName();
                    if (callName != null && sqlName.equals(callName)) {
                        return meth;
                    }
                    sqlNameCache.put(callName, meth);
                }
            }
        }
        return null;
    }

    public UserDefinedFunctionCaller getUDFById(int id) {
        return m_UDFs.get(id);
    }
}
