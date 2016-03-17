package org.voltdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.UDFParameter;

public class UserDefinedFunctionCaller {
    private Method m_method;
    private int m_retType;
    private CatalogMap<UDFParameter> m_params;
    public UserDefinedFunctionCaller(Method method,
                                     CatalogMap<UDFParameter> params,
                                     int retType) {
        m_method = method;
        m_params = params;
        m_retType = retType;
    }

    private void checkType(int expectedType, Object value) throws VoltTypeException {
        ;
    }

    Object call(ParameterSet params) throws InvocationTargetException, IllegalAccessException, IllegalArgumentException {
        // Unpack the parameter set into an object array.
        Object[] actuals = new Object[params.size()];
        for (int idx = 0; idx < params.size(); idx += 1) {
            // Check the argument types here.
            actuals[idx] = params.getParam(idx);
        }
        // Check the return type here.
        Object retVal = m_method.invoke(null, actuals);
        checkType(m_retType, retVal);
        return retVal;
    }
}
