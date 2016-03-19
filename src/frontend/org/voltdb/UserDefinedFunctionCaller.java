package org.voltdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.UDFParameter;

public class UserDefinedFunctionCaller {
    private Method m_method;
    private int m_retType;
    private int [] m_params;
    private Object m_fid;
    public UserDefinedFunctionCaller(Method method,
                                     int fid,
                                     CatalogMap<UDFParameter> params,
                                     int retType) {
        m_fid = fid;
        m_method = method;
        m_params = new int[params.size()];
        for (UDFParameter param : params) {
            assert(0 <= param.getIndex() && param.getIndex() < m_params.length);
            m_params[param.getIndex()] = param.getType();
        }
        m_retType = retType;
    }

    private void checkType(int expectedType, Object value) throws VoltTypeException {
        ;
    }

    public Object call(Object ...actuals) throws InvocationTargetException, IllegalAccessException, IllegalArgumentException {
        // Unpack the parameter set into an object array.
        for (int idx = 0; idx < actuals.length; idx += 1) {
            checkType(m_params[idx], actuals);
        }
        // Do the call.
        Object retVal = m_method.invoke(null, actuals);
        // Check the return type here.
        checkType(m_retType, retVal);
        return retVal;
    }

    @Override
    public String toString() {
        return String.format("UDF(%d;%s)", m_fid, m_method.getName());
    }
}
