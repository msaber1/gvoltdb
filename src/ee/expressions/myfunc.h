#define PCRE2_CODE_UNIT_WIDTH 8
#include <string.h>


#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <locale>
#include <iomanip>

namespace voltdb {
template<> inline NValue NValue::callUnary<FUNC_REVSTR>() const {
    if (isNull())
        return getNullStringValue();

    if (getValueType() != VALUE_TYPE_VARCHAR) {
        throwCastSQLException (getValueType(), VALUE_TYPE_VARCHAR);
    }

    int32_t length;
    const char* targetChars = getObject_withoutNull(&length);
    std::string reverseStr(targetChars);
    for (int i=0; i<length/2; i++){

    	char temp = reverseStr[i];
    	reverseStr[i] = reverseStr[length-i-1];
    	reverseStr[length-i-1] = temp;
    }

    return getTempStringValue(reverseStr.c_str(), reverseStr.length());
}
}
