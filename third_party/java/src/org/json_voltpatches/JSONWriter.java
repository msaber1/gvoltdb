package org.json_voltpatches;

import java.io.IOException;
import java.io.Writer;

/*
Copyright (c) 2006 JSON.org

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

The Software shall be used for Good, not Evil.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/**
 * JSONWriter provides a quick and convenient way of producing JSON text.
 * The texts produced strictly conform to JSON syntax rules. No whitespace is
 * added, so the results are ready for transmission or storage. Each instance of
 * JSONWriter can produce one JSON text.
 * <p>
 * A JSONWriter instance provides a <code>value</code> method for appending
 * values to the
 * text, and a <code>key</code>
 * method for adding keys before values in objects. There are <code>array</code>
 * and <code>endArray</code> methods that make and bound array values, and
 * <code>object</code> and <code>endObject</code> methods which make and bound
 * object values. All of these methods return the JSONWriter instance,
 * permitting a cascade style. For example, <pre>
 * new JSONWriter(myWriter)
 *     .object()
 *         .key("JSON")
 *         .value("Hello, World!")
 *     .endObject();</pre> which writes <pre>
 * {"JSON":"Hello, World!"}</pre>
 * <p>
 * The first method called must be <code>array</code> or <code>object</code>.
 * There are no methods for adding commas or colons. JSONWriter adds them for
 * you. Objects and arrays can be nested up to 20 levels deep.
 * <p>
 * This can sometimes be easier than using a JSONObject to build a string.
 * @author JSON.org
 * @version 2010-03-11
 */
public class JSONWriter {
    private static final int MAXDEPTH = 20;

    /**
     * The comma flag determines if a comma should be output before the next
     * value.
     */
    private boolean comma;

    /**
     * The current mode. Values:
     * 'a' (array),
     * 'd' (done),
     * 'i' (initial),
     * 'k' (key),
     * 'o' (object).
     */
    protected char mode;

    /**
     * The object/array stack.
     */
    private final JSONObject[] stack = new JSONObject[MAXDEPTH];

    /**
     * The stack top index. A value of 0 indicates that the stack is empty.
     */
    private int top;

    /**
     * The writer that will receive the output.
     */
    protected final Writer writer;

    /**
     * Make a fresh JSONWriter. It can be used to build one JSON text.
     */
    public JSONWriter(Writer w) {
        this.comma = false;
        this.mode = 'i';
        this.top = 0;
        this.writer = w;
    }

    /**
     * Append a value.
     * @param s A string value.
     * @return this
     * @throws JSONException If the value is out of sequence.
     */
    private JSONWriter append(String s) throws JSONException {
        if (s == null) {
            throw new JSONException("Null pointer");
        }
        try {
            if (this.mode == 'o') {
                this.mode = 'k';
            }
            else if (this.mode == 'a') {
                if (this.comma) {
                    this.writer.write(',');
                }
            }
            else {
                throw new JSONException("Value out of sequence.");
            }

            this.writer.write(s);
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        this.comma = true;
        return this;
    }

    /**
     * Begin appending a new array. All values until the balancing
     * <code>endArray</code> will be appended to this array. The
     * <code>endArray</code> method must be called to mark the array's end.
     * @return this
     * @throws JSONException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public JSONWriter array() throws JSONException {
        if (this.mode == 'i' || this.mode == 'o' || this.mode == 'a') {
            // Push an array scope.
            if (this.top >= MAXDEPTH) {
                throw new JSONException("Nesting too deep.");
            }
            this.stack[this.top++] = null;
            try {
                if (this.comma) {
                    this.writer.write(",[");
                }
                else {
                    this.writer.write('[');
                }
            }
            catch (IOException e) {
                throw new JSONException(e);
            }
            this.mode = 'a';
            this.comma = false;
            return this;
        }
        throw new JSONException("Misplaced array.");
    }

    /**
     * End something.
     * @param m Mode
     * @param c Closing character
     * @return this
     * @throws JSONException If unbalanced.
     */
    private JSONWriter end(char m, char c) throws JSONException {
        if (this.mode != m) {
            throw new JSONException(m == 'a' ? "Misplaced endArray." :
                    "Misplaced endObject.");
        }
        // Pop an array or object scope.
        if (this.top <= 0) {
            throw new JSONException("Nesting error.");
        }
        char x = this.stack[--this.top] == null ? 'a' : 'k';
        if (x != m) {
            throw new JSONException("Nesting error.");
        }
        this.mode = this.top == 0 ? 'd' : this.stack[this.top - 1] == null ? 'a' : 'k';
        this.comma = true;

        try {
            this.writer.write(c);
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        return this;
    }

    /**
     * End an array. This method most be called to balance calls to
     * <code>array</code>.
     * @return this
     * @throws JSONException If incorrectly nested.
     */
    public JSONWriter endArray() throws JSONException {
        return this.end('a', ']');
    }

    /**
     * End an object. This method most be called to balance calls to
     * <code>object</code>.
     * @return this
     * @throws JSONException If incorrectly nested.
     */
    public JSONWriter endObject() throws JSONException {
        return this.end('k', '}');
    }

    /**
     * Append a key. The key will be associated with the next value. In an
     * object, every value must be preceded by a key.
     * @param s A key string.
     * @return this
     * @throws JSONException If the key is out of place. For example, keys
     *  do not belong in arrays or if the key is null.
     */
    public JSONWriter key(String s) throws JSONException {
        if (s == null) {
            throw new JSONException("Null key.");
        }
        if (this.mode == 'k') {
            try {
                stack[top - 1].putOnce(s, Boolean.TRUE);
                if (this.comma) {
                    this.writer.write(',');
                }
                this.writer.write('"');
                this.writer.write(JSONObject.quotable(s));
                this.writer.write("\":");
                this.comma = false;
                this.mode = 'o';
                return this;
            } catch (IOException e) {
                throw new JSONException(e);
            }
        }
        throw new JSONException("Misplaced key.");
    }


    /**
     * Begin appending a new object. All keys and values until the balancing
     * <code>endObject</code> will be appended to this object. The
     * <code>endObject</code> method must be called to mark the object's end.
     * @return this
     * @throws JSONException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public JSONWriter object() throws JSONException {
        if (this.mode == 'i' || this.mode == 'o' || this.mode == 'a') {
            // push an object scope
            if (this.top >= MAXDEPTH) {
                throw new JSONException("Nesting too deep.");
            }
            this.stack[this.top++] = new JSONObject();
            try {
                if (this.comma) {
                    this.writer.write(",{");
                }
                else {
                    this.writer.write('{');
                }
            }
            catch (IOException e) {
                throw new JSONException(e);
            }
            this.mode = 'k';
            this.comma = false;
            return this;
        }
        throw new JSONException("Misplaced object.");

    }


    /**
     * Pop an array or object scope.
     * @param c The scope to close.
     * @throws JSONException If nesting is wrong.
     */
    private void pop(char c) throws JSONException {
        if (this.top <= 0) {
            throw new JSONException("Nesting error.");
        }
        char m = this.stack[this.top - 1] == null ? 'a' : 'k';
        if (m != c) {
            throw new JSONException("Nesting error.");
        }
        this.top -= 1;
        this.mode = this.top == 0 ? 'd' : this.stack[this.top - 1] == null ? 'a' : 'k';
    }

    /**
     * Push an array or object scope.
     * @param c The scope to open.
     * @throws JSONException If nesting is too deep.
     */
    private void push(JSONObject jo) throws JSONException {
        if (this.top >= MAXDEPTH) {
            throw new JSONException("Nesting too deep.");
        }
        this.stack[this.top] = jo;
        this.mode = jo == null ? 'a' : 'k';
        this.top += 1;
    }


    /**
     * Append either the value <code>true</code> or the value
     * <code>false</code>.
     * @param b A boolean.
     * @return this
     * @throws JSONException
     */
    public JSONWriter value(boolean b) throws JSONException {
        return this.append(b ? "true" : "false");
    }

    /**
     * Append a double value.
     * @param d A double.
     * @return this
     * @throws JSONException If the number is not finite.
     */
    public JSONWriter value(double d) throws JSONException {
        return this.value(new Double(d));
    }

    /**
     * Append a long value.
     * @param l A long.
     * @return this
     * @throws JSONException
     */
    public JSONWriter value(long l) throws JSONException {
        return this.append(Long.toString(l));
    }


    /**
     * Append an object value.
     * @param o The object to append. It can be null, or a Boolean, Number,
     *   String, JSONObject, or JSONArray, or an object with a toJSONString()
     *   method.
     * @return this
     * @throws JSONException If the value is out of sequence.
     */
    public JSONWriter value(Object o) throws JSONException {
        return this.append(JSONObject.valueToString(o));
    }

    /*
     * An optimized replacement of key and value calls for the common case of
     * a symbol key -- new to the current JSON Object, and with no special
     * characters, so it never needs escaping
     *  -- and a string value -- that may be null or contain escaped characters.
     * If the key is not new, this call simply overwrites the previous value
     * for the key without throwing the usual duplicate key error.
     * @param key a simple string with no escape characters
     * @param value a general string value that may HAVE escaped characters
     * @return this
     * @throws JSONException If the key is null or arrives out of sequence,
     *                       or there is an IOException.
     */
    private JSONWriter keySymbolValueEscapedStringPair(String key, String escapedValue) throws JSONException {
        if (key == null) {
            throw new JSONException("Null key.");
        }
        if (this.mode != 'k') {
            throw new JSONException("Misplaced key.");
        }
        try {
            stack[top - 1].put(key, true);
            if (this.comma) {
                this.writer.write(',');
            }
            this.writer.write('"');
            this.writer.write(key);
            this.writer.write("\":\"");
            this.writer.write(escapedValue);
            this.writer.write('"');
            this.comma = true;
            return this;
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
    }

    /*
     * An optimized replacement of key and value calls for the common case of
     * a symbol key -- new to the current JSON Object, and with no special
     * characters, so it never needs escaping
     *  -- and a string value -- that may be null or need escaping.
     * If the key is not new, this call simply overwrites the previous value
     * for the key without throwing the usual duplicate key error.
     * @param key a simple string with no escape characters
     * @param value a general non-null string value that may need escape characters
     * @return this
     * @throws JSONException If the key is null or arrives out of sequence,
     *                       or there is an IOException.
     */
    public JSONWriter keySymbolValuePair(String key, String value) throws JSONException {
        return keySymbolValueEscapedStringPair(key, JSONObject.quotable(value));
    }

    /*
     * An optimized replacement of key and value calls for the common case of
     * a symbol key -- new to the current JSON Object, and with no special
     * characters, so it never needs escaping
     *  -- and a string value -- that may be null or need escaping.
     * If the key is not new, this call simply overwrites the previous value
     * for the key without throwing the usual duplicate key error.
     * @param key a simple string with no escape characters
     * @param value a general string value that may need escape characters
     * @return this
     * @throws JSONException If the key is null or arrives out of sequence,
     *                       or there is an IOException.
     */
    public JSONWriter keySymbolValuePair(String key, long value) throws JSONException {
        return keySymbolValueEscapedStringPair(key, Long.toString(value));
    }

    public JSONWriter keySymbolValuePair(String key, boolean value) throws JSONException {
        return keySymbolValueEscapedStringPair(key, Boolean.toString(value));
    }

}
