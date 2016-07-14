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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum UDFType {
    INVALID     (0), // For Parsing...
    SCALAR      (1),
    AGGREGATE   (2);

    UDFType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, UDFType> idx_lookup = new HashMap<Integer, UDFType>();
    protected static final Map<String, UDFType> name_lookup = new HashMap<String, UDFType>();
    static {
        for (UDFType vt : EnumSet.allOf(UDFType.class)) {
            UDFType.idx_lookup.put(vt.ordinal(), vt);
            UDFType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, UDFType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, UDFType> getNameMap() {
        return name_lookup;
    }

    public static UDFType get(Integer idx) {
        UDFType ret = UDFType.idx_lookup.get(idx);
        return (ret == null ? UDFType.INVALID : ret);
    }

    public static UDFType get(String name) {
        UDFType ret = UDFType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? UDFType.INVALID : ret);
    }
}
