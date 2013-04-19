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

#ifndef _EE_COMMON_MAPUTILS_H_
#define _EE_COMMON_MAPUTILS_H_

#include <map>
#include <boost/unordered_map.hpp>

namespace voltdb {

// A collection of static wrappers around common std and boost map functions that provide
// terse calls to verbose idioms.

class MapUtils {
public:
    // Return the value found in the map at the specified key, otherwise the specified default value.
    // This is analogous to python's dict.get with 2 arguments.
    template<typename K, typename V, typename M>
    static inline V getValueAtKeyOrDefault(K const &key, M const &the_map, const V& default_value)
    {
        typename M::const_iterator lookup = the_map.find(key);
        if (lookup == the_map.end()) {
            return default_value;
        }
        return lookup->second;
    }

private:
    // Return the value found in the map at the specified key, otherwise a 0-initialized null value.
    // This is analogous to java's Map.get or python's dict.get with 1 argument.
    template<typename K, typename V, typename M>
    static inline V getValueAtKeyOrNullExplicit(K const &key, M const &the_map)
    {
        /* Works for any value type castable from 0/NULL */
        return getValueAtKeyOrDefault<K, V, M>(key, the_map, (V)NULL);
    }

    // Return the value found in the map at the specified key, otherwise a default initialized null value.
    // This is analogous to java's Map.get or python's dict.get with 1 argument.
    template<typename K, typename V, typename M>
    static inline V getSmartPtrAtKeyOrNullExplicit(K const &key, M const &the_map)
    {
        /* Works for any smart pointer that default initializes to NULL */
        V smartnull;
        return getValueAtKeyOrDefault<K, V, M>(key, the_map, smartnull);
    }

public:
// This macro allows IMPLICIT parameterization by BOTH the map template AND its value type at the same time by
// providing identical overloads for different MAP TEMPLATES (like std::map and boost::unordered_map).
// The problem is that C++ will only infer a parameter corresponding to a declared argument that has a
// parameter type, so "template <typename T> T function(T& the_collection);"
// applied to "std::vector<int> x;" becomes "std::vector<int> function< std::vector<int> >(x);"
// OR it will infer a parameter corresponding to a parameter of an argument that has an explicitly declared
// parameter type, so "template <typename T> T function(std::vector<T> & the_collection);"
// applied to "std::vector<int> x;" becomes "int function<int>(x);"
// but it does not (easily?) do both tricks at once so that
// "template <typename T, typename V> T function(V& the_collection);"
// applied to "std::vector<int> x;" becomes "int function< int, vector<int> >(x);".
// There may be a more elegant way to get this functionality -- possibly only in C++11?
// but use of this macro manages to work around the problem.
#define ENABLE_MAPUTILS_FUNCTIONS_SUPPORT_FOR_MAP_TEMPLATE( MAPTEMPLATE )                  \
                                                                                           \
    template<typename K, typename V>                                                       \
    static inline V getValueAtKeyOrNull(K const &key, MAPTEMPLATE<K, V> const &the_map)    \
    { return getValueAtKeyOrNullExplicit<K, V, MAPTEMPLATE<K, V> >(key, the_map); }        \
                                                                                           \
    template<typename K, typename V>                                                       \
    static inline V getSmartPtrAtKeyOrNull(K const &key, MAPTEMPLATE<K, V> const &the_map) \
    { return getSmartPtrAtKeyOrNullExplicit<K, V, MAPTEMPLATE<K, V> >(key, the_map); }


// These are essentially instantiations of overloads that lock in the map type so that it AND
// its value type (for which there is no separate argument) can be implicitly instantiated.
ENABLE_MAPUTILS_FUNCTIONS_SUPPORT_FOR_MAP_TEMPLATE(std::map);
ENABLE_MAPUTILS_FUNCTIONS_SUPPORT_FOR_MAP_TEMPLATE(boost::unordered_map);

};

}  // namespace voltdb

#endif /* _EE_COMMON_MAPUTILS_H_ */
