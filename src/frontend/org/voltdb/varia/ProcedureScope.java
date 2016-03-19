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

package org.voltdb.varia;

import static com.google_voltpatches.common.base.Preconditions.checkState;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.inject.Key;
import com.google.inject.OutOfScopeException;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

public class ProcedureScope implements Scope {

    private static final Provider<Object> SEEDED_KEY_PROVIDER =
        new Provider<Object>() {
          @Override
        public Object get() {
            throw new IllegalStateException("If you got here then it means that" +
                " your code asked for scoped object which should have been" +
                " explicitly seeded in this scope by calling" +
                " ProcedureScope.seed(), but was not.");
          }
        };
    private final ThreadLocal<Map<Key<?>, Object>> values
        = new ThreadLocal<Map<Key<?>, Object>>();

    public void enter() {
      checkState(values.get() == null, "A scoping block is already in progress");
      values.set(Maps.<Key<?>, Object>newHashMap());
    }

    public void exit() {
      checkState(values.get() != null, "No scoping block in progress");
      values.remove();
    }

    public <T> void seed(Key<T> key, T value) {
      Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);
      checkState(!scopedObjects.containsKey(key), "A value for the key %s was " +
          "already seeded in this scope. Old value: %s New value: %s", key,
          scopedObjects.get(key), value);
      scopedObjects.put(key, value);
    }

    public <T> void seed(Class<T> clazz, T value) {
      seed(Key.get(clazz), value);
    }

    public <T> void seedVaria(TypeLiteral<T> tl, T value) {
        seed(Key.get(tl, Varia.class), value);
    }

    @Override
    public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
      return new Provider<T>() {
        @Override
        public T get() {
          Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);

          @SuppressWarnings("unchecked")
          T current = (T) scopedObjects.get(key);
          if (current == null && !scopedObjects.containsKey(key)) {
            current = unscoped.get();

            // don't remember proxies; these exist only to serve circular dependencies
            if (Scopes.isCircularProxy(current)) {
              return current;
            }

            scopedObjects.put(key, current);
          }
          return current;
        }
      };
    }

    private <T> Map<Key<?>, Object> getScopedObjectMap(Key<T> key) {
      Map<Key<?>, Object> scopedObjects = values.get();
      if (scopedObjects == null) {
        throw new OutOfScopeException("Cannot access " + key
            + " outside of a scoping block");
      }
      return scopedObjects;
    }

    /**
     * Returns a provider that always throws exception complaining that the object
     * in question must be seeded before it can be injected.
     *
     * @return typed provider
     */
    @SuppressWarnings({"unchecked"})
    public static <T> Provider<T> seededKeyProvider() {
      return (Provider<T>) SEEDED_KEY_PROVIDER;
    }
  }
