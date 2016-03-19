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

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google_voltpatches.common.base.Supplier;

public class ProcedureScopeModule extends AbstractModule {

    ProcedureScope scope;
    final TypeLiteral<?> tl;

    public ProcedureScopeModule(TypeLiteral<?> ttl) {
        this.tl = ttl;
    }

    @Override
    protected void configure() {
        scope = new ProcedureScope();
        bindScope(ProcedureScoped.class, scope);

        bind(ProcedureScope.class)
            .annotatedWith(Names.named("procedureScope"))
            .toInstance(scope);

        bindVariaProvider(tl);
    }

    private <T> void bindVariaProvider(TypeLiteral<T> tl) {
        Key<T> key = Key.get(tl, Varia.class);
        Provider<T> variaProvider = scope.scope(key, null);
        bind(key).toProvider(variaProvider).in(ProcedureScoped.class);
    }

    @Provides @Named("variaType")
    Supplier<TypeLiteral<?>> getVariaTypeLiteral() {
        return new Supplier<TypeLiteral<?>>() {
            @Override
            public TypeLiteral<?> get() {
                return tl;
            }
        };
    }

    @Provides
    Padder<?> getAvroPadder() {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Padder<?> padder = new AvroPadder(tl.getRawType());
        return padder;
    }
}
