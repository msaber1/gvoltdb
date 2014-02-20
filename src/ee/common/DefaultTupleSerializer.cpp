/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
#include "common/DefaultTupleSerializer.h"
#include "common/serializeio.h"
#include "common/TupleSchema.h"

namespace voltdb {
/**
 * Serialize the provided tuple to the provide serialize output
 */
void DefaultTupleSerializer::serializeTo(TableTuple tuple, ReferenceSerializeOutput *out) {
    tuple.serializeTo(*out);
}

/**
 * Calculate the maximum size of a serialized tuple based upon the schema of the table/tuple
 */
int DefaultTupleSerializer::getMaxSerializedTupleSize(const TupleSchema *schema) {
    size_t size = 4;
    size += static_cast<size_t>(schema->tupleLength());
    for (int ii = 0; ii < schema->columnCount(); ii++) {
        if (isObjectType(schema->columnType(ii))) {
            if (schema->columnIsInlined(ii)) {
                size += 3;//Serialization always uses a 4-byte length prefix in place of the inlined 1.
                continue;
            }
            // the out-of-line length and value do get serialized.
            size += 4 + schema->columnDeclaredLength(ii) *
                (schema->columnDeclaredUnitIsBytes(ii) ? 1 : MAX_UTF8_BYTES_PER_CHARACTER);
            // the StringRef pointer does not get serialized
            size -= sizeof(void*);
        }
    }
    return static_cast<int>(size);
}

}

