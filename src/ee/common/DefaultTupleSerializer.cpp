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
        if ( ! isObjectType(schema->columnType(ii))) {
            // Tuple bytes for fixed length values are serialized one for one
            continue;
        }
        if (schema->columnIsInlined(ii)) {
            // Serialization always uses a 4-byte length prefix
            // in the place of the inlined object's 1 byte prefix.
            size += 3;
        } else {
            // The StringRef pointer adds to tuple size but not serialized size.
            size -= sizeof(void*);
            // Serialization adds a 4 byte length and value bytes up to the maximum
            // required to store a value of the declared byte or character count.
            size += 4 + (schema->columnDeclaredLength(ii) *
                    (schema->columnDeclaredUnitIsBytes(ii) ? 1 : MAX_UTF8_BYTES_PER_CHARACTER));
        }
    }
    return static_cast<int>(size);
}
}

