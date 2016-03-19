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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.voltcore.utils.ByteBufferInputStream;
import org.voltcore.utils.CappedByteBufferOutputStream;

import com.google.inject.TypeLiteral;
import com.google_voltpatches.common.base.Supplier;

public class AvroPadder<T> implements Padder<T> {
    final SpecificDatumWriter<T> dw;
    final SpecificDatumReader<T> dr;
    final TypeLiteral<T> tl;
    T datum;
    long crc;
    int version;

    public AvroPadder(Class<T> z) {
        this.datum = null;
        this.crc = 0L;
        this.version = 0;
        this.dr = new SpecificDatumReader<T>(z);
        this.dw = new SpecificDatumWriter<T>(z);
        this.tl = TypeLiteral.get(z);
    }

    @Override
    public T itch(ByteBuffer bb, Supplier<T> initialValue) throws PadderException {
        if (bb.limit() < 4) {
            datum = initialValue.get();
            return datum;
        }
        int padversion = bb.getInt();
        if (padversion == version) {
            bb.position(0);
            return datum;
        }
        if (bb.limit() == bb.position()) {
            datum = null;
            crc = 0L;
            return datum;
        }
        Decoder dcdr = new DecoderFactory().binaryDecoder(
            new ByteBufferInputStream(bb), null);

        T decoded;
        try {
            decoded = dr.read(null, dcdr);
        } catch (IOException e) {
            throw new PadderException("failed to deserialize scratch pad content", e);
        }
        datum = decoded;

        version = padversion;
        bb.flip().position(4);
        CRC32 crcmkr = new CRC32();
        crcmkr.update(bb);
        crc = crcmkr.getValue();
        bb.flip();
        return datum;
    }

    @Override
    public boolean scratch(T newdatum, ByteBuffer bb) throws PadderException {
        bb.clear();
        if (newdatum == null) {
            if (datum != null) {
                crc = 0L;
                datum = null;
                version = version + 1;
                bb.putInt(version);
                bb.flip();
                return true;
            } else {
                bb.putInt(version);
                bb.flip();
                return false;
            }
        }
        bb.position(4);
        Encoder ecdr = new EncoderFactory().binaryEncoder(
            new CappedByteBufferOutputStream(bb), null);
        try {
            dw.write(newdatum, ecdr);
            ecdr.flush();
        } catch (IOException e) {
            throw new PadderException("failed to serialize scratch pad content", e);
        }
        bb.flip().position(4);
        CRC32 crcmkr = new CRC32();
        crcmkr.update(bb);
        long newcrc = crcmkr.getValue();
        bb.flip();
        if (newcrc == crc) {
            bb.putInt(version);
            bb.position(0);
            return false;
        }
        version = version + 1;
        bb.putInt(version);
        bb.position(0);
        datum = newdatum;
        crc = newcrc;
        return true;
    }

    @Override
    public TypeLiteral<T> getScratchPadType() {
        return tl;
    }
}
