/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package db.procedures;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class SampleRecord
{
    public final int event_instance_id;
    public final Object event_type_id;
    public final Object event_date;
    public final Object partner_id;
    public final Object consumer_id;
    public final Object address;
    public final Object sub_domain_1;
    public final Object sub_domain_2;
    public final Object sub_domain_3;
    public final Object event_quantity;
    public final Object event_value;
    public final Object transaction_code;
    public final Object in_message_id;
    public final Object out_message_id;
    public final Object event_tracking_id;
    public final Object tracking_rule_id;

    public SampleRecord(int event_instance_id, Random rand)
    {
        this.event_instance_id          = event_instance_id;
        this.event_type_id              = nextInteger(rand);
        this.event_date                 = nextTimestamp(rand, true);
        this.partner_id                 = nextInteger(rand);
        this.consumer_id                = nextBigint(rand);
        this.address                    = nextVarchar(rand, 1, 25);
        this.sub_domain_1               = nextVarchar(rand, 1, 25);
        this.sub_domain_2               = nextVarchar(rand, 1, 25);
        this.sub_domain_3               = nextVarchar(rand, 1, 25);
        this.event_quantity             = nextInteger(rand);
        this.event_value                = nextFloat(rand);
        this.transaction_code           = nextVarchar(rand, true, 1, 25);
        this.in_message_id              = nextBigint(rand);
        this.out_message_id             = nextBigint(rand);
        this.event_tracking_id          = nextBigint(rand);
        this.tracking_rule_id           = nextInteger(rand);
    }

    private static Object nextTinyint(Random rand)
    {
        return nextTinyint(rand, false);
    }
    private static Object nextTinyint(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        byte result;
        do { result = (new Integer(rand.nextInt())).byteValue(); } while(result == VoltType.NULL_TINYINT);
        return result;
    }

    private static Object nextSmallint(Random rand)
    {
        return nextSmallint(rand, false);
    }
    private static Object nextSmallint(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        short result;
        do { result = (new Integer(rand.nextInt())).shortValue(); } while(result == VoltType.NULL_SMALLINT);
        return result;
    }

    private static Object nextInteger(Random rand)
    {
        return nextInteger(rand, false);
    }
    private static Object nextInteger(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        int result;
        do { result = rand.nextInt(); } while(result == VoltType.NULL_INTEGER);
        return result;
    }

    private static Object nextBigint(Random rand)
    {
        return nextBigint(rand, false);
    }
    private static Object nextBigint(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        long result;
        do { result = rand.nextLong(); } while(result == VoltType.NULL_BIGINT);
        return result;
    }

    private static Object nextTimestamp(Random rand)
    {
        return nextTimestamp(rand, false);
    }
    private static Object nextTimestamp(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        return new TimestampType(Math.abs(rand.nextInt())*1000l);
    }

    private static Object nextFloat(Random rand)
    {
        return nextFloat(rand, false);
    }
    private static Object nextFloat(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        double result; // Inconsistent naming (!)  Underlying database type is Double
        do { result = rand.nextDouble(); } while(result == VoltType.NULL_FLOAT);
        return result;
    }

    private static Object nextDecimal(Random rand)
    {
        return nextDecimal(rand, false);
    }
    private static Object nextDecimal(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        return (new BigDecimal(rand.nextDouble()*rand.nextLong())).setScale(12, RoundingMode.HALF_EVEN);
    }

    private static Object nextVarchar(Random rand, int minLength, int maxLength)
    {
        return nextVarchar(rand, false, minLength, maxLength);
    }
    private static Object nextVarchar(Random rand, boolean isNullable, int minLength, int maxLength)
    {
        if (isNullable && rand.nextBoolean()) return null;
        int length = (maxLength==minLength)?maxLength:rand.nextInt(maxLength-minLength)+minLength;
        StringBuilder result = new StringBuilder(length);
        while(result.length() < length)
            result.append(Long.toBinaryString(rand.nextLong()));
        return result.toString().substring(0,Math.min(result.length(), length)-1);
    }
}
