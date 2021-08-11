/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.foundry.athena.connector;

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.foundry.athena.api.FilterValue;
import com.palantir.foundry.athena.api.FoundryAthenaErrors;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeList;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

@SuppressWarnings("JdkObsolete")
final class FilterValueConverter implements ArrowTypeVisitor<FilterValue> {

    private final Object value;

    FilterValueConverter(Object value) {
        this.value = value;
    }

    @Override
    public FilterValue visit(Int type) {
        switch (type.getBitWidth()) {
            case 8:
                return FilterValue.numberFilter((Byte) value);
            case 16:
                return FilterValue.numberFilter((Short) value);
            case 32:
                return FilterValue.numberFilter((Integer) value);
            case 64:
                return FilterValue.numberFilter((Long) value);
        }
        throw FoundryAthenaErrors.unsupportedPartitionType("Int", Optional.of(type.getBitWidth()));
    }

    @Override
    public FilterValue visit(FloatingPoint type) {
        switch (type.getPrecision()) {
            case SINGLE:
                return FilterValue.numberFilter((Float) value);
            case DOUBLE:
                return FilterValue.numberFilter((Double) value);
            default:
        }
        throw FoundryAthenaErrors.unsupportedPartitionType("FloatingPoint", Optional.of(type.getPrecision()));
    }

    @Override
    public FilterValue visit(Utf8 _type) {
        return FilterValue.stringFilter(value.toString());
    }

    @Override
    public FilterValue visit(LargeUtf8 _type) {
        return FilterValue.stringFilter(value.toString());
    }

    @Override
    public FilterValue visit(Bool _type) {
        return FilterValue.booleanFilter((Boolean) value);
    }

    @Override
    public FilterValue visit(Decimal type) {
        BigDecimal bigDecimal;
        if (value instanceof Double) {
            bigDecimal = BigDecimal.valueOf((double) value);
            bigDecimal = bigDecimal.setScale(type.getScale(), RoundingMode.HALF_UP);
        } else {
            bigDecimal = ((BigDecimal) value).setScale(type.getScale(), RoundingMode.HALF_UP);
        }
        return FilterValue.stringFilter(bigDecimal.toString());
    }

    @Override
    @SuppressWarnings("JavaUtilDate")
    public FilterValue visit(ArrowType.Date _type) {
        if (value instanceof Date) {
            org.joda.time.Days days = org.joda.time.Days.daysBetween(
                    BlockUtils.EPOCH, new org.joda.time.DateTime(((Date) value).getTime()));
            return FilterValue.numberFilter(days.getDays());
        } else if (value instanceof LocalDate) {
            int days = (int) ((LocalDate) value).toEpochDay();
            return FilterValue.numberFilter(days);
        } else if (value instanceof Long) {
            return FilterValue.numberFilter(((Long) value).intValue());
        } else {
            return FilterValue.numberFilter((int) value);
        }
    }

    @Override
    @SuppressWarnings("JavaUtilDate")
    public FilterValue visit(Timestamp _type) {
        if (value instanceof Long) {
            return FilterValue.dateTimeFilter(SafeLong.of((Long) value * 1000));
        } else {
            long dateTimeWithZone;
            if (value instanceof ZonedDateTime) {
                dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime) value);
            } else if (value instanceof LocalDateTime) {
                dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(
                        ((LocalDateTime) value)
                                .atZone(BlockUtils.UTC_ZONE_ID)
                                .toInstant()
                                .toEpochMilli(),
                        BlockUtils.UTC_ZONE_ID.getId());
            } else if (value instanceof Date) {
                long ldtInLong = Instant.ofEpochMilli(((Date) value).getTime())
                        .atZone(BlockUtils.UTC_ZONE_ID)
                        .toInstant()
                        .toEpochMilli();
                dateTimeWithZone =
                        DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, BlockUtils.UTC_ZONE_ID.getId());
            } else {
                dateTimeWithZone = (long) value;
            }
            // Athena only provides timestamps in milliseconds but we require microseconds
            return FilterValue.dateTimeFilter(SafeLong.of(dateTimeWithZone * 1000));
        }
    }

    @Override
    public FilterValue visit(Binary _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Binary", Optional.empty());
    }

    @Override
    public FilterValue visit(LargeBinary _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("LargeBinary", Optional.empty());
    }

    @Override
    public FilterValue visit(FixedSizeBinary _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("FixedSizeBinary", Optional.empty());
    }

    @Override
    public FilterValue visit(Null _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Null", Optional.empty());
    }

    @Override
    public FilterValue visit(Struct _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Struct", Optional.empty());
    }

    @Override
    public FilterValue visit(ArrowType.List _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("List", Optional.empty());
    }

    @Override
    public FilterValue visit(LargeList _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("LargeList", Optional.empty());
    }

    @Override
    public FilterValue visit(FixedSizeList _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("FixedSizeList", Optional.empty());
    }

    @Override
    public FilterValue visit(Union _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Union", Optional.empty());
    }

    @Override
    public FilterValue visit(Map _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Union", Optional.empty());
    }

    @Override
    public FilterValue visit(Time _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Time", Optional.empty());
    }

    @Override
    public FilterValue visit(Interval _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Interval", Optional.empty());
    }

    @Override
    public FilterValue visit(Duration _type) {
        throw FoundryAthenaErrors.unsupportedPartitionType("Duration", Optional.empty());
    }
}
