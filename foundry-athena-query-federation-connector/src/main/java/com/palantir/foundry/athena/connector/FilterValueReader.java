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

import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.foundry.athena.api.FilterValue;
import com.palantir.foundry.athena.api.FoundryAthenaErrors;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
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
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

final class FilterValueReader implements ArrowTypeVisitor<FilterValue> {

    private final FieldReader fieldReader;

    FilterValueReader(FieldReader fieldReader) {
        this.fieldReader = fieldReader;
    }

    @Override
    public FilterValue visit(Int type) {
        switch (type.getBitWidth()) {
            case 8:
                return FilterValue.numberFilter(fieldReader.readByte());
            case 16:
                return FilterValue.numberFilter(fieldReader.readShort());
            case 32:
                return FilterValue.numberFilter(fieldReader.readInteger());
            case 64:
                return FilterValue.numberFilter(fieldReader.readLong());
        }
        throw FoundryAthenaErrors.unsupportedPartitionType("Int", Optional.of(type.getBitWidth()));
    }

    @Override
    public FilterValue visit(FloatingPoint type) {
        switch (type.getPrecision()) {
            case SINGLE:
                return FilterValue.numberFilter(fieldReader.readFloat());
            case DOUBLE:
                return FilterValue.numberFilter(fieldReader.readDouble());
            default:
        }
        throw FoundryAthenaErrors.unsupportedPartitionType("FloatingPoint", Optional.of(type.getPrecision()));
    }

    @Override
    public FilterValue visit(Utf8 _type) {
        return FilterValue.stringFilter(fieldReader.readText().toString());
    }

    @Override
    public FilterValue visit(LargeUtf8 _type) {
        return FilterValue.stringFilter(fieldReader.readText().toString());
    }

    @Override
    public FilterValue visit(Bool _type) {
        return FilterValue.booleanFilter(fieldReader.readBoolean());
    }

    @Override
    public FilterValue visit(Decimal _type) {
        return FilterValue.stringFilter(fieldReader.readBigDecimal().toString());
    }

    @Override
    public FilterValue visit(Date _type) {
        return FilterValue.numberFilter(fieldReader.readInteger().longValue());
    }

    @Override
    public FilterValue visit(Timestamp _type) {
        ZonedDateTime unpackedDateTime = DateTimeFormatterUtil.constructZonedDateTime(fieldReader.readLong());
        return FilterValue.dateTimeFilter(
                SafeLong.of(Instant.EPOCH.until(unpackedDateTime.toOffsetDateTime(), ChronoUnit.MICROS)));
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
    public FilterValue visit(List _type) {
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
        throw FoundryAthenaErrors.unsupportedPartitionType("Map", Optional.empty());
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
