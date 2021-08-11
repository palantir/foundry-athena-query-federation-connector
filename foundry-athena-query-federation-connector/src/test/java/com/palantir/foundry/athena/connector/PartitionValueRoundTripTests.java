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

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.foundry.athena.api.DateDay;
import com.palantir.foundry.athena.api.FilterValue;
import com.palantir.foundry.athena.api.PartitionValue;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

final class PartitionValueRoundTripTests {

    private static final String FIELD_NAME = "field-name";

    @BeforeEach
    public void before() {
        // lambdas always run in UTC, and the SDK's DateTimeFormatterUtil only works if the system timezone is UTC
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @ParameterizedTest
    @MethodSource
    void partitionValueRoundTrip(ArrowType arrowType, PartitionValue partitionValue, FilterValue filterValue) {
        BlockSpiller blockSpiller = new InMemorySingleBlockSpiller(
                new Schema(ImmutableList.of(new Field(FIELD_NAME, FieldType.nullable(arrowType), ImmutableList.of()))),
                ConstraintEvaluator.emptyEvaluator());

        blockSpiller.writeRows((block, _rowIdx) -> {
            partitionValue.accept(new PartitionValueWriter(block, FIELD_NAME, 0));
            return 1;
        });

        FieldReader fieldReader = blockSpiller.getBlock().getFieldReader(FIELD_NAME);
        assertThat(arrowType.accept(new FilterValueReader(fieldReader))).isEqualTo(filterValue);

        blockSpiller.close();
    }

    @SuppressWarnings("UnusedMethod")
    private static Stream<Arguments> partitionValueRoundTrip() {
        return Stream.of(
                Arguments.of(ArrowType.Bool.INSTANCE, PartitionValue.boolean_(true), FilterValue.booleanFilter(true)),
                Arguments.of(MinorType.TINYINT.getType(), PartitionValue.byte_(1), FilterValue.numberFilter((byte) 1)),
                Arguments.of(
                        MinorType.SMALLINT.getType(), PartitionValue.short_(1), FilterValue.numberFilter((short) 1)),
                Arguments.of(MinorType.INT.getType(), PartitionValue.integer(1), FilterValue.numberFilter((int) 1)),
                Arguments.of(
                        MinorType.BIGINT.getType(), PartitionValue.long_(SafeLong.of(1)), FilterValue.numberFilter(1L)),
                Arguments.of(MinorType.FLOAT4.getType(), PartitionValue.float_(1.1f), FilterValue.numberFilter(1.1f)),
                Arguments.of(MinorType.FLOAT8.getType(), PartitionValue.double_(1.1), FilterValue.numberFilter(1.1)),
                Arguments.of(
                        new ArrowType.Decimal(3, 2),
                        PartitionValue.decimal(new BigDecimal("1.10")),
                        FilterValue.stringFilter("1.10")),
                Arguments.of(
                        MinorType.DATEDAY.getType(),
                        PartitionValue.date(DateDay.of(SafeLong.of(10))),
                        FilterValue.numberFilter(10L)),
                Arguments.of(
                        ArrowType.Utf8.INSTANCE, PartitionValue.string("string"), FilterValue.stringFilter("string")),
                Arguments.of(
                        new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.getId()),
                        PartitionValue.datetime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(10), ZoneOffset.UTC)),
                        FilterValue.dateTimeFilter(SafeLong.of(10000L))),
                Arguments.of(
                        new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.getId()),
                        PartitionValue.datetime(
                                OffsetDateTime.ofInstant(Instant.ofEpochMilli(10), ZoneId.of("+05:00"))),
                        FilterValue.dateTimeFilter(SafeLong.of(10000L))));
    }
}
