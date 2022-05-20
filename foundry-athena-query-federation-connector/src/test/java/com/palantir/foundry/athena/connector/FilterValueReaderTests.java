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

import static com.palantir.conjure.java.api.testing.Assertions.assertThatServiceExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.foundry.athena.api.FilterValue;
import com.palantir.foundry.athena.api.FoundryAthenaErrors;
import com.palantir.logsafe.SafeArg;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class FilterValueReaderTests {

    @Mock
    private FieldReader fieldReader;

    private FilterValueReader filterValueReader;

    @BeforeEach
    void before() {
        filterValueReader = new FilterValueReader(fieldReader);

        // lambdas always run in UTC, and the SDK's DateTimeFormatterUtil only works if the system timezone is UTC
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Test
    void visit_Int_16() {
        Int type = new Int(16, true);
        short value = 1;
        when(fieldReader.readShort()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.numberFilter(value));
    }

    @Test
    void visit_Int_32() {
        Int type = new Int(32, true);
        int value = 1;
        when(fieldReader.readInteger()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.numberFilter(value));
    }

    @Test
    void visit_Int_64() {
        Int type = new Int(64, true);
        long value = 1;
        when(fieldReader.readLong()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.numberFilter((double) value));
    }

    @Test
    void visit_Int_withInvalidBitWidth_throwsServiceException() {
        Int type = new Int(7, true);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Int"), SafeArg.of("precision", Optional.of(7)));
    }

    @Test
    void visit_FloatingPoint_Single() {
        FloatingPoint type = new FloatingPoint(FloatingPointPrecision.SINGLE);
        float value = 1.1f;
        when(fieldReader.readFloat()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.numberFilter(value));
    }

    @Test
    void visit_FloatingPoint_Double() {
        FloatingPoint type = new FloatingPoint(FloatingPointPrecision.DOUBLE);
        double value = 1.1;
        when(fieldReader.readDouble()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.numberFilter(value));
    }

    @Test
    void visit_FloatingPoint_Half_throwsServiceException() {
        FloatingPoint type = new FloatingPoint(FloatingPointPrecision.HALF);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(
                        SafeArg.of("unsupportedType", "FloatingPoint"),
                        SafeArg.of("precision", Optional.of(FloatingPointPrecision.HALF)));
    }

    @Test
    void visit_Utf8() {
        Utf8 type = mock(Utf8.class);
        String value = "string";
        when(fieldReader.readText()).thenReturn(new Text(value));
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.stringFilter(value));
    }

    @Test
    void visit_Bool() {
        Bool type = mock(Bool.class);
        boolean value = true;
        when(fieldReader.readBoolean()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.booleanFilter(value));
    }

    @Test
    void visit_Date() {
        Date type = mock(Date.class);
        int value = 20;
        when(fieldReader.readInteger()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.numberFilter(value));
    }

    @Test
    void visit_Timestamp() {
        Timestamp type = mock(Timestamp.class);
        long value =
                DateTimeFormatterUtil.packDateTimeWithZone(ZonedDateTime.of(1970, 1, 1, 0, 0, 20, 0, ZoneOffset.UTC));
        when(fieldReader.readLong()).thenReturn(value);
        assertThat(filterValueReader.visit(type)).isEqualTo(FilterValue.dateTimeFilter(SafeLong.of(20000000L)));
    }

    @Test
    void visit_Binary_throwsServiceException() {
        Binary type = mock(Binary.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Binary"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_FixedSizeBinary_throwsServiceException() {
        FixedSizeBinary type = mock(FixedSizeBinary.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "FixedSizeBinary"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_Null_throwsServiceException() {
        Null type = mock(Null.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Null"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_Struct_throwsServiceException() {
        Struct type = mock(Struct.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Struct"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_List_throwsServiceException() {
        List type = mock(List.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "List"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_FixedSizeList_throwsServiceException() {
        FixedSizeList type = mock(FixedSizeList.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "FixedSizeList"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_Union_throwsServiceException() {
        Union type = mock(Union.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Union"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_Time_throwsServiceException() {
        Time type = mock(Time.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Time"), SafeArg.of("precision", Optional.empty()));
    }

    @Test
    void visit_Interval_throwsServiceException() {
        Interval type = mock(Interval.class);
        assertThatServiceExceptionThrownBy(() -> filterValueReader.visit(type))
                .hasType(FoundryAthenaErrors.UNSUPPORTED_PARTITION_TYPE)
                .hasArgs(SafeArg.of("unsupportedType", "Interval"), SafeArg.of("precision", Optional.empty()));
    }
}
