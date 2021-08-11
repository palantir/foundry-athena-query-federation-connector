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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import com.palantir.foundry.athena.api.AndFilter;
import com.palantir.foundry.athena.api.FalseFilter;
import com.palantir.foundry.athena.api.Filter;
import com.palantir.foundry.athena.api.FilterType;
import com.palantir.foundry.athena.api.FilterValue;
import com.palantir.foundry.athena.api.NotFilter;
import com.palantir.foundry.athena.api.NullFilter;
import com.palantir.foundry.athena.api.OrFilter;
import com.palantir.foundry.athena.api.TrueFilter;
import com.palantir.foundry.athena.api.ValueFilter;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("UnusedMethod")
final class ConstraintConverterTests {

    private static final BlockAllocator ALLOCATOR = new BlockAllocatorImpl();
    private static final String COLUMN = "col1";
    private static final ArrowType TYPE = MinorType.INT.getType();
    private static final Schema SCHEMA = new Schema(ImmutableList.of(Field.nullable(COLUMN, TYPE)));
    private static Block values;
    private static Block empty;

    @BeforeAll
    static void beforeAll() {
        values = ALLOCATOR.createBlock(SCHEMA);
        values.setRowCount(3);
        values.setValue(COLUMN, 0, 1);
        values.setValue(COLUMN, 1, 2);
        values.setValue(COLUMN, 2, 3);
        empty = ALLOCATOR.createBlock(SCHEMA);
        empty.setRowCount(0);
    }

    @AfterAll
    static void afterAll() {
        ALLOCATOR.close();
    }

    @ParameterizedTest
    @MethodSource
    void testConvert(ValueSet valueSet, Filter expected) {
        Filter actual = ConstraintConverter.convert(COLUMN, valueSet);
        assertThat(actual).isEqualTo(expected);
    }

    private static Stream<Arguments> testConvert() {
        return Stream.of(
                Arguments.of(new AllOrNoneValueSet(MinorType.INT.getType(), true, true), true_()),
                Arguments.of(new AllOrNoneValueSet(MinorType.INT.getType(), true, false), not(null_())),
                Arguments.of(new AllOrNoneValueSet(MinorType.INT.getType(), false, true), null_()),
                Arguments.of(new AllOrNoneValueSet(MinorType.INT.getType(), false, false), false_()),
                Arguments.of(
                        new EquatableValueSet(values, true, true), or(equalTo(1), equalTo(2), equalTo(3), null_())),
                Arguments.of(new EquatableValueSet(values, true, false), or(equalTo(1), equalTo(2), equalTo(3))),
                Arguments.of(
                        new EquatableValueSet(values, false, true),
                        or(and(not(equalTo(1)), not(equalTo(2)), not(equalTo(3))), null_())),
                Arguments.of(
                        new EquatableValueSet(values, false, false),
                        and(not(equalTo(1)), not(equalTo(2)), not(equalTo(3)))),
                Arguments.of(new EquatableValueSet(empty, true, true), null_()),
                Arguments.of(new EquatableValueSet(empty, true, false), false_()),
                Arguments.of(new EquatableValueSet(empty, false, true), true_()),
                Arguments.of(new EquatableValueSet(empty, false, false), not(null_())),
                Arguments.of(SortedRangeSet.of(true, Range.all(ALLOCATOR, TYPE)), true_()),
                Arguments.of(SortedRangeSet.of(false, Range.all(ALLOCATOR, TYPE)), not(null_())),
                Arguments.of(
                        SortedRangeSet.of(true, Range.greaterThan(ALLOCATOR, TYPE, 1)), or(greaterThan(1), null_())),
                Arguments.of(SortedRangeSet.of(false, Range.greaterThan(ALLOCATOR, TYPE, 1)), or(greaterThan(1))),
                Arguments.of(
                        SortedRangeSet.of(true, Range.greaterThanOrEqual(ALLOCATOR, TYPE, 1)),
                        or(greaterThanOrEqual(1), null_())),
                Arguments.of(
                        SortedRangeSet.of(false, Range.greaterThanOrEqual(ALLOCATOR, TYPE, 1)),
                        or(greaterThanOrEqual(1))),
                Arguments.of(SortedRangeSet.of(true, Range.lessThan(ALLOCATOR, TYPE, 1)), or(lessThan(1), null_())),
                Arguments.of(SortedRangeSet.of(false, Range.lessThan(ALLOCATOR, TYPE, 1)), or(lessThan(1))),
                Arguments.of(
                        SortedRangeSet.of(true, Range.lessThanOrEqual(ALLOCATOR, TYPE, 1)),
                        or(lessThanOrEqual(1), null_())),
                Arguments.of(
                        SortedRangeSet.of(false, Range.lessThanOrEqual(ALLOCATOR, TYPE, 1)), or(lessThanOrEqual(1))),
                Arguments.of(
                        SortedRangeSet.of(true, Range.range(ALLOCATOR, TYPE, 1, false, 3, false)),
                        or(and(greaterThan(1), lessThan(3)), null_())),
                Arguments.of(
                        SortedRangeSet.of(true, Range.range(ALLOCATOR, TYPE, 1, true, 3, false)),
                        or(and(greaterThanOrEqual(1), lessThan(3)), null_())),
                Arguments.of(
                        SortedRangeSet.of(true, Range.range(ALLOCATOR, TYPE, 1, false, 3, true)),
                        or(and(greaterThan(1), lessThanOrEqual(3)), null_())),
                Arguments.of(
                        SortedRangeSet.of(true, Range.range(ALLOCATOR, TYPE, 1, true, 3, true)),
                        or(and(greaterThanOrEqual(1), lessThanOrEqual(3)), null_())),
                Arguments.of(
                        SortedRangeSet.of(false, Range.range(ALLOCATOR, TYPE, 1, false, 3, false)),
                        or(and(greaterThan(1), lessThan(3)))),
                Arguments.of(
                        SortedRangeSet.of(false, Range.range(ALLOCATOR, TYPE, 1, true, 3, false)),
                        or(and(greaterThanOrEqual(1), lessThan(3)))),
                Arguments.of(
                        SortedRangeSet.of(false, Range.range(ALLOCATOR, TYPE, 1, false, 3, true)),
                        or(and(greaterThan(1), lessThanOrEqual(3)))),
                Arguments.of(
                        SortedRangeSet.of(false, Range.range(ALLOCATOR, TYPE, 1, true, 3, true)),
                        or(and(greaterThanOrEqual(1), lessThanOrEqual(3)))),
                Arguments.of(
                        SortedRangeSet.of(
                                false, Range.greaterThan(ALLOCATOR, TYPE, 3), Range.equal(ALLOCATOR, TYPE, 1)),
                        or(equalTo(1), greaterThan(3))),
                Arguments.of(SortedRangeSet.of(false, Range.equal(ALLOCATOR, TYPE, 1)), equalTo(1)),
                Arguments.of(
                        SortedRangeSet.of(
                                true,
                                Range.range(ALLOCATOR, TYPE, 1, false, 3, true),
                                Range.range(ALLOCATOR, TYPE, 2, false, 6, true),
                                Range.range(ALLOCATOR, TYPE, 7, true, 9, false)),
                        or(and(greaterThan(1), lessThanOrEqual(6)), and(greaterThanOrEqual(7), lessThan(9)), null_())));
    }

    private static Filter equalTo(double value) {
        return Filter.value(ValueFilter.of(COLUMN, FilterType.EQUAL_TO, FilterValue.numberFilter(value)));
    }

    private static Filter greaterThan(double value) {
        return Filter.value(ValueFilter.of(COLUMN, FilterType.GREATER_THAN, FilterValue.numberFilter(value)));
    }

    private static Filter greaterThanOrEqual(double value) {
        return Filter.value(ValueFilter.of(COLUMN, FilterType.GREATER_THAN_OR_EQUAL, FilterValue.numberFilter(value)));
    }

    private static Filter lessThan(double value) {
        return Filter.value(ValueFilter.of(COLUMN, FilterType.LESS_THAN, FilterValue.numberFilter(value)));
    }

    private static Filter lessThanOrEqual(double value) {
        return Filter.value(ValueFilter.of(COLUMN, FilterType.LESS_THAN_OR_EQUAL, FilterValue.numberFilter(value)));
    }

    private static Filter true_() {
        return Filter.true_(TrueFilter.of());
    }

    private static Filter false_() {
        return Filter.false_(FalseFilter.of());
    }

    private static Filter not(Filter filter) {
        return Filter.not(NotFilter.of(filter));
    }

    private static Filter null_() {
        return Filter.null_(NullFilter.of(COLUMN));
    }

    private static Filter or(Filter... filters) {
        return Filter.or(OrFilter.of(ImmutableList.copyOf(filters)));
    }

    private static Filter and(Filter... filters) {
        return Filter.and(AndFilter.of(ImmutableList.copyOf(filters)));
    }
}
