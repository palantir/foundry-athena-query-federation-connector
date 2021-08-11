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

import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Utility for converting Athena constraints (as defined by a {@link ValueSet} to a {@link Filter} that can be
 * evaluated server-side.
 */
final class ConstraintConverter {

    private ConstraintConverter() {}

    public static Filter convert(String columnName, ValueSet valueSet) {
        if (valueSet.isAll() && valueSet.isNullAllowed()) {
            return Filter.true_(TrueFilter.of());
        } else if (valueSet.isAll()) {
            return Filter.not(NotFilter.of(Filter.null_(NullFilter.of(columnName))));
        } else if (valueSet.isNone()) {
            return Filter.false_(FalseFilter.of());
        }

        if (valueSet instanceof AllOrNoneValueSet) {
            return convert(columnName, (AllOrNoneValueSet) valueSet);
        } else if (valueSet instanceof EquatableValueSet) {
            return convert(columnName, (EquatableValueSet) valueSet);
        } else if (valueSet instanceof SortedRangeSet) {
            return convert(columnName, (SortedRangeSet) valueSet);
        } else {
            throw new SafeIllegalArgumentException(
                    "Unsupported ValueSet type", SafeArg.of("type", valueSet.getClass()));
        }
    }

    private static Filter convert(String columnName, AllOrNoneValueSet valueSet) {
        if (valueSet.isNullAllowed()) {
            return Filter.null_(NullFilter.of(columnName));
        } else {
            return Filter.not(NotFilter.of(Filter.null_(NullFilter.of(columnName))));
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static Filter convert(String columnName, EquatableValueSet valueSet) {
        Filter isNull = Filter.null_(NullFilter.of(columnName));
        if (valueSet.isSingleValue() && valueSet.isNullAllowed()) {
            // only null allowed
            return isNull;
        } else if (valueSet.isSingleValue()) {
            // only one non-null value allowed
            FilterValue value = convertValue(valueSet.getType(), valueSet.getSingleValue());
            return Filter.value(ValueFilter.of(columnName, FilterType.EQUAL_TO, value));
        } else {
            List<Filter> filters = IntStream.range(0, valueSet.getValueBlock().getRowCount())
                    .boxed()
                    .map(index -> convertValue(valueSet.getType(), valueSet.getValue(index)))
                    .map(value -> {
                        Filter equalTo = Filter.value(ValueFilter.of(columnName, FilterType.EQUAL_TO, value));
                        return valueSet.isWhiteList() ? equalTo : Filter.not(NotFilter.of(equalTo));
                    })
                    .collect(Collectors.toList());

            if (valueSet.isWhiteList() && valueSet.isNullAllowed()) {
                // values are allowed, null allowed
                return filters.isEmpty()
                        ? isNull
                        : Filter.or(OrFilter.builder()
                                .filters(filters)
                                .filters(isNull)
                                .build());
            } else if (valueSet.isWhiteList()) {
                // values allowed, null not allowed
                return filters.isEmpty()
                        ? Filter.false_(FalseFilter.of())
                        : Filter.or(OrFilter.builder().filters(filters).build());
            } else if (valueSet.isNullAllowed()) {
                // values not allowed, null allowed
                return filters.isEmpty()
                        ? isNull
                        : Filter.or(OrFilter.builder()
                                .filters(Filter.and(AndFilter.of(filters)))
                                .filters(isNull)
                                .build());
            } else {
                // values not allowed, null not allowed
                return filters.isEmpty() ? Filter.not(NotFilter.of(isNull)) : Filter.and(AndFilter.of(filters));
            }
        }
    }

    private static Filter convert(String columnName, SortedRangeSet valueSet) {
        Filter isNull = Filter.null_(NullFilter.of(columnName));
        if (valueSet.isSingleValue() && valueSet.isNullAllowed()) {
            return isNull;
        } else if (valueSet.isSingleValue()) {
            Range range = valueSet.getOrderedRanges().iterator().next();
            return Filter.value(ValueFilter.of(
                    columnName, FilterType.EQUAL_TO, convertValue(range.getType(), range.getSingleValue())));
        } else {
            List<Filter> filters = valueSet.getOrderedRanges().stream()
                    .map(range -> convertRange(columnName, range))
                    .collect(Collectors.toList());
            OrFilter.Builder builder = OrFilter.builder().filters(filters);
            if (valueSet.isNullAllowed()) {
                builder.filters(isNull);
            }
            return Filter.or(builder.build());
        }
    }

    private static Filter convertRange(String columnName, Range range) {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        if (range.isSingleValue()) {
            return Filter.value(ValueFilter.of(
                    columnName, FilterType.EQUAL_TO, convertValue(range.getType(), range.getSingleValue())));
        } else if (range.isAll()) {
            return Filter.true_(TrueFilter.of());
        } else if (low.isLowerUnbounded()) {
            return convertMarker(columnName, high, false);
        } else if (high.isUpperUnbounded()) {
            return convertMarker(columnName, low, true);
        } else {
            return Filter.and(AndFilter.builder()
                    .filters(convertMarker(columnName, low, true))
                    .filters(convertMarker(columnName, high, false))
                    .build());
        }
    }

    private static Filter convertMarker(String columnName, Marker marker, boolean low) {
        return Filter.value(ValueFilter.of(
                columnName, convertBound(marker.getBound(), low), convertValue(marker.getType(), marker.getValue())));
    }

    private static FilterType convertBound(Bound bound, boolean low) {
        switch (bound) {
            case ABOVE:
                return FilterType.GREATER_THAN;
            case BELOW:
                return FilterType.LESS_THAN;
            case EXACTLY:
                return low ? FilterType.GREATER_THAN_OR_EQUAL : FilterType.LESS_THAN_OR_EQUAL;
            default:
                throw new SafeIllegalArgumentException("Unsupported bound", SafeArg.of("bound", bound));
        }
    }

    private static FilterValue convertValue(ArrowType arrowType, Object value) {
        return arrowType.accept(new FilterValueConverter(value));
    }
}
