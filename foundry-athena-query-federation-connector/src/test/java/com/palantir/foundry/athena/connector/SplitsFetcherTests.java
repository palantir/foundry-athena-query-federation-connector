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
import static org.mockito.Mockito.when;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.foundry.athena.api.AndFilter;
import com.palantir.foundry.athena.api.Filter;
import com.palantir.foundry.athena.api.FilterType;
import com.palantir.foundry.athena.api.FilterValue;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.GetSlicesRequest;
import com.palantir.foundry.athena.api.GetSlicesResponse;
import com.palantir.foundry.athena.api.OrFilter;
import com.palantir.foundry.athena.api.Slice;
import com.palantir.foundry.athena.api.ValueFilter;
import com.palantir.foundry.athena.connector.SplitsFetcher.SpillLocationFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class SplitsFetcherTests {

    private static final String CATALOG_NAME = "catalog-name";
    private static final String PARTITION_COLUMN = "integer";

    @Mock
    private FoundryAuthProvider authProvider;

    @Mock
    private FoundryAthenaMetadataServiceBlocking metadataService;

    @Mock
    private Slices slices;

    @Mock
    private GetSplitsRequest request;

    @Mock
    private Constraints constraints;

    @Mock
    private Schema schema;

    @Mock
    private SpillLocationFactory spillLocationFactory;

    @Mock
    private SpillLocation spillLocation;

    @Mock
    private Slice firstSlice;

    @Mock
    private Slice secondSlice;

    @Mock
    private Split firstSplit;

    @Mock
    private Split secondSplit;

    @Mock
    private EncryptionKey encryptionKey;

    private SplitsFetcher splitsFetcher;
    public static final Map<String, String> CUSTOM_METADATA = FoundryAthenaObjectMapper.objectMapper()
            .convertValue(TestConstants.CATALOG_LOCATOR, new TypeReference<>() {});

    @BeforeEach
    void before() {
        when(authProvider.getAuthHeader()).thenReturn(TestConstants.AUTH_HEADER);

        when(schema.getCustomMetadata()).thenReturn(CUSTOM_METADATA);

        when(request.getCatalogName()).thenReturn(CATALOG_NAME);
        when(request.getSchema()).thenReturn(schema);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary())
                .thenReturn(ImmutableMap.of(
                        PARTITION_COLUMN,
                        EquatableValueSet.newBuilder(new BlockAllocatorImpl(), MinorType.INT.getType(), true, false)
                                .add(1)
                                .add(3)
                                .build()));

        when(spillLocationFactory.makeSpillLocation()).thenReturn(spillLocation);
        when(slices.toSplit(spillLocation, encryptionKey, firstSlice)).thenReturn(firstSplit);
        when(slices.toSplit(spillLocation, encryptionKey, secondSlice)).thenReturn(secondSplit);

        splitsFetcher = new SplitsFetcher(authProvider, metadataService, slices);
    }

    @Test
    void getSplits() {
        when(metadataService.getSlices(authProvider.getAuthHeader(), getSlicesRequest(Optional.empty())))
                .thenReturn(getSlicesResponse(firstSlice, secondSlice));

        assertThat(splitsFetcher.getSplits(request, spillLocationFactory, encryptionKey))
                .isEqualTo(new GetSplitsResponse(CATALOG_NAME, ImmutableSet.of(firstSplit, secondSplit)));
    }

    @Test
    void getSplits_twoPages() {
        when(metadataService.getSlices(authProvider.getAuthHeader(), getSlicesRequest(Optional.empty())))
                .thenReturn(getSlicesResponse(Optional.of("second-page"), firstSlice));

        when(metadataService.getSlices(authProvider.getAuthHeader(), getSlicesRequest(Optional.of("second-page"))))
                .thenReturn(getSlicesResponse(secondSlice));

        assertThat(splitsFetcher.getSplits(request, spillLocationFactory, encryptionKey))
                .isEqualTo(new GetSplitsResponse(CATALOG_NAME, ImmutableSet.of(firstSplit, secondSplit)));
    }

    private static GetSlicesRequest getSlicesRequest(Optional<String> nextPageToken) {
        return GetSlicesRequest.builder()
                .locator(TestConstants.CATALOG_LOCATOR)
                .filter(Filter.and(AndFilter.of(ImmutableList.of(Filter.or(OrFilter.of(ImmutableList.of(
                        Filter.value(
                                ValueFilter.of(PARTITION_COLUMN, FilterType.EQUAL_TO, FilterValue.numberFilter(1))),
                        Filter.value(ValueFilter.of(
                                PARTITION_COLUMN, FilterType.EQUAL_TO, FilterValue.numberFilter(3))))))))))
                .nextPageToken(nextPageToken)
                .build();
    }

    private static GetSlicesResponse getSlicesResponse(Slice... slices) {
        return getSlicesResponse(Optional.empty(), slices);
    }

    private static GetSlicesResponse getSlicesResponse(Optional<String> nextPageToken, Slice... slices) {
        return GetSlicesResponse.builder()
                .slices(ImmutableList.copyOf(slices))
                .nextPageToken(nextPageToken)
                .build();
    }
}
