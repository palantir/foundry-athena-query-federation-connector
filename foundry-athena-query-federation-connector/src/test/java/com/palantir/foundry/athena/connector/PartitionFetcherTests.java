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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.GetPartitionsRequest;
import com.palantir.foundry.athena.api.GetPartitionsResponsePage;
import com.palantir.foundry.athena.api.Partition;
import com.palantir.foundry.athena.api.PartitionValue;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class PartitionFetcherTests {

    private static final GetPartitionsRequest REQUEST = GetPartitionsRequest.builder()
            .locator(TestConstants.CATALOG_LOCATOR)
            .limit(PartitionFetcher.PAGE_SIZE)
            .pageToken(Optional.empty())
            .build();
    private static final Schema PARTITION_BLOCK_SCHEMA = new Schema(
            ImmutableList.of(new Field("integer", FieldType.nullable(MinorType.INT.getType()), ImmutableList.of())));
    private static final Schema UNPARTITIONED_PARTITION_BLOCK_SCHEMA = new Schema(ImmutableList.of(new Field(
            FoundryAthenaConstants.PARTITION_ID_COLUMN, FieldType.nullable(Types.MinorType.INT.getType()), null)));

    @Mock
    private FoundryAuthProvider authProvider;

    @Mock
    private FoundryAthenaMetadataServiceBlocking metadataService;

    @Mock
    private GetTableLayoutRequest request;

    @Mock
    private QueryStatusChecker queryStatusChecker;

    private BlockSpiller blockSpiller;
    private PartitionFetcher partitionFetcher;

    @BeforeEach
    void before() {
        partitionFetcher = new PartitionFetcher(authProvider, metadataService);
    }

    @AfterEach
    void after() {
        blockSpiller.close();
    }

    @Test
    void writePartitions() {

        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        when(authProvider.getAuthHeader()).thenReturn(TestConstants.AUTH_HEADER);
        when(request.getSchema())
                .thenReturn(new Schema(
                        TestConstants.SCHEMA.getFields(),
                        FoundryAthenaObjectMapper.objectMapper()
                                .convertValue(TestConstants.CATALOG_LOCATOR, TableFetcher.METADATA_TYPE)));
        when(request.getPartitionCols()).thenReturn(ImmutableSet.of("integer"));
        blockSpiller = new InMemorySingleBlockSpiller(PARTITION_BLOCK_SCHEMA, ConstraintEvaluator.emptyEvaluator());

        when(metadataService.getPartitions(TestConstants.AUTH_HEADER, REQUEST))
                .thenReturn(GetPartitionsResponsePage.builder()
                        .partitions(
                                ImmutableList.of(Partition.of(ImmutableMap.of("integer", PartitionValue.integer(1)))))
                        .nextPageToken(Optional.empty())
                        .build());

        partitionFetcher.writePartitions(blockSpiller, request, queryStatusChecker);

        List<List<Object>> expected = ImmutableList.of(ImmutableList.of(1));
        assertThat(TestUtils.readBlockDataAsColumns(PARTITION_BLOCK_SCHEMA, blockSpiller.getBlock()))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void writePartitions_twoPages() {
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        when(authProvider.getAuthHeader()).thenReturn(TestConstants.AUTH_HEADER);
        when(request.getSchema())
                .thenReturn(new Schema(
                        TestConstants.SCHEMA.getFields(),
                        FoundryAthenaObjectMapper.objectMapper()
                                .convertValue(TestConstants.CATALOG_LOCATOR, TableFetcher.METADATA_TYPE)));
        when(request.getPartitionCols()).thenReturn(ImmutableSet.of("integer"));
        blockSpiller = new InMemorySingleBlockSpiller(PARTITION_BLOCK_SCHEMA, ConstraintEvaluator.emptyEvaluator());

        when(metadataService.getPartitions(TestConstants.AUTH_HEADER, REQUEST))
                .thenReturn(GetPartitionsResponsePage.builder()
                        .partitions(
                                ImmutableList.of(Partition.of(ImmutableMap.of("integer", PartitionValue.integer(1)))))
                        .nextPageToken(Optional.of("second-page"))
                        .build());

        when(metadataService.getPartitions(
                        TestConstants.AUTH_HEADER,
                        GetPartitionsRequest.builder()
                                .from(REQUEST)
                                .pageToken("second-page")
                                .build()))
                .thenReturn(GetPartitionsResponsePage.builder()
                        .partitions(
                                ImmutableList.of(Partition.of(ImmutableMap.of("integer", PartitionValue.integer(3)))))
                        .nextPageToken(Optional.empty())
                        .build());

        partitionFetcher.writePartitions(blockSpiller, request, queryStatusChecker);

        List<List<Object>> expected = ImmutableList.of(ImmutableList.of(1, 3));
        assertThat(TestUtils.readBlockDataAsColumns(PARTITION_BLOCK_SCHEMA, blockSpiller.getBlock()))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void writePartitions_whenQueryStopsRunning_doesNotWriteAdditionalRowsToBlock() {
        when(queryStatusChecker.isQueryRunning()).thenReturn(true).thenReturn(false);

        when(authProvider.getAuthHeader()).thenReturn(TestConstants.AUTH_HEADER);
        when(request.getSchema())
                .thenReturn(new Schema(
                        TestConstants.SCHEMA.getFields(),
                        FoundryAthenaObjectMapper.objectMapper()
                                .convertValue(TestConstants.CATALOG_LOCATOR, TableFetcher.METADATA_TYPE)));
        when(request.getPartitionCols()).thenReturn(ImmutableSet.of("integer"));
        blockSpiller = new InMemorySingleBlockSpiller(PARTITION_BLOCK_SCHEMA, ConstraintEvaluator.emptyEvaluator());

        when(metadataService.getPartitions(TestConstants.AUTH_HEADER, REQUEST))
                .thenReturn(GetPartitionsResponsePage.builder()
                        .partitions(
                                ImmutableList.of(Partition.of(ImmutableMap.of("integer", PartitionValue.integer(1)))))
                        .nextPageToken(Optional.of("second-page"))
                        .build());

        partitionFetcher.writePartitions(blockSpiller, request, queryStatusChecker);

        List<List<Object>> expected = ImmutableList.of(ImmutableList.of(1));
        assertThat(TestUtils.readBlockDataAsColumns(PARTITION_BLOCK_SCHEMA, blockSpiller.getBlock()))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void writePartitions_whenTableIsUnpartitioned() {
        when(request.getPartitionCols()).thenReturn(ImmutableSet.of());
        blockSpiller = new InMemorySingleBlockSpiller(
                UNPARTITIONED_PARTITION_BLOCK_SCHEMA, ConstraintEvaluator.emptyEvaluator());

        partitionFetcher.writePartitions(blockSpiller, request, queryStatusChecker);

        List<List<Object>> expected = ImmutableList.of(ImmutableList.of(1));
        assertThat(TestUtils.readBlockDataAsColumns(UNPARTITIONED_PARTITION_BLOCK_SCHEMA, blockSpiller.getBlock()))
                .containsExactlyElementsOf(expected);
    }
}
