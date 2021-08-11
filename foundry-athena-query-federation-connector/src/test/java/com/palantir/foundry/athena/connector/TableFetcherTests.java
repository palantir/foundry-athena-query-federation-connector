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

import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.google.common.collect.ImmutableSet;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.GetTableResponse;
import com.palantir.foundry.athena.api.TableName;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class TableFetcherTests {

    private static final String CATALOG_NAME = "catalog-name";
    private static final String TABLE_NAME = "table-name";
    private static final ImmutableSet<String> PARTITION_COLUMNS = ImmutableSet.of("column");

    @Mock
    private FoundryAuthProvider authProvider;

    @Mock
    private FoundryAthenaMetadataServiceBlocking metadataService;

    @Mock
    private GetTableRequest request;

    @Mock
    private com.amazonaws.athena.connector.lambda.domain.TableName tableName;

    private TableFetcher tableFetcher;

    @BeforeEach
    void before() {
        when(authProvider.getAuthHeader()).thenReturn(TestConstants.AUTH_HEADER);

        tableFetcher = new TableFetcher(authProvider, metadataService);
    }

    @Test
    void getTable() {
        when(request.getCatalogName()).thenReturn(CATALOG_NAME);
        when(request.getTableName()).thenReturn(tableName);
        when(tableName.getTableName()).thenReturn(TABLE_NAME);

        when(metadataService.getTable(TestConstants.AUTH_HEADER, TableName.of(TABLE_NAME)))
                .thenReturn(GetTableResponse.builder()
                        .locator(TestConstants.CATALOG_LOCATOR)
                        .schema(TestConstants.SCHEMA)
                        .partitionColumns(PARTITION_COLUMNS)
                        .build());

        com.amazonaws.athena.connector.lambda.metadata.GetTableResponse expected =
                new com.amazonaws.athena.connector.lambda.metadata.GetTableResponse(
                        CATALOG_NAME,
                        tableName,
                        new Schema(
                                TestConstants.SCHEMA.getFields(),
                                FoundryAthenaObjectMapper.objectMapper()
                                        .convertValue(TestConstants.CATALOG_LOCATOR, TableFetcher.METADATA_TYPE)),
                        PARTITION_COLUMNS);

        assertThat(tableFetcher.getTable(request)).isEqualTo(expected);
    }
}
