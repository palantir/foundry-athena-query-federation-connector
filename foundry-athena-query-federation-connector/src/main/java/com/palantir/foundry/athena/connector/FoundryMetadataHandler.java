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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"Slf4jLogsafeArgs", "PreferSafeLogger"})
public final class FoundryMetadataHandler extends MetadataHandler {

    private static final Logger log = LoggerFactory.getLogger(FoundryMetadataHandler.class);

    private static final String SCHEMA_NAME = "foundry";

    private final TableFetcher tableFetcher;
    private final PartitionFetcher partitionFetcher;
    private final SplitsFetcher splitsFetcher;

    public FoundryMetadataHandler() {
        super(FoundryAthenaConstants.SOURCE_TYPE);
        FoundryAthenaConnection connection =
                FoundryAthenaConnection.buildFromEnvironmentWithSecretSupplier(this::getSecret);
        this.tableFetcher =
                new TableFetcher(connection.authProvider(), connection.clients().metadataService());
        this.partitionFetcher = new PartitionFetcher(
                connection.authProvider(), connection.clients().metadataService());
        this.splitsFetcher = new SplitsFetcher(
                connection.authProvider(), connection.clients().metadataService());
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator _allocator, ListSchemasRequest request) {
        return new ListSchemasResponse(request.getCatalogName(), ImmutableSet.of(SCHEMA_NAME));
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator _allocator, ListTablesRequest request) {
        return new ListTablesResponse(request.getCatalogName(), ImmutableSet.of(), null);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator _allocator, GetTableRequest request) {
        return tableFetcher.getTable(request);
    }

    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request) {
        if (request.getPartitionCols().isEmpty()) {
            partitionSchemaBuilder.addIntField(FoundryAthenaConstants.PARTITION_ID_COLUMN);
        }
        request.getSchema().getCustomMetadata().forEach(partitionSchemaBuilder::addMetadata);
    }

    @Override
    public void getPartitions(
            BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) {
        log.debug("Getting partitions with constraints: {}", request.getConstraints());
        partitionFetcher.writePartitions(blockWriter, request, queryStatusChecker);
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator _allocator, GetSplitsRequest request) {
        log.debug("Getting splits with constraints: {}", request.getConstraints());
        EncryptionKey encryptionKey = makeEncryptionKey(); // can be shared across
        return splitsFetcher.getSplits(request, () -> makeSpillLocation(request), encryptionKey);
    }
}
