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
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.handlers.AthenaExceptionFilter;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.foundry.athena.api.FetchSliceRequest;
import com.palantir.foundry.athena.api.FoundryAthenaRecordServiceBlocking;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"Slf4jLogsafeArgs", "PreferSafeLogger"})
public final class FoundryRecordHandler extends RecordHandler {

    private static final Logger log = LoggerFactory.getLogger(FoundryRecordHandler.class);

    private final AmazonS3 amazonS3;
    private final AmazonAthena athena;
    private final ThrottlingInvoker athenaInvoker = ThrottlingInvoker.newDefaultBuilder(
                    AthenaExceptionFilter.ATHENA_EXCEPTION_FILTER)
            .build();
    private final FoundryAuthProvider foundryAuthProvider;
    private final FoundryAthenaRecordServiceBlocking recordService;
    private final ThrottlingInvoker throttlingInvoker = ThrottlingInvoker.newDefaultBuilder(
                    FoundryThrottlingExceptionFilter.INSTANCE)
            .build();

    public FoundryRecordHandler() {
        super(FoundryAthenaConstants.SOURCE_TYPE);
        this.amazonS3 = AmazonS3ClientBuilder.defaultClient();
        this.athena = AmazonAthenaClientBuilder.defaultClient();

        FoundryAthenaConnection connection =
                FoundryAthenaConnection.buildFromEnvironmentWithSecretSupplier(this::getSecret);
        this.foundryAuthProvider = connection.authProvider();
        this.recordService = connection.clients().recordService();
    }

    @VisibleForTesting
    FoundryRecordHandler(
            AmazonS3 amazonS3,
            AmazonAthena athena,
            AWSSecretsManager secretsManager,
            FoundryAuthProvider foundryAuthProvider,
            FoundryAthenaRecordServiceBlocking recordService) {
        super(amazonS3, secretsManager, athena, FoundryAthenaConstants.SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.athena = athena;
        this.foundryAuthProvider = foundryAuthProvider;
        this.recordService = recordService;
    }

    @Override
    @SuppressWarnings("MustBeClosedChecker")
    public RecordResponse doReadRecords(BlockAllocator allocator, ReadRecordsRequest request) throws Exception {
        log.info("doReadRecords: {}:{}", request.getSchema(), request.getSplit().getSpillLocation());
        log.debug("Reading records with constraints: {}", request.getConstraints());
        SpillConfig spillConfig = getSpillConfig(request);
        S3Spiller spiller = new S3Spiller(amazonS3, spillConfig, allocator);

        List<String> columnNames =
                request.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());

        // create a temporary block to obtain a handle to the BufferAllocator and allocator id
        BufferAllocator bufferAllocator;
        String allocatorId;
        try (Block block = allocator.createBlock(request.getSchema())) {
            bufferAllocator = block.getFieldVectors().get(0).getAllocator();
            allocatorId = block.getAllocatorId();
        }

        throttlingInvoker.setBlockSpiller(spiller);
        try (QueryStatusChecker queryStatusChecker =
                        new QueryStatusChecker(athena, athenaInvoker, request.getQueryId());
                InputStream is = throttlingInvoker.invoke(() -> recordService.fetchSlice(
                        foundryAuthProvider.getAuthHeader(),
                        FetchSliceRequest.builder()
                                .slice(Slices.INSTANCE.fromSplit(request.getSplit()))
                                .columnNames(columnNames)
                                .maxBatchSize(SafeLong.of(spillConfig.getMaxBlockBytes()))
                                .build()))) {

            // we do not auto-close the reader to avoid releasing the buffers before serialization in the case
            // the block is held in memory
            PeekableArrowStreamReader reader = new PeekableArrowStreamReader(is, bufferAllocator);
            VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
            Block block = new Block(allocatorId, request.getSchema(), vectorSchemaRoot);

            reader.loadNextBatch();
            // spill if we have more blocks to read or the current block is too large to return
            if (reader.hasNextBatch() || block.getSize() > spillConfig.getMaxInlineBlockSize()) {
                do {
                    spiller.spillBlock(block);
                } while (queryStatusChecker.isQueryRunning() && reader.loadNextBatch());

                // we have spilled so we can clean up the reader
                reader.close();
                return new RemoteReadRecordsResponse(
                        request.getCatalogName(),
                        request.getSchema(),
                        spiller.getSpillLocations(),
                        spillConfig.getEncryptionKey());
            } else {
                // no more batches so immediately return the block
                return new ReadRecordsResponse(request.getCatalogName(), block);
            }
        }
    }

    @Override
    protected void readWithConstraint(
            BlockSpiller _spiller, ReadRecordsRequest _recordsRequest, QueryStatusChecker _queryStatusChecker) {
        throw new SafeIllegalStateException(
                "'readWithConstraint' should not be called as 'doReadRecords' is overridden");
    }
}
