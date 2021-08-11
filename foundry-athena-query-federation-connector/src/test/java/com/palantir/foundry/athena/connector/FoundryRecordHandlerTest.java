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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.foundry.athena.api.FoundryAthenaRecordServiceBlocking;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class FoundryRecordHandlerTest {

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private AmazonAthena athena;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private FoundryAuthProvider foundryAuthProvider;

    @Mock
    private FoundryAthenaRecordServiceBlocking recordService;

    @Mock
    private ReadRecordsRequest readRecordsRequest;

    private final Map<String, byte[]> s3 = new LinkedHashMap<>();
    private BlockAllocator allocator;
    private ObjectMapper objectMapper;
    private FoundryRecordHandler handler;

    @BeforeEach
    void beforeEach() {
        allocator = new BlockAllocatorImpl();
        objectMapper = VersionedObjectMapperFactory.create(allocator);
        handler = new FoundryRecordHandler(amazonS3, athena, secretsManager, foundryAuthProvider, recordService);

        when(readRecordsRequest.getQueryId()).thenReturn("queryId");
        when(readRecordsRequest.getMaxBlockSize()).thenReturn(50L);
        when(readRecordsRequest.getMaxInlineBlockSize()).thenReturn(50L);
        when(readRecordsRequest.getSchema()).thenReturn(TestConstants.SCHEMA);
        when(readRecordsRequest.getSplit()).thenReturn(TestConstants.SPLIT);
    }

    @AfterEach
    void afterEach() {
        s3.clear();
    }

    @Test
    void testReadRecordsSingle() throws Exception {
        when(readRecordsRequest.getCatalogName()).thenReturn("catalog");

        List<List<Object>> data = ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(1.1, 2.2));
        InputStream stream = ArrowUtils.writeToStream(
                TestConstants.SCHEMA, ImmutableList.of(ArrowUtils.createBatch(TestConstants.SCHEMA, data)));
        when(recordService.fetchSlice(any(), any())).thenReturn(stream);

        RecordResponse response = handler.doReadRecords(allocator, readRecordsRequest);

        assetRoundTripSerializable(response);
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);

        ReadRecordsResponse readRecordsResponse = (ReadRecordsResponse) response;
        assertThat(TestUtils.readBlockDataAsColumns(TestConstants.SCHEMA, readRecordsResponse.getRecords()))
                .isEqualTo(data);

        assertThat(s3).isEmpty();
    }

    @Test
    void testReadRecordsMultiple() throws Exception {
        mockS3();

        when(readRecordsRequest.getCatalogName()).thenReturn("catalog");

        List<List<Object>> firstBatchData = ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(1.1, 2.2));
        List<List<Object>> secondBatchData = ImmutableList.of(ImmutableList.of(3, 4), ImmutableList.of(3.3, 4.4));
        InputStream stream = ArrowUtils.writeToStream(
                TestConstants.SCHEMA,
                ImmutableList.of(
                        ArrowUtils.createBatch(TestConstants.SCHEMA, firstBatchData),
                        ArrowUtils.createBatch(TestConstants.SCHEMA, secondBatchData)));
        when(recordService.fetchSlice(any(), any())).thenReturn(stream);

        RecordResponse response = handler.doReadRecords(allocator, readRecordsRequest);

        assetRoundTripSerializable(response);
        assertThat(response).isInstanceOf(RemoteReadRecordsResponse.class);

        assertThat(s3).hasSize(2);
        List<byte[]> blobs = new ArrayList<>(s3.values());
        assertThatSpillLocationContainsData(blobs.get(0), firstBatchData);
        assertThatSpillLocationContainsData(blobs.get(1), secondBatchData);
    }

    @Test
    void testReadRecordsLargeBlock() throws Exception {
        mockS3();

        when(readRecordsRequest.getCatalogName()).thenReturn("catalog");

        List<List<Object>> data =
                ImmutableList.of(ImmutableList.of(1, 2, 3, 4, 5), ImmutableList.of(1.1, 2.2, 3.3, 4.4, 5.5));
        InputStream stream = ArrowUtils.writeToStream(
                TestConstants.SCHEMA, ImmutableList.of(ArrowUtils.createBatch(TestConstants.SCHEMA, data)));
        when(recordService.fetchSlice(any(), any())).thenReturn(stream);

        RecordResponse response = handler.doReadRecords(allocator, readRecordsRequest);

        assetRoundTripSerializable(response);
        assertThat(response).isInstanceOf(RemoteReadRecordsResponse.class);

        assertThat(s3).hasSize(1);
        List<byte[]> blobs = new ArrayList<>(s3.values());
        assertThatSpillLocationContainsData(blobs.get(0), data);
    }

    @Test
    void testThrottling() {
        when(recordService.fetchSlice(any(), any())).thenThrow(QosException.throttle());

        assertThatThrownBy(() -> handler.doReadRecords(allocator, readRecordsRequest))
                .isInstanceOf(FederationThrottleException.class);
    }

    private void mockS3() {
        when(amazonS3.putObject(
                        eq(TestConstants.BUCKET), anyString(), any(InputStream.class), any(ObjectMetadata.class)))
                .then(args -> {
                    s3.put(args.getArgument(1), ByteStreams.toByteArray(args.getArgument(2)));
                    return new PutObjectResult();
                });
    }

    private void assetRoundTripSerializable(FederationResponse response) throws JsonProcessingException {
        FederationResponse other =
                objectMapper.readValue(objectMapper.writeValueAsString(response), FederationResponse.class);
        assertThat(other).isNotNull();
    }

    private void assertThatSpillLocationContainsData(byte[] bytes, List<List<Object>> expected) {
        Block block =
                new AesGcmBlockCrypto(allocator).decrypt(TestConstants.ENCRYPTION_KEY, bytes, TestConstants.SCHEMA);
        assertThat(TestUtils.readBlockDataAsColumns(TestConstants.SCHEMA, block))
                .isEqualTo(expected);
    }
}
