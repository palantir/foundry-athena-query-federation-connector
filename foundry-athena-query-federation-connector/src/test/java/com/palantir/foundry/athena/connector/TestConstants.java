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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.foundry.athena.api.CatalogLocator;
import com.palantir.foundry.athena.api.Slice;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.ri.ResourceIdentifier;
import com.palantir.tokens.auth.AuthHeader;
import java.util.UUID;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public final class TestConstants {

    private static final ObjectMapper MAPPER = ObjectMappers.newClientObjectMapper();

    public static final RootAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
    public static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("notatoken");
    public static final ResourceIdentifier DATASET_RID = ResourceIdentifier.of("ri.foundry.main.dataset.d");
    public static final ResourceIdentifier TRANSACTION_RID =
            ResourceIdentifier.of(String.format("ri.foundry.main.transaction.%s", UUID.randomUUID()));
    public static final ResourceIdentifier START_TRANSACTION_RID =
            ResourceIdentifier.of(String.format("ri.foundry.main.transaction.%s", UUID.randomUUID()));
    public static final String BRANCH = "master";
    public static final CatalogLocator CATALOG_LOCATOR = CatalogLocator.builder()
            .datasetRid(DATASET_RID)
            .branch(BRANCH)
            .startTransactionRid(START_TRANSACTION_RID)
            .endTransactionRid(TRANSACTION_RID)
            .build();
    public static final Schema SCHEMA = new Schema(ImmutableList.of(
            new Field("integer", FieldType.nullable(MinorType.INT.getType()), ImmutableList.of()),
            new Field("double", FieldType.nullable(MinorType.FLOAT8.getType()), ImmutableList.of())));
    public static final Slice SLICE = MAPPER.convertValue(
            ImmutableMap.builder()
                    .put("locator", CATALOG_LOCATOR)
                    .put("basePath", "")
                    .put(
                            "spec",
                            ImmutableMap.builder()
                                    .put("type", "parquet")
                                    .put("parquet", ImmutableMap.builder().put("path", "/path/to/file"))
                                    .build())
                    .build(),
            Slice.class);

    public static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newClientObjectMapper();
    public static final String BUCKET = "bucket";
    public static final SpillLocation SPILL_LOCATION = new S3SpillLocation(BUCKET, "key", true);
    public static final EncryptionKey ENCRYPTION_KEY = new LocalKeyFactory().create();
    public static final Split SPLIT;

    static {
        try {
            SPLIT = Split.newBuilder(SPILL_LOCATION, ENCRYPTION_KEY)
                    .add(Slices.SLICE_KEY, OBJECT_MAPPER.writeValueAsString(TestConstants.SLICE))
                    .build();
        } catch (JsonProcessingException e) {
            throw new SafeRuntimeException(e);
        }
    }

    private TestConstants() {}
}
