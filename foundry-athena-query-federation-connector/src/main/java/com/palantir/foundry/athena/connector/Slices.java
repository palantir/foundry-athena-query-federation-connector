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
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.foundry.athena.api.Slice;
import com.palantir.logsafe.exceptions.SafeRuntimeException;

enum Slices {
    INSTANCE;

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newClientObjectMapper();

    @VisibleForTesting
    static final String SLICE_KEY = "slice";

    Split toSplit(SpillLocation spillLocation, EncryptionKey encryptionKey, Slice slice) {
        try {
            return Split.newBuilder(spillLocation, encryptionKey)
                    .add(SLICE_KEY, OBJECT_MAPPER.writeValueAsString(slice))
                    .build();
        } catch (JsonProcessingException e) {
            throw new SafeRuntimeException("failed to serialize slice", e);
        }
    }

    Slice fromSplit(Split split) {
        try {
            return OBJECT_MAPPER.readValue(split.getProperty(SLICE_KEY), Slice.class);
        } catch (JsonProcessingException e) {
            throw new SafeRuntimeException("failed to deserialize slice", e);
        }
    }
}
