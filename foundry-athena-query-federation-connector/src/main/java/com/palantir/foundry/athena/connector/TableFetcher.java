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

import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

final class TableFetcher {

    @VisibleForTesting
    static final TypeReference<Map<String, String>> METADATA_TYPE = new TypeReference<>() {};

    private final FoundryAuthProvider authProvider;
    private final FoundryAthenaMetadataServiceBlocking metadataService;

    TableFetcher(FoundryAuthProvider authProvider, FoundryAthenaMetadataServiceBlocking metadataService) {
        this.authProvider = authProvider;
        this.metadataService = metadataService;
    }

    GetTableResponse getTable(GetTableRequest request) {
        com.palantir.foundry.athena.api.GetTableResponse getTableResponse = metadataService.getTable(
                authProvider.getAuthHeader(),
                com.palantir.foundry.athena.api.TableName.of(
                        request.getTableName().getTableName()));

        return new GetTableResponse(
                request.getCatalogName(),
                request.getTableName(),
                new Schema(
                        getTableResponse.getSchema().getFields(),
                        FoundryAthenaObjectMapper.objectMapper()
                                .convertValue(getTableResponse.getLocator(), METADATA_TYPE)),
                getTableResponse.getPartitionColumns());
    }
}
