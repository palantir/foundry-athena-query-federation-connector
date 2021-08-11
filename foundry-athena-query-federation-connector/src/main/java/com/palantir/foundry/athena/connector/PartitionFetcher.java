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
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.foundry.athena.api.CatalogLocator;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.GetPartitionsRequest;
import com.palantir.foundry.athena.api.GetPartitionsResponsePage;
import java.util.Optional;

final class PartitionFetcher {

    @VisibleForTesting
    static final int PAGE_SIZE = 1000;

    private final FoundryAuthProvider authProvider;
    private final FoundryAthenaMetadataServiceBlocking metadataService;

    PartitionFetcher(FoundryAuthProvider authProvider, FoundryAthenaMetadataServiceBlocking metadataService) {
        this.authProvider = authProvider;
        this.metadataService = metadataService;
    }

    void writePartitions(
            BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) {
        if (request.getPartitionCols().isEmpty()) {
            blockWriter.writeRows((block, rowNum) -> {
                block.setValue(FoundryAthenaConstants.PARTITION_ID_COLUMN, rowNum, 1);
                return 1;
            });
        } else {
            getAndWritePartitions(blockWriter, request, queryStatusChecker);
        }
    }

    private void getAndWritePartitions(
            BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) {
        CatalogLocator locator = FoundryAthenaObjectMapper.objectMapper()
                .convertValue(request.getSchema().getCustomMetadata(), CatalogLocator.class);
        Optional<String> pageToken = Optional.empty();
        while (queryStatusChecker.isQueryRunning()) {
            GetPartitionsResponsePage page = metadataService.getPartitions(
                    authProvider.getAuthHeader(),
                    GetPartitionsRequest.builder()
                            .locator(locator)
                            .limit(PAGE_SIZE)
                            .pageToken(pageToken)
                            .build());

            page.getPartitions()
                    .forEach(partition -> blockWriter.writeRows((block, rowNum) -> {
                        boolean matched = partition.get().entrySet().stream()
                                .map(fieldName -> fieldName
                                        .getValue()
                                        .accept(new PartitionValueWriter(block, fieldName.getKey(), rowNum)))
                                .reduce(true, Boolean::logicalAnd);

                        // if all fields passed then we wrote 1 row
                        return matched ? 1 : 0;
                    }));

            if (page.getNextPageToken().isPresent()) {
                pageToken = page.getNextPageToken();
            } else {
                return;
            }
        }
    }
}
