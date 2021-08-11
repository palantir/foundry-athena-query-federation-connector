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
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.palantir.foundry.athena.api.AndFilter;
import com.palantir.foundry.athena.api.CatalogLocator;
import com.palantir.foundry.athena.api.Filter;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.GetSlicesRequest;
import com.palantir.foundry.athena.api.GetSlicesResponse;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Slf4jLogsafeArgs")
final class SplitsFetcher {

    private static final Logger log = LoggerFactory.getLogger(SplitsFetcher.class);

    private final FoundryAuthProvider authProvider;
    private final FoundryAthenaMetadataServiceBlocking metadataService;
    private final Slices slices;

    SplitsFetcher(FoundryAuthProvider authProvider, FoundryAthenaMetadataServiceBlocking metadataService) {
        this(authProvider, metadataService, Slices.INSTANCE);
    }

    SplitsFetcher(
            FoundryAuthProvider authProvider, FoundryAthenaMetadataServiceBlocking metadataService, Slices slices) {
        this.authProvider = authProvider;
        this.metadataService = metadataService;
        this.slices = slices;
    }

    GetSplitsResponse getSplits(
            GetSplitsRequest request, SpillLocationFactory spillLocationFactory, EncryptionKey encryptionKey) {
        CatalogLocator locator = FoundryAthenaObjectMapper.objectMapper()
                .convertValue(request.getSchema().getCustomMetadata(), CatalogLocator.class);

        Optional<Filter> filter;
        if (request.getConstraints().getSummary().isEmpty()) {
            filter = Optional.empty();
        } else {
            // we just push down all constraints which will include those for any partition columns
            filter = Optional.of(Filter.and(AndFilter.of(request.getConstraints().getSummary().entrySet().stream()
                    .map(entry -> ConstraintConverter.convert(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()))));
        }

        Set<Split> splits = new HashSet<>();
        Optional<String> pageToken = Optional.empty();
        while (true) {
            GetSlicesResponse response = metadataService.getSlices(
                    authProvider.getAuthHeader(),
                    GetSlicesRequest.builder()
                            .locator(locator)
                            .filter(filter)
                            .nextPageToken(pageToken)
                            .build());

            splits.addAll(response.getSlices().stream()
                    .map(slice -> slices.toSplit(spillLocationFactory.makeSpillLocation(), encryptionKey, slice))
                    .collect(Collectors.toSet()));

            if (response.getNextPageToken().isPresent()) {
                pageToken = response.getNextPageToken();
            } else {
                log.debug("finished planning splits. number of splits: {}", splits.size());
                return new GetSplitsResponse(request.getCatalogName(), splits);
            }
        }
    }

    @FunctionalInterface
    interface SpillLocationFactory {
        SpillLocation makeSpillLocation();
    }
}
