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

import com.google.common.base.Suppliers;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgent.Agent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.FoundryAthenaRecordServiceBlocking;
import com.palantir.refreshable.Refreshable;
import java.net.URI;
import java.util.OptionalInt;
import java.util.function.Supplier;

final class FoundryAthenaClients {

    private static final String FOUNDRY_ATHENA_SERVICE = "foundry-athena";
    private static final String FOUNDRY_ATHENA_SERVICE_PATH = "/foundry-athena/api";

    private final Supplier<FoundryAthenaMetadataServiceBlocking> metadataService;
    private final Supplier<FoundryAthenaRecordServiceBlocking> recordService;

    FoundryAthenaClients(URI baseUrl, OptionalInt maxNumStreamingRetries) {
        ReloadingFactory dialogueClients = DialogueClients.create(Refreshable.only(ServicesConfigBlock.builder()
                        .from(ServicesConfigBlockDefaults.INSTANCE)
                        .from(ServicesConfigBlockEnvironmentOverrides.INSTANCE)
                        .putServices(
                                FOUNDRY_ATHENA_SERVICE,
                                PartialServiceConfiguration.builder()
                                        .addUris(baseUrl.resolve(FOUNDRY_ATHENA_SERVICE_PATH)
                                                .toString())
                                        .build())
                        .build()))
                .withUserAgent(UserAgent.of(Agent.of("athena-lambda", "0.0.0"))); // TODO(ahiggins): real version
        this.metadataService = Suppliers.memoize(
                () -> dialogueClients.get(FoundryAthenaMetadataServiceBlocking.class, FOUNDRY_ATHENA_SERVICE));
        this.recordService = Suppliers.memoize(() -> {
            ReloadingFactory clientFactory = dialogueClients;
            if (maxNumStreamingRetries.isPresent()) {
                clientFactory = clientFactory.withMaxNumRetries(maxNumStreamingRetries.getAsInt());
            }
            return clientFactory.get(FoundryAthenaRecordServiceBlocking.class, FOUNDRY_ATHENA_SERVICE);
        });
    }

    public FoundryAthenaMetadataServiceBlocking metadataService() {
        return metadataService.get();
    }

    public FoundryAthenaRecordServiceBlocking recordService() {
        return recordService.get();
    }
}
