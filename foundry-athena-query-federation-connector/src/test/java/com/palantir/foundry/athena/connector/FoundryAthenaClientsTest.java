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

import com.palantir.foundry.athena.api.FoundryAthenaMetadataServiceBlocking;
import com.palantir.foundry.athena.api.FoundryAthenaRecordServiceBlocking;
import java.net.URI;
import java.util.OptionalInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class FoundryAthenaClientsTest {

    @Mock
    private OptionalInt maxNumStreamingRetries;

    private FoundryAthenaClients foundryAthenaClients;

    @BeforeEach
    void before() {
        foundryAthenaClients = new FoundryAthenaClients(URI.create("https://base-uri/"), maxNumStreamingRetries);
    }

    @Test
    void metadataService() {
        assertThat(foundryAthenaClients.metadataService()).isInstanceOf(FoundryAthenaMetadataServiceBlocking.class);
    }

    @Test
    void recordService() {
        when(maxNumStreamingRetries.isPresent()).thenReturn(false);
        assertThat(foundryAthenaClients.recordService()).isInstanceOf(FoundryAthenaRecordServiceBlocking.class);
    }

    @Test
    void recordService_withMaxNumStreamingRetries() {
        when(maxNumStreamingRetries.isPresent()).thenReturn(true);
        assertThat(foundryAthenaClients.recordService()).isInstanceOf(FoundryAthenaRecordServiceBlocking.class);
    }
}
