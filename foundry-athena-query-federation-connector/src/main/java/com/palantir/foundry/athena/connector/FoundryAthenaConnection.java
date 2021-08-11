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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.RandomSampler;
import com.palantir.tracing.Tracer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

final class FoundryAthenaConnection {

    private static final String FOUNDRY_URL = "foundry_url";
    private static final String FOUNDRY_SECRET = "foundry_secret";
    private static final String TRACE_RATE = "trace_sample_rate";
    private static final String MAX_NUM_STREAMING_RETRIES = "max_num_streaming_retries";

    private final FoundryAthenaClients clients;
    private final FoundryAuthProvider authProvider;

    private FoundryAthenaConnection(FoundryAthenaClients clients, FoundryAuthProvider authProvider) {
        this.clients = clients;
        this.authProvider = authProvider;
    }

    public static FoundryAthenaConnection buildFromEnvironmentWithSecretSupplier(
            Function<String, String> secretSupplier) {
        configureTraceSampling();
        return new FoundryAthenaConnection(buildClients(), buildAuthProvider(secretSupplier));
    }

    private static void configureTraceSampling() {
        float traceRate = getOptionalConfigurationFromEnvironment(TRACE_RATE)
                .map(Float::valueOf)
                .orElse(0.01f);
        Tracer.setSampler(RandomSampler.create(traceRate));
    }

    private static FoundryAthenaClients buildClients() {
        try {
            return new FoundryAthenaClients(
                    new URI(getMandatoryConfigurationFromEnvironment(FOUNDRY_URL)),
                    getOptionalIntConfigurationFromEnvironment(MAX_NUM_STREAMING_RETRIES));
        } catch (URISyntaxException e) {
            throw new SafeRuntimeException("failed to parse foundry url", e);
        }
    }

    private static FoundryAuthProvider buildAuthProvider(Function<String, String> secretSupplier) {
        return new FoundryAuthProvider(secretSupplier, getMandatoryConfigurationFromEnvironment(FOUNDRY_SECRET));
    }

    private static String getMandatoryConfigurationFromEnvironment(String name) {
        return getOptionalConfigurationFromEnvironment(name)
                .orElseThrow(() -> new SafeRuntimeException(
                        "Could not read configuration from environment, was null or empty",
                        SafeArg.of("configurationName", name)));
    }

    private static Optional<String> getOptionalConfigurationFromEnvironment(String name) {
        return Optional.ofNullable(System.getenv(name));
    }

    private static OptionalInt getOptionalIntConfigurationFromEnvironment(String name) {
        String raw = System.getenv(name);
        if (raw == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Integer.parseInt(raw));
    }

    public FoundryAthenaClients clients() {
        return clients;
    }

    public FoundryAuthProvider authProvider() {
        return authProvider;
    }
}
