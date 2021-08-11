/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.tokens.auth.BearerToken;
import java.util.Map;
import java.util.Optional;

final class ServicesConfigBlockEnvironmentOverrides extends ServicesConfigBlock {

    static final ServicesConfigBlockEnvironmentOverrides INSTANCE = new ServicesConfigBlockEnvironmentOverrides();

    private ServicesConfigBlockEnvironmentOverrides() {}

    @Override
    public Optional<BearerToken> defaultApiToken() {
        return Optional.empty();
    }

    @Override
    public Optional<SslConfiguration> defaultSecurity() {
        return Optional.empty();
    }

    @Override
    public Map<String, PartialServiceConfiguration> services() {
        return ImmutableMap.of();
    }

    @Override
    public Optional<ProxyConfiguration> defaultProxyConfiguration() {
        return Optional.empty();
    }

    @Override
    public Optional<HumanReadableDuration> defaultConnectTimeout() {
        return getOptionalConfigurationFromEnvironment("connect_timeout").map(HumanReadableDuration::valueOf);
    }

    @Override
    public Optional<HumanReadableDuration> defaultReadTimeout() {
        return getOptionalConfigurationFromEnvironment("read_timeout").map(HumanReadableDuration::valueOf);
    }

    @Override
    public Optional<HumanReadableDuration> defaultWriteTimeout() {
        return getOptionalConfigurationFromEnvironment("write_timeout").map(HumanReadableDuration::valueOf);
    }

    @Override
    public Optional<HumanReadableDuration> defaultBackoffSlotSize() {
        return getOptionalConfigurationFromEnvironment("backoff_slot_size").map(HumanReadableDuration::valueOf);
    }

    @Override
    public Optional<Boolean> defaultEnableGcmCipherSuites() {
        return getOptionalConfigurationFromEnvironment("enable_gcm_cipher_suites")
                .map(Boolean::valueOf);
    }

    @Override
    public Optional<Boolean> defaultEnableHttp2() {
        return getOptionalConfigurationFromEnvironment("enable_http_2").map(Boolean::valueOf);
    }

    @Override
    public Optional<Boolean> defaultFallbackToCommonNameVerification() {
        return getOptionalConfigurationFromEnvironment("fallback_to_common_name_verification")
                .map(Boolean::valueOf);
    }

    private static Optional<String> getOptionalConfigurationFromEnvironment(String name) {
        return Optional.ofNullable(System.getenv(name));
    }
}
