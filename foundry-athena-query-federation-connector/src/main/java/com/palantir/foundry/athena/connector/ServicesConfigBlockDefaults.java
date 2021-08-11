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
import com.google.common.io.Resources;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tokens.auth.BearerToken;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

final class ServicesConfigBlockDefaults extends ServicesConfigBlock {

    static final ServicesConfigBlockDefaults INSTANCE = new ServicesConfigBlockDefaults();

    private static final String TRUST_STORE = "truststore.jks";

    private ServicesConfigBlockDefaults() {}

    @Override
    public Optional<BearerToken> defaultApiToken() {
        return Optional.empty();
    }

    @Override
    public Optional<SslConfiguration> defaultSecurity() {
        try {
            return Optional.of(SslConfiguration.of(
                    Paths.get(Resources.getResource(TRUST_STORE).toURI())));
        } catch (URISyntaxException e) {
            throw new SafeIllegalStateException("could not get URI for truststore", e);
        }
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
        return Optional.of(HumanReadableDuration.seconds(30));
    }

    @Override
    public Optional<HumanReadableDuration> defaultReadTimeout() {
        return Optional.empty();
    }

    @Override
    public Optional<HumanReadableDuration> defaultWriteTimeout() {
        return Optional.empty();
    }

    @Override
    public Optional<HumanReadableDuration> defaultBackoffSlotSize() {
        return Optional.empty();
    }

    @Override
    public Optional<Boolean> defaultEnableGcmCipherSuites() {
        return Optional.empty();
    }

    @Override
    public Optional<Boolean> defaultEnableHttp2() {
        return Optional.empty();
    }

    @Override
    public Optional<Boolean> defaultFallbackToCommonNameVerification() {
        return Optional.empty();
    }
}
