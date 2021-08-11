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

import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tokens.auth.BearerToken;
import java.util.function.Function;

final class FoundryAuthProvider {

    private final Function<String, String> secretSupplier;
    private final String secretName;

    FoundryAuthProvider(Function<String, String> secretSupplier, String secretName) {
        this.secretSupplier = secretSupplier;
        this.secretName = secretName;
    }

    public AuthHeader getAuthHeader() {
        return AuthHeader.of(BearerToken.valueOf(secretSupplier.apply(secretName)));
    }
}
