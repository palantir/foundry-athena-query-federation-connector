/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.foundry.athena.connector;

import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FoundryAthenaVersion {

    private FoundryAthenaVersion() {}

    private static final Logger log = LoggerFactory.getLogger(FoundryAthenaVersion.class);

    private static final String MAJOR = "major";
    private static final String MINOR = "minor";
    private static final String PATCH = "patch";

    private static final Pattern VERSION_PATTERN =
            Pattern.compile("(?<" + MAJOR + ">\\d+)\\.(?<" + MINOR + ">\\d+)\\.(?<" + PATCH + ">\\d+.*)");

    public static String loadFromClasspath() {
        String version = Optional.ofNullable(
                        FoundryAthenaVersion.class.getPackage().getImplementationVersion())
                .orElse("0.0.0");
        log.debug("Version {}", SafeArg.of("version", version));

        Matcher matcher = VERSION_PATTERN.matcher(version);
        if (!matcher.matches()) {
            throw new IllegalStateException(
                    "The version '" + version + "' did not match the expected pattern MAJOR.MINOR.PATCH");
        }

        return String.format(
                "%s.%s.%s",
                Integer.parseInt(matcher.group(MAJOR)), Integer.parseInt(matcher.group(MINOR)), matcher.group(PATCH));
    }
}
