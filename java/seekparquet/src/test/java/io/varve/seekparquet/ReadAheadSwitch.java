// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Runs every listing that asks for no read-ahead with the read-ahead the system property {@code
 * seekparquet.test.readAhead} gives ({@code ./gradlew test -Pseekparquet.test.readAhead=4}), so that the
 * whole suite checks that read-ahead changes no result. Registered for every test class
 * (junit-platform.properties); without the property it changes nothing.
 */
public final class ReadAheadSwitch implements BeforeAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        ReadAhead.testDefault = Integer.getInteger("seekparquet.test.readAhead", 0);
    }
}
