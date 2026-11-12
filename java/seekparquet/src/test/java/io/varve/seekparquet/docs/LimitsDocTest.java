// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.docs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.Limits;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The defaults docs/reference/limits.md states (rendered from the Go reader's source) are the defaults
 * {@link Limits#DEFAULT} returns here, derived ones included. Go's TestLimitsDoc checks the same page.
 */
class LimitsDocTest {

    private static final Pattern ROW =
            Pattern.compile("(?m)^\\| \\[`(\\w+)`\\]\\(#\\w+\\) \\| `(\\w+)` \\| ([^|]+) \\| ([^|]+) \\|$");
    private static final Pattern AMOUNT =
            Pattern.compile("([0-9][0-9,]*)(?: (KiB|MiB|GiB))?(?: at their defaults)?\\s*$");
    private static final Map<String, Integer> SHIFT = Map.of("KiB", 10, "MiB", 20, "GiB", 30);

    @Test
    void defaultsMatchTheReference() throws Exception {
        Path page = Path.of(System.getProperty("seekparquet.conformance"), "..", "docs", "reference", "limits.md");
        Matcher row = ROW.matcher(Files.readString(page));
        List<String> methods = new ArrayList<>();
        while (row.find()) {
            String method = row.group(2);
            methods.add(method);
            Matcher amount = AMOUNT.matcher(row.group(3).strip());
            assertTrue(amount.find(), "no amount in " + row.group(3));
            long want = Long.parseLong(amount.group(1).replace(",", ""));
            if (amount.group(2) != null) {
                want <<= SHIFT.get(amount.group(2));
            }
            long got = ((Number) Limits.class.getMethod(method).invoke(Limits.DEFAULT)).longValue();
            assertEquals(want, got, "Limits.DEFAULT." + method + "() against limits.md");
        }
        assertTrue(!methods.isEmpty(), "no settings table in " + page);
    }
}
