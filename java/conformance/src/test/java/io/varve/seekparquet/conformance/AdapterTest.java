// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.conformance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AdapterTest {
    @Test
    void cursorOffsetsRejectSignsFractionsExponentsAndOverflow() {
        Adapter adapter = new Adapter();
        for (String offset : new String[] {"-0", "-1", "0.0", "0e0", "9223372036854775808"}) {
            for (String op : new String[] {"range", "list"}) {
                String response = adapter.handle("{\"id\":\"cursor\",\"op\":\"" + op
                        + "\",\"limit\":1,\"delimiter_base64\":\"Lw==\",\"after\":{\"key_base64\":\"YQ==\",\"offset\":" + offset + "}}");
                assertTrue(response.contains("\"status\":\"invalid_request\""), response);
                assertTrue(response.contains("cursor"), response);
            }
        }
    }

    @Test
    void rangeRejectsPrefixCursorBeforeOpeningDataset() {
        String response = new Adapter().handle("{\"id\":\"range\",\"op\":\"range\",\"limit\":1,\"after\":{\"prefix_base64\":\"\"}}");
        assertTrue(response.contains("a range accepts only a row cursor"), response);
    }
}
