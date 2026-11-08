// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.conformance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdapterTest {
    @TempDir Path tmp;

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

    @Test
    void cursorsRequireExactlyOneTagWithCanonicalBase64() {
        for (String cursor : new String[] {"\"YQ==\"", "{}", "{\"key_base64\":\"YQ==\"}",
                "{\"prefix_base64\":null}", "{\"prefix_base64\":\"YQ\"}",
                "{\"key_base64\":null,\"offset\":0}", "{\"key_base64\":\"YR==\",\"offset\":0}",
                "{\"key_base64\":\"YQ==\",\"offset\":\"0\"}",
                "{\"key_base64\":\"YQ==\",\"offset\":0,\"prefix_base64\":\"\"}",
                "{\"prefix_base64\":\"\",\"extra\":0}"}) {
            for (String op : new String[] {"range", "list"}) {
                String response = new Adapter().handle("{\"id\":\"tags\",\"op\":\"" + op
                        + "\",\"limit\":1,\"delimiter_base64\":\"Lw==\",\"after\":" + cursor + "}");
                assertTrue(response.contains("\"status\":\"invalid_request\""), response);
                assertTrue(!response.contains("dataset is missing"), response);
            }
        }
    }

    @Test
    void validCursorEndpointsAndEmptyBytesAreAccepted() throws IOException {
        Files.writeString(tmp.resolve("manifest.json"), """
                {"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"test",
                 "key":{"column":"key","unique":false},"parts":[]}
                """);
        StringBuilder dataset = new StringBuilder();
        MiniJson.quote(dataset, tmp.toString());
        Adapter adapter = new Adapter();
        for (String op : new String[] {"range", "list"}) {
            for (String cursor : new String[] {"null", "{\"key_base64\":\"\",\"offset\":0}",
                    "{\"key_base64\":\"YQ==\",\"offset\":9223372036854775807}"}) {
                String response = adapter.handle("{\"id\":\"valid\",\"op\":\"" + op
                        + "\",\"dataset\":" + dataset + ",\"limit\":1,\"delimiter_base64\":\"Lw==\",\"after\":" + cursor + "}");
                assertTrue(response.contains("\"status\":\"ok\""), response);
            }
        }
        String response = adapter.handle("{\"id\":\"prefix\",\"op\":\"list\",\"dataset\":" + dataset
                + ",\"limit\":1,\"delimiter_base64\":\"Lw==\",\"after\":{\"prefix_base64\":\"\"}}");
        assertTrue(response.contains("\"status\":\"ok\""), response);
    }
}
