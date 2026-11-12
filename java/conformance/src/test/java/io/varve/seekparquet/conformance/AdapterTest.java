// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.conformance;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.varve.seekparquet.Dataset;
import io.varve.seekparquet.Query;
import io.varve.seekparquet.ReadOptions;
import io.varve.seekparquet.ReadStats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdapterTest {
    @TempDir Path tmp;

    private static String request(String op, String dataset, String limit, String extra) {
        StringBuilder path = new StringBuilder();
        MiniJson.quote(path, Path.of(System.getProperty("seekparquet.conformance"), "datasets", dataset).toString());
        return "{\"id\":\"limits\",\"op\":\"" + op + "\",\"dataset\":" + path
                + ",\"limit\":" + limit + ",\"delimiter_base64\":\"Lw==\"" + extra + "}";
    }

    @Test
    void oversizedLimitsReadAllMatchingEntriesWithoutChangingLibraryDefaults() {
        Adapter adapter = new Adapter();
        Adapter paged = new Adapter(2);
        for (String dataset : new String[] {"empty", "repeated-keys", "delimiter"}) {
            for (String op : new String[] {"range", "list"}) {
                String expected = adapter.handle(request(op, dataset, "1000", ""));
                assertTrue(expected.contains("\"status\":\"ok\""), expected);
                for (String limit : new String[] {"1000001", "2147483647", "2147483648",
                        "9223372036854775808", "999999999999999999999999999999999999"}) {
                    assertEquals(expected, adapter.handle(request(op, dataset, limit, "")));
                    assertEquals(expected, paged.handle(request(op, dataset, limit, "")));
                }
            }
        }
        // The library has no collected-result cap: the adapter pages only to bound its own requests.
        assertEquals(1_000_000, Adapter.PAGE_LIMIT);
        Path empty = Path.of(System.getProperty("seekparquet.conformance"), "datasets", "empty");
        try (Dataset dataset = Dataset.open(empty)) {
            // A collected result's limit must fit a List: beyond it rows is an invalid request, before any read.
            java.util.List<ReadStats> stats = new java.util.ArrayList<>();
            assertThrows(IllegalArgumentException.class,
                    () -> dataset.rows(Query.builder().limit(Long.MAX_VALUE).build(), ReadOptions.of(stats::add)));
            assertEquals(java.util.List.of(ReadStats.NONE), stats);
            assertEquals(0, dataset.rows(Query.builder().limit(Integer.MAX_VALUE).build()).size());
        }
    }

    @Test
    void verifyChecksEveryPartOfAFreshOrHeldDataset() {
        Adapter adapter = new Adapter();
        assertTrue(adapter.handle("{\"id\":\"1\",\"op\":\"hello\"}").contains("\"verify\""));
        assertTrue(adapter.handle(request("verify", "multipart", "1", "")).contains("\"status\":\"ok\""));
        String missing = "invalid/missing-part";
        assertTrue(adapter.handle(request("open", missing, "1", "")).contains("\"status\":\"ok\""));
        for (String op : new String[] {"range", "verify", "open", "verify"}) {
            String response = adapter.handle(request(op, missing, "100", ""));
            String want = op.equals("open") ? "\"status\":\"ok\"" : "\"code\":\"missing_part\"";
            assertTrue(response.contains(want), op + ": " + response);
        }
    }

    @Test
    void internalPagesPreserveLimitsBoundsProjectionAndTaggedContinuations() {
        Adapter adapter = new Adapter();
        Adapter paged = new Adapter(2);
        for (String op : new String[] {"range", "list"}) {
            for (String limit : new String[] {"1", "2", "3", "4", "5", "15", "24", "25"}) {
                for (String extra : new String[] {"", ",\"columns\":[\"size\"]",
                        ",\"prefix_base64\":\"YQ==\"", ",\"end_base64\":\"Yw==\"",
                        ",\"after\":{\"key_base64\":\"YQ==\",\"offset\":1}"}) {
                    String req = request(op, "repeated-keys", limit, extra);
                    String expected = adapter.handle(req);
                    assertTrue(expected.contains("\"status\":\"ok\""), expected);
                    assertEquals(expected, paged.handle(req), req);
                }
            }
        }
        String req = request("list", "delimiter", "100",
                ",\"after\":{\"prefix_base64\":\"YS8=\"}");
        assertEquals(new Adapter().handle(req), new Adapter(1).handle(req));
    }

    @Test
    void invalidLimitsStillFailBeforeOpeningDataset() {
        for (String op : new String[] {"range", "list"}) {
            for (String limit : new String[] {"0", "-1", "1.5", "1e3", "null", "\"1\""}) {
                String response = new Adapter().handle("{\"op\":\"" + op + "\",\"limit\":" + limit + "}");
                assertTrue(response.contains("\"status\":\"invalid_request\""), response);
                assertTrue(response.contains("limit"), response);
            }
        }
    }

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
