// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The cursor token, version 1 (spec/cursor-token-1.md), against the shared golden vectors that the Go
 * reader's tests read too: every valid token parses to its cursor and that cursor encodes to exactly the
 * token (a Go token in, the same Java token out), every invalid token is an invalid request, and every
 * unencodable cursor is refused.
 */
class CursorTokenTest {

    private static Map<String, Json.Node> vectors() throws Exception {
        byte[] raw = Files.readAllBytes(ConformanceFixturesTest.fixture("vectors/cursor-token-1.json"));
        return ((Json.Obj) Json.parse(raw)).members();
    }

    private static Map<String, Json.Node> members(Json.Node n) {
        return ((Json.Obj) n).members();
    }

    private static String string(Json.Node n) {
        return ((Json.Str) n).value();
    }

    @Test
    void validTokensRoundTrip() throws Exception {
        List<Json.Node> valid = ((Json.Arr) vectors().get("valid")).items();
        assertFalse(valid.isEmpty());
        for (Json.Node v : valid) {
            String token = string(members(v).get("token"));
            Map<String, Json.Node> want = members(members(v).get("cursor"));
            Cursor expected = want.containsKey("prefix_base64")
                    ? new Cursor.CommonPrefix(Bytes.fromBase64(string(want.get("prefix_base64"))))
                    : new Cursor.RowPosition(Bytes.fromBase64(string(want.get("key_base64"))), Long.parseLong(string(want.get("offset"))));
            Cursor parsed = Cursor.parse(token);
            assertEquals(expected, parsed, token);
            assertEquals(token, parsed.encode(), token);
            assertEquals(token, expected.encode(), token);
        }
    }

    @Test
    void invalidTokensAreInvalidRequests() throws Exception {
        List<Json.Node> invalid = ((Json.Arr) vectors().get("invalid")).items();
        assertFalse(invalid.isEmpty());
        for (Json.Node v : invalid) {
            String token = string(members(v).get("token"));
            assertThrows(IllegalArgumentException.class, () -> Cursor.parse(token), token);
        }
    }

    /**
     * The unencodable vectors are cursors no Java value can hold: neither or both of a key and a prefix,
     * and a prefix cursor with an offset, have no representation in the sealed alternatives; a negative
     * offset is refused when the row cursor is constructed.
     */
    @Test
    void unencodableCursorsCannotBeMade() throws Exception {
        List<Json.Node> unencodable = ((Json.Arr) vectors().get("unencodable")).items();
        assertEquals(4, unencodable.size());
        for (Json.Node v : unencodable) {
            Map<String, Json.Node> c = members(members(v).get("cursor"));
            boolean key = c.containsKey("key_base64");
            boolean prefix = c.containsKey("prefix_base64");
            if (key && !prefix) {
                long offset = Long.parseLong(string(c.get("offset")));
                assertTrue(offset < 0);
                Bytes k = Bytes.fromBase64(string(c.get("key_base64")));
                assertThrows(IllegalArgumentException.class, () -> new Cursor.RowPosition(k, offset));
            } else {
                // Neither, both, or a prefix with an offset: no constructor of Cursor takes such a shape.
                assertTrue(key == prefix || c.containsKey("offset"));
            }
        }
    }

    @Test
    void lengthIsBoundedBeforeDecoding() {
        String tooLong = "spc1.p." + "A".repeat(Cursor.MAX_TOKEN_LENGTH);
        assertThrows(IllegalArgumentException.class, () -> Cursor.parse(tooLong));
        // The longest payload that fits: 786,411 bytes or more encode within the bound whatever the offset.
        byte[] big = new byte[786_411];
        Arrays.fill(big, (byte) 0xFF);
        String token = new Cursor.RowPosition(Bytes.of(big), Long.MAX_VALUE).encode();
        assertTrue(token.length() <= Cursor.MAX_TOKEN_LENGTH);
        assertEquals(new Cursor.RowPosition(Bytes.of(big), Long.MAX_VALUE), Cursor.parse(token));
        byte[] huge = new byte[Cursor.MAX_TOKEN_LENGTH];
        assertThrows(IllegalArgumentException.class, () -> new Cursor.CommonPrefix(Bytes.of(huge)).encode());
    }

    @Test
    void resultsCarryCursors() {
        Row row = new Row(Bytes.utf8("a"), 3, List.of(), List.of());
        assertEquals(new Cursor.RowPosition(Bytes.utf8("a"), 3), row.cursor());
        assertEquals("spc1.r.YQ.3", row.cursor().encode());
        ListEntry prefix = new ListEntry.CommonPrefix(Bytes.utf8("a/"));
        assertEquals("spc1.p.YS8", prefix.cursor().encode());
        assertEquals(row.cursor(), new ListEntry.RowEntry(row).cursor());
    }
}
