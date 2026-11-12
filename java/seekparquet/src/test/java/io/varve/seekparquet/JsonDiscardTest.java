// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * The discard path that checks excess {@code parts} entries without building them accepts and refuses
 * exactly what the building path does, with the same error: a seeded differential over generated JSON with
 * numbers, escapes, surrogates, duplicate names, nesting near the limit, truncation and trailing data.
 */
class JsonDiscardTest {

    private static final String[] NUMBERS = {"0", "-0", "12", "-3.25", "1e9", "2E-3", "0.5e+2", "01", "-", "1.", "1e", "+1", ".5", "1.e3", "00"};
    private static final String[] STRINGS = {"\"\"", "\"a\"", "\"\\n\\t\\\\\\\"\\/\"", "\"\\u00e9\"", "\"\\ud83d\\ude00\"", "\"\\ud800\"",
        "\"\\udc00\"", "\"\\ud800x\"", "\"\\q\"", "\"\\u12\"", "\"\\u12g4\"", "\"\u00e9\u20ac\"", "\"\u0001\"", "\"open", "\"\\"};
    private static final String[] LITERALS = {"true", "false", "null", "tru", "nul", "falsey", "True"};

    private static String value(Random r, int depth) {
        if (depth > 0 && r.nextInt(8) == 0) {
            return "[".repeat(depth) + "]".repeat(depth);
        }
        return switch (r.nextInt(depth > 6 ? 3 : 5)) {
            case 0 -> NUMBERS[r.nextInt(NUMBERS.length)];
            case 1 -> STRINGS[r.nextInt(STRINGS.length)];
            case 2 -> LITERALS[r.nextInt(LITERALS.length)];
            case 3 -> {
                StringBuilder b = new StringBuilder("[");
                int n = r.nextInt(4);
                for (int i = 0; i < n; i++) b.append(i == 0 ? "" : r.nextInt(20) == 0 ? ",," : ",").append(value(r, depth + 1));
                if (r.nextInt(20) == 0) b.append(',');
                yield b.append(r.nextInt(30) == 0 ? "" : "]").toString();
            }
            default -> {
                StringBuilder b = new StringBuilder("{");
                int n = r.nextInt(4);
                for (int i = 0; i < n; i++) {
                    String name = r.nextInt(6) == 0 ? "\"k\"" : STRINGS[r.nextInt(5)].replace("\\ud83d\\ude00", "x" + i);
                    b.append(i == 0 ? "" : ",").append(r.nextInt(25) == 0 ? "k" : name).append(r.nextInt(25) == 0 ? "" : ":").append(value(r, depth + 1));
                }
                yield b.append(r.nextInt(30) == 0 ? "" : "}").toString();
            }
        };
    }

    private static String outcome(byte[] raw, int keep) {
        try {
            Json.parse(raw, keep);
            return "ok";
        } catch (Json.SyntaxException e) {
            return "syntax: " + e.getMessage();
        } catch (LimitExceededException e) {
            return "limit: " + e.limit() + " " + e.declared();
        }
    }

    @Test
    void discardAndBuildAgree() {
        Random r = new Random(1112);
        int accepted = 0;
        int refused = 0;
        for (int i = 0; i < 20_000; i++) {
            String entry = value(r, 0);
            if (r.nextInt(10) == 0) entry = entry.substring(0, r.nextInt(entry.length() + 1));
            String deep = r.nextInt(50) == 0 ? "[".repeat(509) + entry + "]".repeat(509) : entry;
            String text = "{\"a\":1,\"parts\":[{\"p\":1}," + deep + "]" + (r.nextInt(40) == 0 ? "x" : "") + "}";
            byte[] raw = text.getBytes(StandardCharsets.UTF_8);
            if (!Utf8.isValid(raw)) continue;
            String built = outcome(raw, Integer.MAX_VALUE);
            String discarded = outcome(raw, 1);
            assertEquals(built, discarded, text);
            if (built.equals("ok")) accepted++; else refused++;
        }
        assertTrue(accepted > 1000 && refused > 1000, accepted + " accepted, " + refused + " refused");
    }
}
