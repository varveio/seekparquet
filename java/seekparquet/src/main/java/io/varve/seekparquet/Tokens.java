// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Base64;
import java.util.Objects;

/** The cursor token, version 1 (spec/cursor-token-1.md). */
final class Tokens {

    private static final String VERSION = "spc1";
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private Tokens() {}

    static String encode(String tag, Bytes payload, String offset) {
        long n = VERSION.length() + 2L + tag.length() + (4L * payload.length() + 2) / 3 + (offset == null ? 0 : 1 + offset.length());
        if (n > Cursor.MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException("a cursor of " + payload.length() + " bytes makes a token beyond "
                    + Cursor.MAX_TOKEN_LENGTH + " characters");
        }
        StringBuilder b = new StringBuilder((int) n).append(VERSION).append('.').append(tag).append('.')
                .append(ENCODER.encodeToString(payload.unsafeArray()));
        if (offset != null) {
            b.append('.').append(offset);
        }
        return b.toString();
    }

    static Cursor parse(String token) {
        Objects.requireNonNull(token, "token");
        if (token.length() > Cursor.MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException("a cursor token of " + token.length() + " characters is beyond " + Cursor.MAX_TOKEN_LENGTH);
        }
        if (!token.startsWith(VERSION + ".")) {
            throw new IllegalArgumentException("not a version 1 cursor token");
        }
        int tagEnd = token.indexOf('.', VERSION.length() + 1);
        if (tagEnd < 0) {
            throw new IllegalArgumentException("a cursor token has no tag");
        }
        String tag = token.substring(VERSION.length() + 1, tagEnd);
        String rest = token.substring(tagEnd + 1);
        switch (tag) {
            case "r" -> {
                int dot = rest.indexOf('.');
                if (dot < 0) {
                    throw new IllegalArgumentException("a row cursor token has no offset");
                }
                Bytes key = bytes(rest.substring(0, dot));
                return new Cursor.RowPosition(key, offset(rest.substring(dot + 1)));
            }
            case "p" -> {
                return new Cursor.CommonPrefix(bytes(rest));
            }
            default -> throw new IllegalArgumentException("a cursor token's tag is unknown");
        }
    }

    /** Decodes canonical unpadded base64url. */
    private static Bytes bytes(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_')) {
                throw new IllegalArgumentException("a cursor token's bytes are not unpadded base64url");
            }
        }
        if (s.length() % 4 == 1) {
            throw new IllegalArgumentException("a cursor token's bytes have an impossible base64url length");
        }
        byte[] b = DECODER.decode(s);
        if (!ENCODER.encodeToString(b).equals(s)) {
            throw new IllegalArgumentException("a cursor token's bytes are not canonical base64url");
        }
        return Bytes.wrap(b);
    }

    /** Decodes a canonical decimal from 0 to 2^63-1. */
    private static long offset(String s) {
        if (s.isEmpty() || s.length() > 19 || (s.length() > 1 && s.charAt(0) == '0')) {
            throw new IllegalArgumentException("a cursor token's offset is not a canonical decimal");
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException("a cursor token's offset is not decimal digits");
            }
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("a cursor token's offset is beyond 2^63-1");
        }
    }
}
