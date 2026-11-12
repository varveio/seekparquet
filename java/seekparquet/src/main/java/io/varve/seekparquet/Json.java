// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A strict RFC 8259 JSON parser for the manifest. It rejects duplicate member names (compared after
 * decoding escapes), unpaired surrogate escapes, unescaped control characters, and anything after
 * the single top-level value but whitespace. Numbers keep their source spelling, and every value
 * records its byte span, so extensions can be exposed exactly as written.
 */
final class Json {

    /** The most nested arrays and objects accepted. */
    static final int MAX_DEPTH = 512;

    /** A parsed value and its span {@code [start, end)} in the source bytes. */
    sealed interface Node {
        int start();

        int end();
    }

    record Obj(Map<String, Node> members, int start, int end) implements Node {}

    record Arr(List<Node> items, int start, int end) implements Node {}

    record Str(String value, int start, int end) implements Node {}

    /** A number, as spelled in the source. */
    record Num(String text, int start, int end) implements Node {}

    record Bool(boolean value, int start, int end) implements Node {}

    record Null(int start, int end) implements Node {}

    /** Malformed JSON. */
    static final class SyntaxException extends Exception {
        private static final long serialVersionUID = 1L;

        SyntaxException(String message, int at) {
            super(message + " at byte " + at);
        }
    }

    private final byte[] in;
    private int pos;

    private Json(byte[] in) {
        this.in = in;
    }

    /**
     * Parses {@code in}, which must already be valid UTF-8.
     *
     * @throws SyntaxException if it is not one strict JSON value
     * @throws LimitExceededException if it nests deeper than {@link #MAX_DEPTH}
     */
    static Node parse(byte[] in) throws SyntaxException {
        Json p = new Json(in);
        p.skipWhitespace();
        Node value = p.value(0);
        p.skipWhitespace();
        if (p.pos != in.length) {
            throw new SyntaxException("unexpected data after the JSON value", p.pos);
        }
        return value;
    }

    private void skipWhitespace() {
        while (pos < in.length) {
            byte b = in[pos];
            if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
                pos++;
            } else {
                break;
            }
        }
    }

    private Node value(int depth) throws SyntaxException {
        if (pos >= in.length) {
            throw new SyntaxException("unexpected end of input", pos);
        }
        byte b = in[pos];
        return switch (b) {
            case '{' -> object(depth + 1);
            case '[' -> array(depth + 1);
            case '"' -> string();
            case 't' -> literal("true", new Bool(true, pos, pos + 4));
            case 'f' -> literal("false", new Bool(false, pos, pos + 5));
            case 'n' -> literal("null", new Null(pos, pos + 4));
            default -> {
                if (b == '-' || (b >= '0' && b <= '9')) {
                    yield number();
                }
                throw new SyntaxException("unexpected character", pos);
            }
        };
    }

    private Node literal(String word, Node node) throws SyntaxException {
        int n = word.length();
        if (pos + n > in.length) {
            throw new SyntaxException("invalid literal", pos);
        }
        for (int i = 0; i < n; i++) {
            if (in[pos + i] != word.charAt(i)) {
                throw new SyntaxException("invalid literal", pos);
            }
        }
        pos += n;
        return node;
    }

    private void checkDepth(int depth) {
        if (depth > MAX_DEPTH) {
            throw new LimitExceededException(LimitName.MANIFEST_NESTING, depth, MAX_DEPTH, "manifest.json");
        }
    }

    private Obj object(int depth) throws SyntaxException {
        checkDepth(depth);
        int start = pos++;
        Map<String, Node> members = new LinkedHashMap<>();
        skipWhitespace();
        if (pos < in.length && in[pos] == '}') {
            pos++;
            return new Obj(members, start, pos);
        }
        while (true) {
            skipWhitespace();
            if (pos >= in.length || in[pos] != '"') {
                throw new SyntaxException("expected a member name", pos);
            }
            int nameAt = pos;
            String name = string().value();
            skipWhitespace();
            if (pos >= in.length || in[pos] != ':') {
                throw new SyntaxException("expected ':'", pos);
            }
            pos++;
            skipWhitespace();
            Node v = value(depth);
            if (members.putIfAbsent(name, v) != null) {
                throw new SyntaxException("duplicate member name \"" + name + "\"", nameAt);
            }
            skipWhitespace();
            if (pos >= in.length) {
                throw new SyntaxException("unterminated object", pos);
            }
            if (in[pos] == ',') {
                pos++;
                continue;
            }
            if (in[pos] == '}') {
                pos++;
                return new Obj(members, start, pos);
            }
            throw new SyntaxException("expected ',' or '}'", pos);
        }
    }

    private Arr array(int depth) throws SyntaxException {
        checkDepth(depth);
        int start = pos++;
        List<Node> items = new ArrayList<>();
        skipWhitespace();
        if (pos < in.length && in[pos] == ']') {
            pos++;
            return new Arr(items, start, pos);
        }
        while (true) {
            skipWhitespace();
            items.add(value(depth));
            skipWhitespace();
            if (pos >= in.length) {
                throw new SyntaxException("unterminated array", pos);
            }
            if (in[pos] == ',') {
                pos++;
                continue;
            }
            if (in[pos] == ']') {
                pos++;
                return new Arr(items, start, pos);
            }
            throw new SyntaxException("expected ',' or ']'", pos);
        }
    }

    private Str string() throws SyntaxException {
        int start = pos++;
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= in.length) {
                throw new SyntaxException("unterminated string", start);
            }
            int b = in[pos] & 0xFF;
            if (b == '"') {
                pos++;
                return new Str(sb.toString(), start, pos);
            }
            if (b < 0x20) {
                throw new SyntaxException("unescaped control character in a string", pos);
            }
            if (b == '\\') {
                pos++;
                if (pos >= in.length) {
                    throw new SyntaxException("unterminated escape", pos);
                }
                byte e = in[pos++];
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        char c = hex4();
                        if (Character.isHighSurrogate(c)) {
                            if (pos + 1 < in.length && in[pos] == '\\' && in[pos + 1] == 'u') {
                                pos += 2;
                                char low = hex4();
                                if (!Character.isLowSurrogate(low)) {
                                    throw new SyntaxException("unpaired surrogate escape", pos);
                                }
                                sb.append(c).append(low);
                            } else {
                                throw new SyntaxException("unpaired surrogate escape", pos);
                            }
                        } else if (Character.isLowSurrogate(c)) {
                            throw new SyntaxException("unpaired surrogate escape", pos);
                        } else {
                            sb.append(c);
                        }
                    }
                    default -> throw new SyntaxException("invalid escape", pos - 1);
                }
                continue;
            }
            // Copy one UTF-8 sequence (the input is valid UTF-8).
            int len = b < 0x80 ? 1 : b < 0xE0 ? 2 : b < 0xF0 ? 3 : 4;
            sb.append(new String(in, pos, len, java.nio.charset.StandardCharsets.UTF_8));
            pos += len;
        }
    }

    private char hex4() throws SyntaxException {
        if (pos + 4 > in.length) {
            throw new SyntaxException("truncated \\u escape", pos);
        }
        int v = 0;
        for (int i = 0; i < 4; i++) {
            int d = Character.digit(in[pos + i], 16);
            if (d < 0) {
                throw new SyntaxException("invalid \\u escape", pos);
            }
            v = (v << 4) | d;
        }
        pos += 4;
        return (char) v;
    }

    private Num number() throws SyntaxException {
        int start = pos;
        if (in[pos] == '-') {
            pos++;
        }
        if (pos >= in.length) {
            throw new SyntaxException("truncated number", start);
        }
        if (in[pos] == '0') {
            pos++;
        } else if (in[pos] >= '1' && in[pos] <= '9') {
            while (pos < in.length && in[pos] >= '0' && in[pos] <= '9') {
                pos++;
            }
        } else {
            throw new SyntaxException("invalid number", start);
        }
        if (pos < in.length && in[pos] == '.') {
            pos++;
            int digits = pos;
            while (pos < in.length && in[pos] >= '0' && in[pos] <= '9') {
                pos++;
            }
            if (pos == digits) {
                throw new SyntaxException("invalid number", start);
            }
        }
        if (pos < in.length && (in[pos] == 'e' || in[pos] == 'E')) {
            pos++;
            if (pos < in.length && (in[pos] == '+' || in[pos] == '-')) {
                pos++;
            }
            int digits = pos;
            while (pos < in.length && in[pos] >= '0' && in[pos] <= '9') {
                pos++;
            }
            if (pos == digits) {
                throw new SyntaxException("invalid number", start);
            }
        }
        return new Num(new String(in, start, pos - start, java.nio.charset.StandardCharsets.US_ASCII), start, pos);
    }
}
