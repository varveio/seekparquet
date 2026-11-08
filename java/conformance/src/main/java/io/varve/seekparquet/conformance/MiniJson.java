// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.conformance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small JSON reader and writer for the adapter protocol. Objects become {@link LinkedHashMap}s
 * (duplicate names are rejected), arrays {@link List}s, strings {@link String}s, numbers {@link
 * Number} tokens kept as their source text, booleans {@link Boolean}s, and null {@link #NULL}.
 */
final class MiniJson {

    /** JSON null, distinct from an absent member. */
    static final Object NULL = new Object() {
        @Override
        public String toString() {
            return "null";
        }
    };

    /** A number as written. */
    record Number(String text) {
        boolean isInteger() {
            return text.matches("-?(0|[1-9][0-9]*)");
        }
    }

    private final String in;
    private int pos;

    private MiniJson(String in) {
        this.in = in;
    }

    static Object parse(String text) {
        MiniJson p = new MiniJson(text);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.pos != text.length()) {
            throw new IllegalArgumentException("trailing data at " + p.pos);
        }
        return v;
    }

    private void ws() {
        while (pos < in.length() && " \t\r\n".indexOf(in.charAt(pos)) >= 0) {
            pos++;
        }
    }

    private Object value() {
        if (pos >= in.length()) {
            throw new IllegalArgumentException("unexpected end");
        }
        char c = in.charAt(pos);
        switch (c) {
            case '{' -> {
                pos++;
                Map<String, Object> m = new LinkedHashMap<>();
                ws();
                if (peek('}')) {
                    pos++;
                    return m;
                }
                while (true) {
                    ws();
                    String k = string();
                    ws();
                    expect(':');
                    ws();
                    if (m.put(k, value()) != null) {
                        throw new IllegalArgumentException("duplicate member " + k);
                    }
                    ws();
                    if (peek(',')) {
                        pos++;
                        continue;
                    }
                    expect('}');
                    return m;
                }
            }
            case '[' -> {
                pos++;
                List<Object> l = new ArrayList<>();
                ws();
                if (peek(']')) {
                    pos++;
                    return l;
                }
                while (true) {
                    ws();
                    l.add(value());
                    ws();
                    if (peek(',')) {
                        pos++;
                        continue;
                    }
                    expect(']');
                    return l;
                }
            }
            case '"' -> {
                return string();
            }
            case 't' -> {
                word("true");
                return Boolean.TRUE;
            }
            case 'f' -> {
                word("false");
                return Boolean.FALSE;
            }
            case 'n' -> {
                word("null");
                return NULL;
            }
            default -> {
                int start = pos;
                while (pos < in.length() && "+-0123456789.eE".indexOf(in.charAt(pos)) >= 0) {
                    pos++;
                }
                if (start == pos) {
                    throw new IllegalArgumentException("unexpected character at " + pos);
                }
                return new Number(in.substring(start, pos));
            }
        }
    }

    private boolean peek(char c) {
        return pos < in.length() && in.charAt(pos) == c;
    }

    private void expect(char c) {
        if (!peek(c)) {
            throw new IllegalArgumentException("expected '" + c + "' at " + pos);
        }
        pos++;
    }

    private void word(String w) {
        if (!in.startsWith(w, pos)) {
            throw new IllegalArgumentException("invalid literal at " + pos);
        }
        pos += w.length();
    }

    private String string() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= in.length()) {
                throw new IllegalArgumentException("unterminated string");
            }
            char c = in.charAt(pos++);
            if (c == '"') {
                return sb.toString();
            }
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            char e = in.charAt(pos++);
            switch (e) {
                case 'n' -> sb.append('\n');
                case 't' -> sb.append('\t');
                case 'r' -> sb.append('\r');
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'u' -> {
                    sb.append((char) Integer.parseInt(in.substring(pos, pos + 4), 16));
                    pos += 4;
                }
                default -> sb.append(e);
            }
        }
    }

    /** Appends {@code s} as a JSON string. */
    static void quote(StringBuilder out, String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20 || Character.isSurrogate(c)) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
