// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable byte string: a key, a prefix, a bound, or a binary value.
 *
 * <p>Byte strings compare in unsigned lexicographic byte order, the order of every comparison in
 * the specification (§1): the first differing byte decides, compared as an unsigned integer, and a
 * proper prefix sorts before the strings it begins. Keys need not be UTF-8.
 *
 * <p>Instances are immutable and safe to share between threads. Factories copy their input, and
 * {@link #toByteArray()} returns a copy, so no caller can change a {@code Bytes} after creation.
 */
public final class Bytes implements Comparable<Bytes> {

    private static final Bytes EMPTY = new Bytes(new byte[0]);

    private final byte[] data;
    private int hash;

    private Bytes(byte[] data) {
        this.data = data;
    }

    /** {@return the empty byte string} */
    public static Bytes empty() {
        return EMPTY;
    }

    /**
     * Returns a byte string holding a copy of {@code bytes}.
     *
     * @param bytes the bytes to copy
     * @return the byte string
     */
    public static Bytes of(byte... bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return bytes.length == 0 ? EMPTY : new Bytes(bytes.clone());
    }

    /**
     * Returns a byte string holding a copy of {@code length} bytes of {@code bytes} from {@code
     * offset}.
     *
     * @param bytes the source array
     * @param offset the first byte to copy
     * @param length the number of bytes to copy
     * @return the byte string
     */
    public static Bytes of(byte[] bytes, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        return length == 0 ? EMPTY : new Bytes(Arrays.copyOfRange(bytes, offset, offset + length));
    }

    /**
     * Returns a byte string holding a copy of the remaining bytes of {@code buffer}, without moving
     * its position.
     *
     * @param buffer the source buffer
     * @return the byte string
     */
    public static Bytes of(ByteBuffer buffer) {
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return copy.length == 0 ? EMPTY : new Bytes(copy);
    }

    /**
     * Returns the UTF-8 encoding of {@code text}, for building keys from text. It is strict: text that is
     * not well-formed UTF-16 (an unpaired surrogate) is rejected rather than encoded with a replacement,
     * which would silently change the key's identity.
     *
     * @param text the text to encode
     * @return the byte string
     * @throws IllegalArgumentException if {@code text} holds an unpaired surrogate
     */
    public static Bytes utf8(String text) {
        try {
            java.nio.ByteBuffer b = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(text));
            byte[] out = new byte[b.remaining()];
            b.get(out);
            return wrap(out);
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new IllegalArgumentException("the text is not well-formed UTF-16: it holds an unpaired surrogate", e);
        }
    }

    /**
     * Decodes standard base64 with padding (RFC 4648 §4), accepting only the canonical spelling:
     * the alphabet {@code A-Z a-z 0-9 + /}, padding present, no whitespace, and zero unused bits,
     * so that re-encoding gives back {@code text}.
     *
     * @param text the base64 text
     * @return the decoded bytes
     * @throws IllegalArgumentException if {@code text} is not canonical padded base64
     */
    public static Bytes fromBase64(String text) {
        if (text.length() % 4 != 0) {
            throw new IllegalArgumentException("base64 text is not padded to a multiple of 4");
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(text);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not valid base64: " + e.getMessage());
        }
        if (!Base64.getEncoder().encodeToString(decoded).equals(text)) {
            throw new IllegalArgumentException("base64 text is not canonical");
        }
        return wrap(decoded);
    }

    /**
     * Wraps an array the caller gives up. Internal use only: the array must not be modified
     * afterwards. Reusable decoder storage must cross the owning boundary through {@code of},
     * never through this method.
     */
    static Bytes wrap(byte[] owned) {
        return owned.length == 0 ? EMPTY : new Bytes(owned);
    }

    /** {@return the number of bytes} */
    public int length() {
        return data.length;
    }

    /** {@return whether this byte string is empty} */
    public boolean isEmpty() {
        return data.length == 0;
    }

    /**
     * Returns the byte at {@code index}.
     *
     * @param index the position, from 0
     * @return the byte
     */
    public byte byteAt(int index) {
        return data[index];
    }

    /** {@return a copy of the bytes} */
    public byte[] toByteArray() {
        return data.clone();
    }

    /** {@return a read-only buffer over the bytes, without copying them} */
    public ByteBuffer asReadOnlyBuffer() {
        return ByteBuffer.wrap(data).asReadOnlyBuffer();
    }

    /**
     * Returns whether this byte string begins with {@code prefix}. Every byte string begins with the
     * empty string.
     *
     * @param prefix the prefix
     * @return whether this begins with it
     */
    public boolean startsWith(Bytes prefix) {
        int n = prefix.data.length;
        return n <= data.length && Arrays.equals(data, 0, n, prefix.data, 0, n);
    }

    /**
     * Returns the bytes from {@code from} (inclusive) to {@code to} (exclusive).
     *
     * @param from the first position
     * @param to the position after the last
     * @return the slice, as a new byte string
     */
    public Bytes slice(int from, int to) {
        Objects.checkFromToIndex(from, to, data.length);
        if (from == 0 && to == data.length) {
            return this;
        }
        return of(data, from, to - from);
    }

    /**
     * Returns the first position at or after {@code from} that holds {@code b}, or -1.
     *
     * @param b the byte to find
     * @param from the position to search from
     * @return its position, or -1
     */
    public int indexOf(byte b, int from) {
        for (int i = Math.max(from, 0); i < data.length; i++) {
            if (data[i] == b) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the end of the keys that begin with this prefix (the prefix successor of spec §1): the
     * least byte string greater than every string that begins with it. It is not the next exact key
     * (an exact key's span ends at the key followed by {@code 0x00}, {@link Span#key}). It is used as an
     * exclusive upper bound. It is this string with its trailing {@code 0xFF} bytes removed and
     * its last remaining byte incremented. It is empty when this string is empty or consists only of
     * {@code 0xFF} bytes: the strings beginning with it are then unbounded above.
     *
     * @return the successor, or empty when there is none
     */
    public Optional<Bytes> prefixEnd() {
        int end = data.length;
        while (end > 0 && data[end - 1] == (byte) 0xFF) {
            end--;
        }
        if (end == 0) {
            return Optional.empty();
        }
        byte[] next = Arrays.copyOf(data, end);
        next[end - 1]++;
        return Optional.of(new Bytes(next));
    }

    /** {@return the bytes as canonical standard base64 with padding} */
    public String toBase64() {
        return Base64.getEncoder().encodeToString(data);
    }

    /** {@return the bytes as lowercase hexadecimal} */
    public String toHex() {
        return HexFormat.of().formatHex(data);
    }

    /**
     * {@return the bytes decoded as UTF-8, lossily: malformed input is replaced with U+FFFD} For display
     * only: a key's identity is its bytes, and the text may not encode back to them.
     */
    public String toStringUtf8() {
        return new String(data, StandardCharsets.UTF_8);
    }

    /** Compares in unsigned lexicographic byte order. */
    @Override
    public int compareTo(Bytes other) {
        return Arrays.compareUnsigned(data, other.data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Bytes other && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && data.length > 0) {
            h = Arrays.hashCode(data);
            hash = h;
        }
        return h;
    }

    /**
     * Returns the bytes as printable ASCII when they all are, and as {@code 0x}-prefixed hexadecimal
     * otherwise.
     */
    @Override
    public String toString() {
        for (byte b : data) {
            if (b < 0x20 || b > 0x7E) {
                return "0x" + toHex();
            }
        }
        return '"' + new String(data, StandardCharsets.US_ASCII) + '"';
    }

    /** The backing array, for this package's own use; never modify it. */
    byte[] unsafeArray() {
        return data;
    }
}
