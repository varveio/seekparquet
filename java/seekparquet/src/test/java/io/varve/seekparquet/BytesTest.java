// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class BytesTest {

    @Test
    void comparesUnsignedLexicographically() {
        assertTrue(Bytes.of((byte) 0x7F).compareTo(Bytes.of((byte) 0x80)) < 0);
        assertTrue(Bytes.of((byte) 0xFF).compareTo(Bytes.of((byte) 0xFF, (byte) 0)) < 0);
        assertTrue(Bytes.empty().compareTo(Bytes.of((byte) 0)) < 0);
        assertEquals(0, Bytes.utf8("ab").compareTo(Bytes.of((byte) 'a', (byte) 'b')));
    }

    @Test
    void successorDropsTrailingFfAndIncrements() {
        assertEquals(Optional.of(Bytes.utf8("b")), Bytes.utf8("a").successor());
        assertEquals(Optional.of(Bytes.of((byte) 'b')), Bytes.of((byte) 'a', (byte) 0xFF, (byte) 0xFF).successor());
        assertEquals(Optional.empty(), Bytes.of((byte) 0xFF, (byte) 0xFF).successor());
        assertEquals(Optional.empty(), Bytes.empty().successor());
    }

    @Test
    void base64IsCanonicalOnly() {
        assertEquals(Bytes.utf8("a"), Bytes.fromBase64("YQ=="));
        assertEquals(Bytes.empty(), Bytes.fromBase64(""));
        assertThrows(IllegalArgumentException.class, () -> Bytes.fromBase64("YQ"));
        assertThrows(IllegalArgumentException.class, () -> Bytes.fromBase64("YR=="));
        assertThrows(IllegalArgumentException.class, () -> Bytes.fromBase64("Y_=="));
    }

    @Test
    void isImmutable() {
        byte[] source = {1, 2, 3};
        Bytes b = Bytes.of(source);
        source[0] = 9;
        b.toByteArray()[1] = 9;
        assertEquals(Bytes.of((byte) 1, (byte) 2, (byte) 3), b);
        assertTrue(b.startsWith(Bytes.of((byte) 1, (byte) 2)));
        assertFalse(b.startsWith(Bytes.of((byte) 1, (byte) 2, (byte) 3, (byte) 4)));
        assertEquals("0x010203", b.toString());
    }
}
