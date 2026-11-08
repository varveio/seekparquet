// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/** Strict UTF-8 validation: shortest forms only, no surrogates, nothing above U+10FFFF. */
final class Utf8 {

    private Utf8() {}

    static boolean isValid(byte[] b) {
        return isValid(b, 0, b.length);
    }

    static boolean isValid(byte[] b, int from, int to) {
        int i = from;
        while (i < to) {
            int c = b[i] & 0xFF;
            if (c < 0x80) {
                i++;
                continue;
            }
            int n;
            int min;
            if (c >= 0xC2 && c <= 0xDF) {
                n = 1;
                min = 0x80;
            } else if (c >= 0xE0 && c <= 0xEF) {
                n = 2;
                min = 0x800;
            } else if (c >= 0xF0 && c <= 0xF4) {
                n = 3;
                min = 0x10000;
            } else {
                return false;
            }
            if (i + n >= to) {
                return false;
            }
            int cp = c & (0x3F >> n);
            for (int k = 1; k <= n; k++) {
                int d = b[i + k] & 0xFF;
                if ((d & 0xC0) != 0x80) {
                    return false;
                }
                cp = (cp << 6) | (d & 0x3F);
            }
            if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
                return false;
            }
            i += n + 1;
        }
        return true;
    }
}
