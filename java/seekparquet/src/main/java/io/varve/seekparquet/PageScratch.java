// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import org.apache.parquet.column.Dictionary;

/**
 * One request's storage for one projected column. Capacities survive row groups and parts; logical
 * pages and dictionaries do not. Never used for keys, shared across requests, or retained by results.
 * Primitive arrays grow to the checked requirement; binary arena growth is capped by maxPageBytes.
 */
final class PageScratch {
    byte[] plain = new byte[0];
    byte[] levels = new byte[0];
    byte[] arena = new byte[0];
    boolean[] nulls = new boolean[0];
    boolean[] bools = new boolean[0];
    int[] ints = new int[0];
    long[] longs = new long[0];
    int[] offsets = new int[0];
    int[] lengths = new int[0];
    byte[][] binaries = new byte[0][];
    Dictionary dictionary;
    byte[][] entries;
    // A lazy dictionary decode must not overwrite the active data page's plain/levels/arena.
    byte[] dictionaryPlain = new byte[0];

    void enterGroup() {
        dictionary = null;
        entries = null;
        Arrays.fill(binaries, null);
    }

    static byte[] grow(byte[] a, int n) { return a.length < n ? new byte[n] : a; }
    static boolean[] grow(boolean[] a, int n) { return a.length < n ? new boolean[n] : a; }
    static int[] grow(int[] a, int n) { return a.length < n ? new int[n] : a; }
    static long[] grow(long[] a, int n) { return a.length < n ? new long[n] : a; }

    byte[][] binaries(int n) {
        if (binaries.length < n) binaries = new byte[n][];
        else Arrays.fill(binaries, null);
        return binaries;
    }

    byte[][] entries(Dictionary d) {
        if (dictionary != d) {
            dictionary = d;
            entries = new byte[d.getMaxId() + 1][];
        }
        return entries;
    }
}
