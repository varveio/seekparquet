// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Structural checks of encoded streams, run before parquet-java's decoders see them. The decoders
 * allocate from counts the stream declares (a bit-packed run's group count, a delta block's value
 * count); these walks check the declared counts against what the page can hold, without decoding
 * any value, so that a hostile page cannot make a decoder allocate beyond the page's own size.
 */
final class EncodingChecks {

    private EncodingChecks() {}

    /**
     * Walks an RLE/bit-packed hybrid stream in {@code b[from, to)} holding at most {@code maxValues}
     * values of {@code bitWidth} bits, and checks that no run declares more values than remain (a
     * bit-packed run may pad its last group).
     */
    static void rleHybrid(byte[] b, int from, int to, int bitWidth, long maxValues) throws DataException {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new DataException("an RLE bit width of " + bitWidth);
        }
        int[] pos = {from};
        long values = 0;
        while (pos[0] < to) {
            long header = uleb(b, pos, to);
            long runs = header >>> 1;
            if (runs == 0 || values >= maxValues) {
                throw new DataException("an empty or excess RLE run");
            }
            boolean packed = (header & 1) != 0;
            long remaining = maxValues - values;
            if (packed ? runs > (remaining + 7) / 8 : runs > remaining) {
                throw new DataException("an RLE run exceeds the remaining value count");
            }
            long bytes = packed ? runs * bitWidth : (bitWidth + 7) / 8;
            if (bytes > to - pos[0]) {
                throw new DataException("a truncated RLE run body");
            }
            pos[0] += (int) bytes;
            values += packed ? runs * 8 : runs;
        }
        if (values < maxValues) {
            throw new DataException("an RLE stream holds fewer values than expected");
        }
    }

    /**
     * Walks {@code count} PLAIN BYTE_ARRAY values (each a 4-byte little-endian length and its bytes)
     * in {@code b[from, to)}, checking that every length lies inside the stream.
     */
    static void plainByteArrays(byte[] b, int from, int to, long count) throws DataException {
        long pos = from;
        for (long i = 0; i < count; i++) {
            if (pos + 4 > to) {
                throw new DataException("a truncated PLAIN BYTE_ARRAY length");
            }
            int p = (int) pos;
            long len = (b[p] & 0xFFL) | (b[p + 1] & 0xFFL) << 8 | (b[p + 2] & 0xFFL) << 16 | (b[p + 3] & 0xFFL) << 24;
            pos += 4 + len;
            if (pos > to) {
                throw new DataException("a PLAIN BYTE_ARRAY value of " + len + " bytes overruns its page");
            }
        }
    }

    /**
     * Walks a DELTA_BINARY_PACKED stream starting at {@code b[from]} and ending no later than {@code
     * to}, holding at most {@code maxValues} values, and returns the offset just after it.
     */
    static int deltaBinaryPacked(byte[] b, int from, int to, long maxValues, Limits limits, String where) throws DataException {
        int[] pos = {from};
        long blockSize = uleb(b, pos, to);
        long miniblocks = uleb(b, pos, to);
        long total = uleb(b, pos, to);
        skipVarint(b, pos, to); // first value, zigzag
        if (blockSize <= 0 || blockSize % 128 != 0 || miniblocks <= 0 || blockSize % miniblocks != 0
                || (blockSize / miniblocks) % 32 != 0) {
            throw new DataException("a DELTA_BINARY_PACKED header with block size " + blockSize + " and " + miniblocks + " miniblocks");
        }
        if (total != maxValues) {
            throw new DataException("a DELTA_BINARY_PACKED stream of " + total + " values where at most " + maxValues + " are expected");
        }
        long perMiniblock = blockSize / miniblocks;
        // parquet-java allocates a padded long buffer plus an int per miniblock, even
        // for a one-value stream. Bound these before its int arithmetic or allocation.
        long padded = total == 0 ? 1 : ((total - 1) / perMiniblock + 1) * perMiniblock + 1;
        long scratch = padded > Integer.MAX_VALUE || miniblocks > Integer.MAX_VALUE || blockSize > Integer.MAX_VALUE
                ? Long.MAX_VALUE : 32 + padded * Long.BYTES + miniblocks * Integer.BYTES;
        if (scratch > limits.maxPageBytes()) {
            throw new LimitExceededException("maxPageBytes", scratch, limits.maxPageBytes(), where + " (delta decoder scratch)");
        }
        long remaining = total - 1;
        while (remaining > 0) {
            skipVarint(b, pos, to); // min delta, zigzag
            if (pos[0] + miniblocks > to) {
                throw new DataException("a truncated DELTA_BINARY_PACKED block");
            }
            int widths = pos[0];
            pos[0] += (int) miniblocks;
            for (int m = 0; m < miniblocks && remaining > 0; m++) {
                int width = b[widths + m] & 0xFF;
                if (width > 64) {
                    throw new DataException("a DELTA_BINARY_PACKED bit width of " + width);
                }
                long bytes = width * perMiniblock / 8;
                if (pos[0] + bytes > to) {
                    throw new DataException("a truncated DELTA_BINARY_PACKED miniblock");
                }
                pos[0] += (int) bytes;
                remaining -= perMiniblock;
            }
        }
        return pos[0];
    }

    /** Skips a varint of at most 10 bytes, of any 64-bit value. */
    static void skipVarint(byte[] b, int[] pos, int to) throws DataException {
        for (int i = 0; i < 10; i++) {
            if (pos[0] >= to) {
                throw new DataException("a truncated varint");
            }
            if ((b[pos[0]++] & 0x80) == 0) {
                return;
            }
        }
        throw new DataException("a varint longer than 10 bytes");
    }

    /** Reads an unsigned LEB128 varint of at most 10 bytes, below 2^63. */
    static long uleb(byte[] b, int[] pos, int to) throws DataException {
        long v = 0;
        for (int shift = 0; shift < 70; shift += 7) {
            if (pos[0] >= to) {
                throw new DataException("a truncated varint");
            }
            int x = b[pos[0]++] & 0xFF;
            if (shift == 63 && (x & 0x7F) != 0) {
                throw new DataException("a varint above 2^63-1");
            }
            v |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) {
                if (v < 0) {
                    throw new DataException("a varint above 2^63");
                }
                return v;
            }
        }
        throw new DataException("a varint longer than 10 bytes");
    }
}
