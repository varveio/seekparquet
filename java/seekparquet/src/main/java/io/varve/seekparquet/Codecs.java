// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.apache.parquet.format.CompressionCodec;

/**
 * Page decompression. The baseline codecs (ENC-1) are UNCOMPRESSED, SNAPPY, GZIP and ZSTD; LZ4_RAW is
 * also read. Every decode is bounded to exactly the size the page header declares, which the caller has
 * checked against its limits, so a hostile page cannot expand into a larger retained output capacity.
 */
final class Codecs {

    private Codecs() {}

    /** Mutable codec state, confined to one request. Construct only codecs actually used. */
    static final class State {
        private SnappyDecompressor snappy;
        private ZstdDecompressor zstd;
        private Lz4Decompressor lz4;
        final java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        SnappyDecompressor snappy() { return snappy == null ? (snappy = new SnappyDecompressor()) : snappy; }
        ZstdDecompressor zstd() { return zstd == null ? (zstd = new ZstdDecompressor()) : zstd; }
        Lz4Decompressor lz4() { return lz4 == null ? (lz4 = new Lz4Decompressor()) : lz4; }
    }

    /** Throws if this reader cannot decompress {@code codec}. */
    static void checkSupported(CompressionCodec codec, String where) {
        if (codec == null) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, where + ": an unknown codec");
        }
        switch (codec) {
            case UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4_RAW -> {}
            default -> throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, where + ": codec " + codec);
        }
    }

    /**
     * Decompresses {@code input} (its remaining bytes) to exactly {@code size} bytes.
     *
     * @throws DataException if the input does not decompress to exactly that size
     */
    static byte[] decompress(CompressionCodec codec, ByteBuffer input, int size, byte[] out, State state) throws DataException {
        if (size < 0 || size > out.length) throw new IllegalArgumentException("output capacity");
        byte[] in;
        int off;
        int len = input.remaining();
        if (input.hasArray()) {
            in = input.array();
            off = input.arrayOffset() + input.position();
        } else {
            in = new byte[len];
            input.duplicate().get(in);
            off = 0;
        }
        try {
            int n = switch (codec) {
                case UNCOMPRESSED -> {
                    if (len != size) {
                        throw new DataException("an uncompressed page of " + len + " bytes declares " + size);
                    }
                    System.arraycopy(in, off, out, 0, size);
                    yield size;
                }
                case SNAPPY -> {
                    int declared = SnappyDecompressor.getUncompressedLength(in, off);
                    if (declared != size) {
                        throw new DataException("a SNAPPY block of " + declared + " bytes in a page that declares " + size);
                    }
                    yield state.snappy().decompress(in, off, len, out, 0, size);
                }
                case ZSTD -> state.zstd().decompress(in, off, len, out, 0, size);
                case LZ4_RAW -> state.lz4().decompress(in, off, len, out, 0, size);
                case GZIP -> gunzip(in, off, len, out, size);
                default -> throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, "codec " + codec);
            };
            if (n != size) {
                throw new DataException("the page decompresses to " + n + " bytes, not the " + size + " it declares");
            }
            return out;
        } catch (DataException | UnsupportedFeatureException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new DataException("the page does not decompress (" + codec + ")", e);
        }
    }

    private static final int FHCRC = 2;
    private static final int FEXTRA = 4;
    private static final int FNAME = 8;
    private static final int FCOMMENT = 16;

    /**
     * Decompresses a GZIP page, which must be exactly one gzip member (RFC 1952): its header, one
     * deflate stream and its CRC-32 and size trailer, with no byte after it. Parquet writers write one
     * member per page, and the Go reader refuses anything after it: a second member or other trailing
     * bytes, which {@code GZIPInputStream} would read or ignore.
     */
    static int gunzip(byte[] in, int off, int len, byte[] out, int size) throws DataException {
        int end = off + len;
        int p = off;
        if (len < 18 || (in[p] & 0xFF) != 0x1F || (in[p + 1] & 0xFF) != 0x8B || in[p + 2] != 8) {
            throw new DataException("the GZIP page is not a gzip member");
        }
        int flags = in[p + 3] & 0xFF;
        p += 10;
        if ((flags & FEXTRA) != 0) {
            if (end - p < 2) {
                throw new DataException("a truncated gzip header");
            }
            p += 2 + ((in[p] & 0xFF) | (in[p + 1] & 0xFF) << 8);
        }
        for (int field : new int[] {FNAME, FCOMMENT}) {
            if ((flags & field) != 0) {
                while (p < end && in[p] != 0) {
                    p++;
                }
                p++; // the terminating zero
            }
        }
        if ((flags & FHCRC) != 0) {
            if (p + 2 > end) {
                throw new DataException("a truncated gzip header");
            }
            CRC32 crc = new CRC32();
            crc.update(in, off, p - off);
            if (((in[p] & 0xFF) | (in[p + 1] & 0xFF) << 8) != (int) (crc.getValue() & 0xFFFF)) {
                throw new DataException("the gzip header fails its checksum");
            }
            p += 2;
        }
        if (p > end) {
            throw new DataException("a truncated gzip header");
        }
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(in, p, end - p);
            int n = 0;
            byte[] probe = new byte[1];
            while (!inflater.finished()) {
                int k = n < size ? inflater.inflate(out, n, size - n) : inflater.inflate(probe);
                if (k > 0 && n == size) {
                    throw new DataException("the GZIP page decompresses to more than the " + size + " bytes it declares");
                }
                n += k;
                if (k == 0 && !inflater.finished() && (inflater.needsInput() || inflater.needsDictionary())) {
                    throw new DataException("a truncated GZIP page");
                }
            }
            int trailer = end - inflater.getRemaining();
            if (inflater.getRemaining() != 8) {
                throw new DataException(inflater.getRemaining() < 8 ? "a truncated gzip trailer"
                        : "the GZIP page has " + (inflater.getRemaining() - 8) + " bytes after its gzip member");
            }
            CRC32 crc = new CRC32();
            crc.update(out, 0, n);
            if (le32(in, trailer) != (int) crc.getValue() || le32(in, trailer + 4) != n) {
                throw new DataException("the GZIP page fails its CRC-32 or size check");
            }
            return n;
        } catch (DataFormatException e) {
            throw new DataException("the GZIP page does not inflate", e);
        } finally {
            inflater.end();
        }
    }

    private static int le32(byte[] b, int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8 | (b[at + 2] & 0xFF) << 16 | (b[at + 3] & 0xFF) << 24;
    }
}
