// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
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
        } catch (RuntimeException | IOException e) {
            throw new DataException("the page does not decompress (" + codec + ")", e);
        }
    }

    private static int gunzip(byte[] in, int off, int len, byte[] out, int size) throws IOException, DataException {
        try (GZIPInputStream z = new GZIPInputStream(new ByteArrayInputStream(in, off, len), 8192)) {
            int n = z.readNBytes(out, 0, size);
            if (z.read() >= 0) {
                throw new DataException("the GZIP page decompresses to more than the " + size + " bytes it declares");
            }
            return n;
        }
    }
}
