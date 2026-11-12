// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.Spec;
import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.PartSource;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.parquet.format.FileMetaData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A part's footer is read in one read of the part's end (the last 64 KiB, the whole part when smaller, never
 * more than {@code maxFooterBytes} plus the trailer), and a footer larger than that window in one more read
 * of exactly the bytes before it. Both magics are checked (CHECK-1): a part larger than the window costs one
 * more read, of its 4-byte leading magic. Each case states its exact reads and bytes; every outcome on a
 * malformed part (refusal code and message, unsupported part, limit, success) is the one the reader gave
 * before the window was bounded, with the leading magic checked too, checked against a copy of that reader's
 * footer read.
 */
class FooterWindowTest {

    private static final int W = PartFile.TAIL_WINDOW;

    /** A part smaller than the window, one larger whose footer fits it, and one whose footer does not. */
    private static byte[] small, large, huge;

    @BeforeAll
    static void parts() throws IOException {
        Spec spec = new Spec();
        spec.rowsPerPage = 50;
        spec.rowsPerGroup = 100;
        small = TestDatasets.part(spec, rows(300));
        spec = new Spec();
        spec.rowsPerPage = 500;
        spec.rowsPerGroup = 6000;
        large = TestDatasets.part(spec, rows(6000));
        spec = new Spec();
        spec.rowsPerPage = 1;
        spec.rowsPerGroup = 1;
        huge = TestDatasets.part(spec, rows(1500));
        assertTrue(small.length < W, "small part: " + small.length);
        assertTrue(large.length > W && footerLength(large) + 8 < 3000, "large part: " + large.length + ", footer " + footerLength(large));
        assertTrue(footerLength(huge) + 8 > W, "huge footer: " + footerLength(huge));
    }

    private static List<TestRow> rows(int n) {
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            rows.add(TestRow.of(String.format("dir%03d/file%06d", i / 50, i), i));
        }
        return rows;
    }

    private static int footerLength(byte[] raw) {
        return ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getInt(raw.length - 8);
    }

    private static FileMetaData exactFooter(byte[] raw) throws DataException {
        int n = footerLength(raw);
        return Thrift.fileMetaData(Arrays.copyOfRange(raw, raw.length - 8 - n, raw.length - 8));
    }

    /** {@return {@code raw} with {@code n} zero bytes inserted before its footer: a larger part with the same footer} */
    private static byte[] grow(byte[] raw, int n) {
        int start = raw.length - 8 - footerLength(raw);
        byte[] out = new byte[raw.length + n];
        System.arraycopy(raw, 0, out, 0, start);
        System.arraycopy(raw, start, out, start + n, raw.length - start);
        return out;
    }

    /**
     * {@return {@code raw} with its footer padded to exactly {@code length} bytes} by an unknown binary field
     * (id 100), which decoding skips, so the footer decodes as before.
     */
    private static byte[] padFooter(byte[] raw, int length) {
        int n = footerLength(raw);
        int start = raw.length - 8 - n;
        for (int v = 1; v <= 5; v++) {
            int pad = length - (n - 1) - 3 - v - 1;
            if (pad < 0 || varint(pad).length != v) {
                continue;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(raw, 0, start + n - 1); // the footer without its stop byte
            out.write(new byte[] {0x08, (byte) 0xC8, 0x01}, 0, 3); // binary field 100 (zigzag 200)
            out.writeBytes(varint(pad));
            out.writeBytes(new byte[pad]);
            out.write(0); // stop
            out.writeBytes(le32(length));
            out.writeBytes("PAR1".getBytes(StandardCharsets.US_ASCII));
            byte[] padded = out.toByteArray();
            assertEquals(length, footerLength(padded));
            return padded;
        }
        throw new AssertionError("cannot pad a " + n + "-byte footer to " + length);
    }

    private static byte[] varint(int v) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((v & ~0x7F) != 0) {
            out.write((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        out.write(v);
        return out.toByteArray();
    }

    private static byte[] le32(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    /** A part over {@code raw}, whose buffers share its array. */
    private static PartSource source(byte[] raw) {
        return new PartSource() {
            @Override public String path() { return "p"; }
            @Override public long size() { return raw.length; }
            @Override public Optional<String> version() { return Optional.empty(); }
            @Override public void close() { }
            @Override public ByteBuffer read(long position, int length, Cancellation c) throws IOException {
                if (position < 0 || length < 0 || position > raw.length - length) {
                    throw new EOFException("read outside the part");
                }
                return ByteBuffer.wrap(raw, (int) position, length).asReadOnlyBuffer();
            }
        };
    }

    private static Limits limits(int maxFooterBytes) {
        Limits d = Limits.DEFAULT;
        return new Limits(d.maxManifestBytes(), d.maxParts(), maxFooterBytes, d.maxIndexBytes(), d.maxPageBytes(), d.maxPageValues(),
                d.maxResultEntries());
    }

    private record Case(String name, byte[] raw, Limits limits, long reads, long bytes) {}

    @Test
    void readsAndBytesOfEachPath() throws Exception {
        byte[] boundary = grow(padFooter(large, W - 8), 1000); // footer and trailer exactly fill the window
        byte[] past = grow(padFooter(large, W - 7), 1000);     // one byte of the footer is before the window
        List<Case> cases = List.of(
                new Case("small part, read whole", small, Limits.DEFAULT, 1, small.length),
                new Case("part of exactly the window", grow(small, W - small.length), Limits.DEFAULT, 1, W),
                // Larger than the window: one more read, of the 4-byte leading magic.
                new Case("part one byte over the window", grow(small, W + 1 - small.length), Limits.DEFAULT, 2, W + 4),
                new Case("large part, footer in the window", large, Limits.DEFAULT, 2, W + 4),
                new Case("footer and trailer exactly the window", boundary, Limits.DEFAULT, 2, W + 4),
                new Case("footer one byte over the window", past, Limits.DEFAULT, 3, W + 1 + 4),
                new Case("footer larger than the window", huge, Limits.DEFAULT, 3, footerLength(huge) + 8L + 4),
                new Case("window bounded by maxFooterBytes", large, limits(3000), 2, 3008 + 4),
                new Case("window bounded to exactly the footer", large, limits(footerLength(large)), 2, footerLength(large) + 8L + 4));
        for (Case c : cases) {
            ReadContext ctx = new ReadContext(Cancellation.none(), c.limits);
            FileMetaData meta = PartFile.readFooter(source(c.raw), "p", c.raw.length, ctx);
            assertEquals(c.reads, ctx.stats.sourceReads, c.name + ": reads");
            assertEquals(c.bytes, ctx.stats.sourceBytes, c.name + ": bytes");
            assertEquals(exactFooter(c.raw), meta, c.name + ": the footer differs from the exact footer bytes'");
            assertEquals(exactFooter(c.raw), previousReadFooter(source(c.raw), c.raw.length, new ReadContext(Cancellation.none(), c.limits)),
                    c.name + ": the previous read's footer");
        }
    }

    /** Every outcome on malformed, truncated or garbage-tailed parts is the previous reader's. */
    @Test
    void malformedPartsEndAsBefore() {
        Map<String, byte[]> inputs = new LinkedHashMap<>();
        Random random = new Random(58);
        for (var e : Map.of("small", small, "large", large, "huge", huge).entrySet()) {
            String name = e.getKey();
            byte[] raw = e.getValue();
            int n = raw.length;
            inputs.put(name, raw);
            inputs.put(name + ", PARE tail", edit(raw, n - 4, "PARE".getBytes(StandardCharsets.US_ASCII)));
            inputs.put(name + ", PAR2 tail", edit(raw, n - 4, "PAR2".getBytes(StandardCharsets.US_ASCII)));
            inputs.put(name + ", PARE head", edit(raw, 0, "PARE".getBytes(StandardCharsets.US_ASCII)));
            inputs.put(name + ", PAR2 head", edit(raw, 0, "PAR2".getBytes(StandardCharsets.US_ASCII)));
            for (long length : new long[] {0, 1, footerLength(raw) - 1, footerLength(raw) + 1, n - 12, n - 11, Math.min(n - 12, W + 100),
                    W - 8, W - 7, 0xFFFFFFFFL}) {
                inputs.put(name + ", footer length " + length, edit(raw, n - 8, le32((int) length)));
            }
            for (int i = 0; i < 8; i++) {
                byte[] garbage = new byte[1 + random.nextInt(4 * W)];
                random.nextBytes(garbage);
                int at = Math.max(0, n - garbage.length);
                inputs.put(name + ", garbage tail " + i, edit(raw, at, Arrays.copyOf(garbage, n - at)));
                byte[] keepMagic = edit(raw, at, Arrays.copyOf(garbage, n - at));
                System.arraycopy("PAR1".getBytes(StandardCharsets.US_ASCII), 0, keepMagic, n - 4, 4);
                inputs.put(name + ", garbage before PAR1 " + i, keepMagic);
            }
            for (int cut : new int[] {0, 1, 4, 11, 12, 13, 64, n / 2, n - 8, n - 1}) {
                inputs.put(name + ", truncated to " + cut, Arrays.copyOf(raw, cut));
            }
        }
        for (var e : inputs.entrySet()) {
            for (Limits limits : List.of(Limits.DEFAULT, limits(1000))) {
                byte[] raw = e.getValue();
                String now = outcome(() -> PartFile.readFooter(source(raw), "p", raw.length, new ReadContext(Cancellation.none(), limits)));
                String before = outcome(() -> previousReadFooter(source(raw), raw.length, new ReadContext(Cancellation.none(), limits)));
                assertEquals(before, now, e.getKey() + ", maxFooterBytes " + limits.maxFooterBytes());
            }
        }
    }

    /** The leading magic is checked whether or not the window holds it: PAR1 or invalid_footer, PARE unsupported. */
    @Test
    void theLeadingMagicIsCheckedOnEveryPath() {
        for (byte[] raw : List.of(small, large, huge)) {
            for (String head : List.of("PAR0", "par1", "\0\0\0\0")) {
                byte[] bad = edit(raw, 0, head.getBytes(StandardCharsets.ISO_8859_1));
                RefusalException e = assertThrows(RefusalException.class,
                        () -> PartFile.readFooter(source(bad), "p", bad.length, new ReadContext(Cancellation.none(), Limits.DEFAULT)));
                assertEquals(RefusalCode.INVALID_FOOTER, e.code());
                assertTrue(e.getMessage().contains("does not begin and end with PAR1"), e.getMessage());
            }
            byte[] encrypted = edit(raw, 0, "PARE".getBytes(StandardCharsets.US_ASCII));
            UnsupportedFeatureException u = assertThrows(UnsupportedFeatureException.class,
                    () -> PartFile.readFooter(source(encrypted), "p", encrypted.length, new ReadContext(Cancellation.none(), Limits.DEFAULT)));
            assertEquals(UnsupportedCode.UNSUPPORTED_ENCRYPTION, u.code());
        }
    }

    /** A footer over {@code maxFooterBytes} is refused before anything beyond the bounded window is read. */
    @Test
    void aFooterOverTheLimitIsRefusedWithinTheBoundedWindow() {
        for (byte[] raw : List.of(large, huge)) {
            ReadContext ctx = new ReadContext(Cancellation.none(), limits(64));
            LimitExceededException e = assertThrows(LimitExceededException.class, () -> PartFile.readFooter(source(raw), "p", raw.length, ctx));
            assertTrue(e.getMessage().contains("maxFooterBytes"), e.getMessage());
            assertEquals(2, ctx.stats.sourceReads);
            assertEquals(4 + 64 + 8, ctx.stats.sourceBytes);
        }
    }

    /** A storage failure on the read of the footer bytes before the window is a storage error, not a refusal. */
    @Test
    void aStorageFailureBeforeTheWindowIsAStorageError() {
        int start = huge.length - 8 - footerLength(huge);
        PartSource inner = source(huge);
        PartSource failing = new PartSource() {
            @Override public String path() { return "p"; }
            @Override public long size() { return huge.length; }
            @Override public Optional<String> version() { return Optional.empty(); }
            @Override public void close() { }
            @Override public ByteBuffer read(long position, int length, Cancellation c) throws IOException {
                if (position == start) {
                    throw new IOException("boom");
                }
                return inner.read(position, length, c);
            }
        };
        StorageException e = assertThrows(StorageException.class,
                () -> PartFile.readFooter(failing, "p", huge.length, new ReadContext(Cancellation.none(), Limits.DEFAULT)));
        assertInstanceOf(IOException.class, e.getCause());
        assertEquals("boom", e.getCause().getMessage());
    }

    private static byte[] edit(byte[] raw, int at, byte[] b) {
        byte[] out = raw.clone();
        System.arraycopy(b, 0, out, at, Math.min(b.length, out.length - at));
        return out;
    }

    private interface Read {
        FileMetaData run() throws Exception;
    }

    /** {@return the outcome of a footer read, with object identities in messages left out} */
    private static String outcome(Read read) {
        return rawOutcome(read).replaceAll("@[0-9a-f]+", "@");
    }

    private static String rawOutcome(Read read) {
        try {
            return "footer " + read.run();
        } catch (RefusalException e) {
            return "refused " + e.code() + ": " + e.getMessage();
        } catch (UnsupportedFeatureException e) {
            return "unsupported " + e.code() + ": " + e.getMessage();
        } catch (Exception e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    /**
     * The reader's footer read before its window was bounded by maxFooterBytes, with the leading magic checked
     * as well as the trailing one (CHECK-1): the oracle of the refusals.
     */
    private static FileMetaData previousReadFooter(PartSource source, long size, ReadContext ctx) throws IOException {
        String name = "p";
        if (size < 12) {
            throw new RefusalException(RefusalCode.INVALID_FOOTER, name, "the part is too short to be Parquet");
        }
        byte[] lead = new byte[4];
        source.read(0, 4, ctx.cancellation).duplicate().get(lead);
        int tailLength = (int) Math.min(size, PartFile.TAIL_WINDOW);
        ByteBuffer tail = source.read(size - tailLength, tailLength, ctx.cancellation).duplicate();
        int end = tail.position() + tailLength;
        byte[] magic = new byte[4];
        tail.duplicate().position(end - 4).get(magic);
        String m = new String(magic, StandardCharsets.ISO_8859_1);
        String l = new String(lead, StandardCharsets.ISO_8859_1);
        if (m.equals("PARE") || l.equals("PARE")) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCRYPTION, "part " + name + " uses Parquet modular encryption");
        }
        if (!m.equals("PAR1") || !l.equals("PAR1")) {
            throw new RefusalException(RefusalCode.INVALID_FOOTER, name, "the file does not begin and end with PAR1");
        }
        long footerLength = Integer.toUnsignedLong(tail.duplicate().order(ByteOrder.LITTLE_ENDIAN).getInt(end - 8));
        if (footerLength > size - 12) {
            throw new RefusalException(RefusalCode.INVALID_FOOTER, name, "the footer length " + footerLength + " does not fit in the part");
        }
        if (footerLength > ctx.limits.maxFooterBytes()) {
            throw new LimitExceededException("maxFooterBytes", footerLength, ctx.limits.maxFooterBytes(), "part " + name);
        }
        byte[] footer = new byte[(int) footerLength];
        if (footerLength + 8 <= tailLength) {
            tail.duplicate().position(end - 8 - (int) footerLength).get(footer);
        } else {
            source.read(size - 8 - footerLength, (int) footerLength, ctx.cancellation).duplicate().get(footer);
        }
        try {
            return Thrift.fileMetaData(footer);
        } catch (DataException e) {
            throw new RefusalException(RefusalCode.INVALID_FOOTER, name, "the footer does not parse: " + e.getMessage());
        }
    }
}
