// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * The decoded-metadata budget holds for whatever the bytes claim, because it is charged in the protocol the
 * decoder reads through: container element types that disagree with the schema, fields of the wrong type
 * skipped as unknown, nested unknown containers, UUID values (compact type 13), varint boundaries and
 * truncation.
 */
class ThriftBudgetTest {

    private static final int PAYLOAD = 8 << 20;
    private static final long BUDGET = 64 << 10;

    private static long allocated(Executable op) {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
        long before = bean.getCurrentThreadAllocatedBytes();
        try {
            op.execute();
        } catch (Throwable ignored) {
            // the outcome is asserted separately
        }
        return bean.getCurrentThreadAllocatedBytes() - before;
    }

    /** Compact-protocol bytes, written by hand so that they can contradict the schema. */
    private static final class Bytes8 {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        Bytes8 b(int... bytes) {
            for (int x : bytes) out.write(x);
            return this;
        }

        Bytes8 varint(long v) {
            while ((v & ~0x7FL) != 0) {
                out.write((int) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            out.write((int) v);
            return this;
        }

        Bytes8 binary(int n) {
            varint(n);
            out.writeBytes(new byte[n]);
            return this;
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    private static void overBudget(byte[] raw, Executable decode) {
        LimitExceededException e = assertThrows(LimitExceededException.class, decode);
        assertEquals(LimitName.METADATA_MEMORY, e.limit());
        long bytes = allocated(decode);
        assertTrue(bytes < PAYLOAD / 8, "the refusal allocated " + bytes + " bytes of a " + raw.length + "-byte input");
    }

    /** A ColumnIndex whose min_values list declares BYTE elements over a binary payload, as the schema reads it. */
    @Test
    void aMismatchedElementTypeIsChargedAsTheDecoderReadsIt() {
        byte[] index = new Bytes8()
                .b(0x19, 0x12, 0x02)          // 1: null_pages, list<bool> of one: false
                .b(0x19, 0x13).binary(PAYLOAD) // 2: min_values, declared list<byte>, holding an 8 MiB binary
                .b(0x19, 0x18).binary(1)      // 3: max_values
                .b(0x15, 0x00)                // 4: boundary_order
                .b(0x00).bytes();
        overBudget(index, () -> Thrift.columnIndex(index, BUDGET));
    }

    /** A footer whose key_value_metadata list declares BYTE elements over KeyValue structs with a huge value. */
    @Test
    void nestedStructsUnderAMismatchedListAreCharged() {
        byte[] footer = new Bytes8()
                .b(0x59, 0x13)                        // 5: key_value_metadata, declared list<byte> of one
                .b(0x18).binary(1).b(0x18).binary(PAYLOAD).b(0x00) // KeyValue{key, value}
                .b(0x00).bytes();
        overBudget(footer, () -> Thrift.fileMetaData(footer, BUDGET));
        // Truncated: the declared length exceeds what remains. Malformed, nothing of that size allocated.
        byte[] truncated = Arrays.copyOf(footer, 1 << 20);
        assertThrows(DataException.class, () -> Thrift.fileMetaData(truncated, Long.MAX_VALUE));
        assertTrue(allocated(() -> Thrift.fileMetaData(truncated, Long.MAX_VALUE)) < PAYLOAD / 8);
    }

    /** A page header field of the wrong type is skipped as unknown, through the same charged reads. */
    @Test
    void aSkippedFieldOfTheWrongTypeIsCharged() {
        byte[] header = new Bytes8()
                .b(0x15, 0x00)                       // 1: type DATA_PAGE
                .b(0x15, 0x02, 0x15, 0x02)           // 2, 3: sizes
                .b(0x2C)                             // 5: data_page_header struct
                .b(0x15, 0x02, 0x15, 0x00, 0x15, 0x06, 0x15, 0x06) // num_values, encodings
                .b(0x1C)                             // 5: statistics struct
                .b(0x19, 0x28).binary(1).binary(PAYLOAD) // 1: max, declared list<binary> (the schema has binary)
                .b(0x00, 0x00, 0x00).bytes();
        overBudget(header, () -> Thrift.pageHeader(header, 0, header.length, BUDGET));
    }

    private static byte[] columnIndex() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writeColumnIndex(new ColumnIndex(List.of(false), List.of(java.nio.ByteBuffer.wrap(new byte[] {1})),
                List.of(java.nio.ByteBuffer.wrap(new byte[] {2})), BoundaryOrder.ASCENDING), out);
        return out.toByteArray();
    }

    /** {@return a ColumnIndex followed, before its stop, by {@code extra} unknown fields} */
    private static byte[] withUnknown(byte[] index, byte[] extra) {
        byte[] out = Arrays.copyOf(index, index.length - 1 + extra.length + 1);
        System.arraycopy(extra, 0, out, index.length - 1, extra.length);
        out[out.length - 1] = 0;
        return out;
    }

    /**
     * UUID values (compact type 13): an unknown UUID field is skipped and charged, as the Thrift runtime skips
     * it; a container of UUIDs, whatever its nesting, is refused as malformed, as the runtime refuses it (its
     * container size check has no UUID case), and a truncated UUID is malformed. The budget lives in the
     * protocol the decoder reads, so these outcomes are the runtime's, with no second parser to disagree.
     */
    @Test
    void uuidValuesAreDecidedByTheRuntime() throws Exception {
        byte[] index = columnIndex();
        ColumnIndex plain = Thrift.columnIndex(index, Long.MAX_VALUE);
        byte[] field = withUnknown(index, new Bytes8().b(0xBD).b(new int[16]).bytes());              // 15: uuid
        assertEquals(plain, Thrift.columnIndex(field, Long.MAX_VALUE));
        byte[] cut = withUnknown(index, new Bytes8().b(0xBD).b(new int[7]).bytes());
        assertThrows(DataException.class, () -> Thrift.columnIndex(cut, Long.MAX_VALUE));
        for (byte[] extra : List.of(
                new Bytes8().b(0xB9, 0x2D).b(new int[32]).bytes(),              // 15: list<uuid> of two
                new Bytes8().b(0xBA, 0x1D).b(new int[16]).bytes(),              // 15: set<uuid> of one
                new Bytes8().b(0xB9, 0x19, 0x1D).b(new int[16]).bytes())) {     // 15: list<list<uuid>>
            assertThrows(DataException.class, () -> Thrift.columnIndex(withUnknown(index, extra), Long.MAX_VALUE));
        }
        // Each skipped UUID is charged: 4,096 of them exceed a 32 KiB budget.
        Bytes8 many = new Bytes8();
        for (int i = 0; i < 4096; i++) many.b(0x1D).b(new int[16]);
        byte[] raw = withUnknown(index, new Bytes8().b(0xBD).b(new int[16]).bytes());
        byte[] lots = withUnknown(raw, many.bytes());
        assertEquals(plain, Thrift.columnIndex(lots, Long.MAX_VALUE));
        assertEquals(LimitName.METADATA_MEMORY,
                assertThrows(LimitExceededException.class, () -> Thrift.columnIndex(lots, 32 << 10)).limit());
    }

    /** Declared lengths at varint boundaries: those beyond the input are malformed, before any allocation. */
    @Test
    void lengthsAtVarintBoundaries() throws Exception {
        byte[] index = columnIndex();
        for (long n : new long[] {127, 128, 16383, 16384, 2097151, 2097152, Integer.MAX_VALUE, 1L << 32, -1L}) {
            byte[] raw = withUnknown(index, new Bytes8().b(0xB8).varint(n).b(1, 2, 3).bytes()); // 15: binary of n
            assertThrows(DataException.class, () -> Thrift.columnIndex(raw, Long.MAX_VALUE), "length " + n);
            assertTrue(allocated(() -> Thrift.columnIndex(raw, Long.MAX_VALUE)) < 1 << 20, "length " + n);
        }
        for (int n : new int[] {0, 1, 127, 128, 16384}) {
            byte[] raw = withUnknown(index, new Bytes8().b(0xB8).binary(n).bytes());
            assertEquals(Thrift.columnIndex(index, Long.MAX_VALUE), Thrift.columnIndex(raw, Long.MAX_VALUE), "length " + n);
        }
    }

    private static byte[] pageHeader() throws Exception {
        DataPageHeader dph = new DataPageHeader(3, Encoding.PLAIN, Encoding.RLE, Encoding.RLE);
        dph.setStatistics(new Statistics().setMax_value(new byte[] {9, 9}).setMin_value(new byte[] {1}).setNull_count(0));
        PageHeader h = new PageHeader(PageType.DATA_PAGE, 10, 10).setData_page_header(dph);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(h, out);
        return out.toByteArray();
    }

    private enum Outcome { DECODED, MALFORMED, OVER_BUDGET }

    private static Outcome outcome(byte[] raw, long budget, int kind) {
        try {
            switch (kind) {
                case 0 -> Thrift.columnIndex(raw, budget);
                case 1 -> Thrift.pageHeader(raw, 0, raw.length, budget);
                default -> Thrift.fileMetaData(raw, budget);
            }
            return Outcome.DECODED;
        } catch (DataException e) {
            return Outcome.MALFORMED;
        } catch (LimitExceededException e) {
            assertEquals(LimitName.METADATA_MEMORY, e.limit());
            return Outcome.OVER_BUDGET;
        }
    }

    /**
     * Seeded mutations (byte flips, truncation, inserted nested unknown fields, large declared lengths) of a
     * ColumnIndex, a page header and a footer: decoding fails only as malformed or over budget, never otherwise;
     * the budget is monotone (what decodes within a budget decodes within a larger one, and an over-budget input
     * either decodes or is malformed without a budget); and no input makes the decoder allocate far beyond its
     * own size.
     */
    @Test
    void mutationsFailOnlyAsMalformedOrOverBudget() throws Exception {
        byte[] part = TestDatasets.part(new TestDatasets.Spec(), List.of(TestDatasets.TestRow.of("a", 1), TestDatasets.TestRow.of("b", 2)));
        int length = java.nio.ByteBuffer.wrap(part, part.length - 8, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] footer = Arrays.copyOfRange(part, part.length - 8 - length, part.length - 8);
        List<byte[]> seeds = List.of(columnIndex(), pageHeader(), footer);
        List<byte[]> inserts = List.of(
                new Bytes8().b(0xBD).b(new int[16]).bytes(),
                new Bytes8().b(0xB9, 0x1D).b(new int[16]).bytes(),
                new Bytes8().b(0xB9, 0x19, 0x18).binary(3).bytes(),
                new Bytes8().b(0xBB).varint(1).b(0x88).binary(2).binary(1).bytes(),
                new Bytes8().b(0xBC, 0x18).binary(4).b(0x00).bytes(),
                new Bytes8().b(0xB8).varint(1 << 20).bytes());
        Random random = new Random(20261112);
        int decoded = 0;
        for (int i = 0; i < 3000; i++) {
            int kind = i % 3;
            byte[] seed = seeds.get(kind);
            byte[] raw = switch (random.nextInt(4)) {
                case 0 -> {
                    byte[] c = seed.clone();
                    for (int f = 1 + random.nextInt(3); f > 0; f--) c[random.nextInt(c.length)] ^= (byte) (1 << random.nextInt(8));
                    yield c;
                }
                case 1 -> Arrays.copyOf(seed, random.nextInt(seed.length));
                case 2 -> withUnknown(seed, inserts.get(random.nextInt(inserts.size())));
                default -> {
                    byte[] c = seed.clone();
                    c[random.nextInt(c.length)] = (byte) 0xFF;
                    yield c;
                }
            };
            long budget = 64 + random.nextInt(4096);
            Outcome small = outcome(raw, budget, kind);
            Outcome large = outcome(raw, Long.MAX_VALUE, kind);
            if (small == Outcome.DECODED) {
                assertEquals(Outcome.DECODED, outcome(raw, 2 * budget, kind), "monotone, case " + i);
                assertEquals(Outcome.DECODED, large, "case " + i);
                decoded++;
            }
            if (large == Outcome.OVER_BUDGET) {
                throw new AssertionError("over an unlimited budget, case " + i);
            }
            byte[] input = raw;
            long bytes = allocated(() -> outcome(input, Long.MAX_VALUE, kind));
            assertTrue(bytes < 64L * input.length + (1 << 20), "case " + i + " allocated " + bytes + " for " + input.length + " bytes");
        }
        assertTrue(decoded > 0, "some mutations decode within a small budget");
    }
}
