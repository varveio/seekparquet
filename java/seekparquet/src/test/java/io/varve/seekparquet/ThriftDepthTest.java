// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Thrift nesting is bounded as the Go reader's internal/thriftcheck bounds it (the top-level struct at
 * depth 0, no value deeper than 64), skipped unknown fields included, and a structure nested deeper is
 * refused as malformed Thrift rather than overflowing the stack. The probes below are the ones checked
 * against the Go reader's decodeBounded, with the same outcomes.
 */
class ThriftDepthTest {

    @TempDir
    Path tmp;

    /** Unknown field 100 (a struct, in the long field-header form) holding n nested struct fields; inner goes in the deepest. */
    static byte[] chain(int n, int... inner) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0x0C);
        b.write(0xC8);
        b.write(0x01);
        for (int i = 0; i < n; i++) {
            b.write(0x1C);
        }
        for (int x : inner) {
            b.write(x);
        }
        for (int i = 0; i <= n; i++) {
            b.write(0x00);
        }
        return b.toByteArray();
    }

    /** Unknown field 100 (a list) holding k nested lists, the deepest empty. */
    static byte[] lists(int k) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0x09);
        b.write(0xC8);
        b.write(0x01);
        for (int i = 0; i < k - 1; i++) {
            b.write(0x19);
        }
        b.write(0x05);
        return b.toByteArray();
    }

    /** A FileMetaData holding only its required fields (version 1, empty schema, 0 rows, no row groups) and {@code field}. */
    static byte[] metadata(byte[] field) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(new byte[] {0x15, 0x02, 0x19, 0x0C, 0x16, 0x00, 0x19, 0x0C});
        b.writeBytes(field);
        b.write(0x00);
        return b.toByteArray();
    }

    static byte[] probe(String name) {
        return switch (name) {
            case "chain63" -> chain(63);
            case "chain64" -> chain(64);
            case "chain10000" -> chain(10_000);
            case "chain63+i32" -> chain(63, 0x15, 0x02);
            case "chain63+bool" -> chain(63, 0x11);
            case "chain62+list1" -> chain(62, 0x19, 0x15, 0x02);
            case "chain62+list0" -> chain(62, 0x19, 0x05);
            case "chain62+set1" -> chain(62, 0x1A, 0x15, 0x02);
            case "chain62+map1" -> chain(62, 0x1B, 0x01, 0x55, 0x02, 0x02);
            case "chain62+map0" -> chain(62, 0x1B, 0x00);
            case "lists64" -> lists(64);
            case "lists65" -> lists(65);
            default -> throw new IllegalArgumentException(name);
        };
    }

    @ParameterizedTest
    @CsvSource({
        "chain63, true", "chain64, false", "chain10000, false", "chain63+i32, false", "chain63+bool, true",
        "chain62+list1, false", "chain62+list0, true", "chain62+set1, false", "chain62+map1, false",
        "chain62+map0, true", "lists64, true", "lists65, false"})
    void nestingIsBoundedAsInGo(String name, boolean decodes) {
        byte[] raw = metadata(probe(name));
        if (decodes) {
            assertDoesNotThrow(() -> Thrift.fileMetaData(raw, Limits.DEFAULT.metadataMemory()));
        } else {
            DataException e = assertThrows(DataException.class, () -> Thrift.fileMetaData(raw, Limits.DEFAULT.metadataMemory()));
            assertTrue(e.getCause().getMessage().contains("nested more than 64"), e.getCause().getMessage());
        }
    }

    @Test
    void pageIndexesAreBoundedToo() {
        // OffsetIndex: page_locations [] then the unknown field; ColumnIndex: its four required lists, empty.
        byte[] offsetIndex = concat(new byte[] {0x19, 0x0C}, chain(10_000), new byte[] {0x00});
        byte[] columnIndex = concat(new byte[] {0x19, 0x01, 0x19, 0x08, 0x19, 0x08, 0x15, 0x00}, chain(10_000), new byte[] {0x00});
        assertThrows(DataException.class, () -> Thrift.offsetIndex(offsetIndex, Limits.DEFAULT.metadataMemory()));
        assertThrows(DataException.class, () -> Thrift.columnIndex(columnIndex, Limits.DEFAULT.metadataMemory()));
        assertDoesNotThrow(() -> Thrift.offsetIndex(concat(new byte[] {0x19, 0x0C}, chain(63), new byte[] {0x00}), Limits.DEFAULT.metadataMemory()));
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            b.writeBytes(p);
        }
        return b.toByteArray();
    }

    private Path dataset(String name, TestDatasets.Spec spec) throws Exception {
        List<List<TestRow>> parts = List.of(
                List.of(TestRow.of("a", 1), TestRow.of("b", 2), TestRow.of("c", 3), TestRow.of("d", 4)),
                List.of(TestRow.of("e", 5), TestRow.of("f", 6)));
        return TestDatasets.write(tmp.resolve(name), spec, parts);
    }

    private static RefusalCode refusal(Path root) {
        try (Dataset ds = Dataset.open(root)) {
            RefusalCode onRead = assertThrows(RefusalException.class, () -> ds.rows(Reads.all(100))).code();
            assertEquals(onRead, assertThrows(RefusalException.class, ds::verify).code());
            return onRead;
        }
    }

    private static int rows(Path root) {
        try (Dataset ds = Dataset.open(root)) {
            ds.verify();
            return ds.rows(Reads.all(100)).size();
        }
    }

    @Test
    void aDeeplyNestedUnknownFooterFieldIsAnInvalidFooter() throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.footerExtraField = chain(10_000);
        assertEquals(RefusalCode.INVALID_FOOTER, refusal(dataset("deep-footer", spec)));
    }

    @Test
    void aDeeplyNestedUnknownPageHeaderFieldIsRefused() throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.keyPageHeaderExtraField = chain(10_000);
        // A key page header that does not decode: its OffsetIndex entry does not locate a page header (as in Go).
        assertEquals(RefusalCode.INVALID_PAGE_INDEX, refusal(dataset("deep-page-header", spec)));
    }

    @Test
    void unknownFieldsNestedToTheLimitStillDecode() throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.footerExtraField = chain(63);
        spec.keyPageHeaderExtraField = chain(63);
        assertEquals(6, rows(dataset("at-limit", spec)));
    }
}
