// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The structured location of a refusal: part, row group, row and key, where the reader has them in hand. */
class RefusalLocationTest {

    @TempDir
    Path tmp;

    private static List<List<TestRow>> rows() {
        return List.of(
                List.of(TestRow.of("a", 1), TestRow.of("b", 2), TestRow.of("c", 3), TestRow.of("d", 4), TestRow.of("e", 5)),
                List.of(TestRow.of("f", 6), TestRow.of("g", 7)));
    }

    @Test
    void aKeyOrderRefusalCarriesPartRowGroupRowAndKey() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.swapFirstKeys = true;
        Path root = TestDatasets.write(tmp, spec, rows());
        try (Dataset ds = Dataset.open(root)) {
            RefusalException e = assertThrows(RefusalException.class, () -> ds.range(RangeQuery.all(100)));
            assertEquals(RefusalCode.KEY_ORDER, e.code());
            assertEquals(Optional.of("data/part-00000.parquet"), e.part());
            assertEquals(OptionalInt.of(0), e.rowGroup());
            assertEquals(OptionalLong.of(1), e.row());
            assertEquals(Optional.of(Bytes.utf8("a")), e.key());
        }
    }

    @Test
    void aPageIndexRefusalCarriesPartAndRowGroupOnly() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.omitKeyColumnIndex = true;
        Path root = TestDatasets.write(tmp, spec, rows());
        try (Dataset ds = Dataset.open(root)) {
            RefusalException e = assertThrows(RefusalException.class, () -> ds.range(RangeQuery.all(100)));
            assertEquals(RefusalCode.MISSING_PAGE_INDEX, e.code());
            assertEquals(Optional.of("data/part-00000.parquet"), e.part());
            // The first group, a b c d, reads without its ColumnIndex; the second, e, is the one refused.
            assertEquals(OptionalInt.of(1), e.rowGroup());
            assertEquals(OptionalLong.empty(), e.row());
            assertEquals(Optional.empty(), e.key());
        }
    }

    @Test
    void aManifestRefusalCarriesNoLocation() {
        RefusalException e = assertThrows(RefusalException.class, () -> Dataset.open(tmp.resolve("nothing-here")).close());
        assertEquals(RefusalCode.NO_MANIFEST, e.code());
        assertEquals(Optional.empty(), e.part());
        assertEquals(OptionalInt.empty(), e.rowGroup());
        assertEquals(OptionalLong.empty(), e.row());
        assertEquals(Optional.empty(), e.key());
    }

    @Test
    void aPartRefusalWithoutARowGroupCarriesOnlyThePart() throws IOException {
        Path root = TestDatasets.write(tmp, new TestDatasets.Spec(), rows());
        java.nio.file.Files.delete(root.resolve("data/part-00001.parquet"));
        try (Dataset ds = Dataset.open(root)) {
            RefusalException e = assertThrows(RefusalException.class, ds::verify);
            assertEquals(RefusalCode.MISSING_PART, e.code());
            assertEquals(Optional.of("data/part-00001.parquet"), e.part());
            assertEquals(OptionalInt.empty(), e.rowGroup());
            assertEquals(Optional.empty(), e.key());
        }
    }

    @Test
    void theKeyIsACopy() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.swapFirstKeys = true;
        Path root = TestDatasets.write(tmp, spec, rows());
        try (Dataset ds = Dataset.open(root)) {
            RefusalException e = assertThrows(RefusalException.class, () -> ds.range(RangeQuery.all(100)));
            byte[] copy = e.key().orElseThrow().toByteArray();
            copy[0] = 'z';
            assertEquals(Bytes.utf8("a"), e.key().orElseThrow());
        }
    }
}
