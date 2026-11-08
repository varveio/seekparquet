// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Refusals from datasets written with injected faults, and the stage that reports them. */
class RefusalTest {

    @TempDir
    Path tmp;

    private static List<List<TestRow>> rows() {
        return List.of(
                List.of(TestRow.of("a", 1), TestRow.of("b", 2), TestRow.of("c", 3), TestRow.of("d", 4), TestRow.of("e", 5)),
                List.of(TestRow.of("f", 6), TestRow.of("g", 7)));
    }

    private static RefusalCode openRefusal(Path root) {
        return assertThrows(RefusalException.class, () -> Dataset.open(root).close()).code();
    }

    @Test
    void aMissingManifestIsNoManifest() {
        assertEquals(RefusalCode.NO_MANIFEST, openRefusal(tmp.resolve("nothing-here")));
    }

    @Test
    void keysOutOfOrderAreRefusedOnReadNotOnOpen() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.swapFirstKeys = true;
        Path root = TestDatasets.write(tmp, spec, rows());
        try (Dataset ds = Dataset.open(root)) {
            RefusalException e = assertThrows(RefusalException.class, () -> ds.range(RangeQuery.all(100)));
            assertEquals(RefusalCode.KEY_ORDER, e.code());
            assertEquals(Optional.of("data/part-00000.parquet"), e.part());
            // A read that never decodes the faulty page succeeds (CHECK-4).
            assertEquals(2, ds.range(RangeQuery.builder().start(Bytes.utf8("f")).limit(10).build()).size());
        }
    }

    @Test
    void aMissingKeyColumnIndexIsRefusedWhenTheRowGroupIsRead() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.omitKeyColumnIndex = true;
        Path root = TestDatasets.write(tmp, spec, rows());
        try (Dataset ds = Dataset.open(root)) {
            assertEquals(RefusalCode.MISSING_PAGE_INDEX, assertThrows(RefusalException.class, () -> ds.range(RangeQuery.all(100))).code());
            assertEquals(1, ds.range(RangeQuery.all(1)).size());
        }
    }

    @Test
    void sizeRowsAndStampsAreCheckedOnOpen() throws IOException {
        Path root = TestDatasets.write(tmp.resolve("size"), new TestDatasets.Spec(), rows());
        Files.write(root.resolve("data/part-00001.parquet"), new byte[] {1}, java.nio.file.StandardOpenOption.APPEND);
        assertEquals(RefusalCode.PART_MISMATCH, openRefusal(root));

        Path missing = TestDatasets.write(tmp.resolve("missing"), new TestDatasets.Spec(), rows());
        Files.delete(missing.resolve("data/part-00001.parquet"));
        assertEquals(RefusalCode.MISSING_PART, openRefusal(missing));

        Path stamps = TestDatasets.write(tmp.resolve("stamps"), new TestDatasets.Spec(), rows());
        String m = Files.readString(stamps.resolve("manifest.json"));
        Files.writeString(stamps.resolve("manifest.json"), m.replace("\"test/v1\"", "\"test/v2\""));
        assertEquals(RefusalCode.STAMP_MISMATCH, openRefusal(stamps));

        Path count = TestDatasets.write(tmp.resolve("count"), new TestDatasets.Spec(), rows());
        m = Files.readString(count.resolve("manifest.json"));
        Files.writeString(count.resolve("manifest.json"), m.replace("\"rows\":2", "\"rows\":3"));
        assertEquals(RefusalCode.PART_MISMATCH, openRefusal(count));
    }

    @Test
    void wrongManifestBoundsAreFoundWhenVerificationIsAskedFor() throws IOException {
        Path root = TestDatasets.write(tmp, new TestDatasets.Spec(), rows());
        String m = Files.readString(root.resolve("manifest.json"));
        // Part 1 claims "g" .. "h" ("aA==" is "h"); its real last key is "g" ("Zw==").
        Files.writeString(root.resolve("manifest.json"), m.replace("\"max_key_base64\":\"Zw==\"", "\"max_key_base64\":\"aA==\""));
        Dataset.open(root).close();
        OpenOptions verify = OpenOptions.DEFAULT.withVerifyPartBounds(true);
        RefusalException e = assertThrows(RefusalException.class,
                () -> Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root), verify, Cancellation.none()));
        assertEquals(RefusalCode.BOUNDS_MISMATCH, e.code());
    }

    @Test
    void limitsAreNotRefusals() throws IOException {
        Path root = TestDatasets.write(tmp, new TestDatasets.Spec(), rows());
        OpenOptions tiny = OpenOptions.DEFAULT.withLimits(new Limits(1 << 20, 1, 1 << 20, 1 << 20, 1 << 20, 1 << 20));
        LimitExceededException e = assertThrows(LimitExceededException.class,
                () -> Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root), tiny, Cancellation.none()));
        assertEquals("maxParts", e.limit());
    }

    @Test
    void projectingANestedColumnIsUnsupportedNotAbsent() {
        Path projection = ConformanceFixturesTest.fixture("datasets/projection");
        try (Dataset ds = Dataset.open(projection)) {
            for (String nested : List.of("meta", "tags")) {
                UnsupportedFeatureException e = assertThrows(UnsupportedFeatureException.class,
                        () -> ds.range(RangeQuery.builder().limit(10).columns(nested).build()));
                assertEquals(UnsupportedCode.UNSUPPORTED_PROJECTION, e.code());
            }
            assertEquals(Value.absent(), ds.range(RangeQuery.builder().limit(1).columns("nowhere").build()).getFirst().values().getFirst());
        }
    }
}
