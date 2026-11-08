// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.seekparquet.storage.LocalStorage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Reads the shared conformance fixtures (written by an independent Parquet writer, with SNAPPY, GZIP
 * and ZSTD, dictionary and delta encodings, and data page v1 and v2) end to end. The conformance
 * harness checks their answers exactly; this test checks that every valid fixture opens, verifies
 * its part bounds, and reads all its rows.
 */
class ConformanceFixturesTest {

    static Path fixture(String relative) {
        String dir = System.getProperty("seekparquet.conformance");
        assumeTrue(dir != null && Files.isDirectory(Path.of(dir)), "the conformance directory is not available");
        return Path.of(dir).resolve(relative);
    }

    @ParameterizedTest
    @ValueSource(strings = {"basic", "binary-keys", "delimiter", "empty", "key-legacy-utf8", "list-bounds", "multipart",
            "no-value-page-index", "non-monotone-bounds", "non-utf8-keys", "projection", "truncated-bounds", "truncated-bounds-2"})
    void everyValidFixtureReadsInFull(String name) {
        Path root = fixture("datasets/" + name);
        OpenOptions options = OpenOptions.DEFAULT.withVerifyPartBounds(true).withOpenConcurrency(1);
        try (Dataset ds = Dataset.open(LocalStorage.of(root), options, Cancellation.none())) {
            List<String> columns = ds.columns().stream().filter(Column::projectable).map(Column::name).toList();
            List<Row> rows = ds.range(RangeQuery.builder().limit(1_000_000).columns(columns).build());
            assertEquals(ds.rows(), rows.size());
            for (int i = 1; i < rows.size(); i++) {
                assertEquals(-1, Integer.signum(rows.get(i - 1).key().compareTo(rows.get(i).key())));
            }
        }
    }
}
