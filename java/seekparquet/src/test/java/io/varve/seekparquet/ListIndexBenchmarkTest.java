// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Reuses the six-million-key local.WriteDir datasets from the Go benchmark. */
class ListIndexBenchmarkTest {
    @Test void sixMillionKeys() {
        String root = System.getenv("SEEKPARQUET_LIST_BENCH_DIR");
        assumeTrue(root != null, "set SEEKPARQUET_LIST_BENCH_DIR to the Go benchmark's retained datasets");
        for (String layout : List.of("wide", "flat")) {
            try (Dataset d = Dataset.open(Path.of(root, layout))) {
                d.verify();
                ListQuery q = ListQuery.builder((byte) '/').prefix(Bytes.utf8(layout.equals("wide")
                        ? "v1.1/stellar/parquet/pubnet/v1/" : "")).limit(1000).build();
                var want = d.list(q);
                measure(d, q, layout, "plain");
                ListIndexStats build = d.buildListIndex(new ListIndexOptions(0, 2, 0), Cancellation.none());
                assertEquals(want, d.list(q));
                measure(d, q, layout, "indexed");
                System.out.printf("LIST_BUILD %s ms=%.3f bytes=%d entries=%d pages=%d%n", layout,
                        build.duration().toNanos() / 1e6, build.bytes(), build.entries(), build.pages());
            }
        }
    }

    private static void measure(Dataset d, ListQuery q, String layout, String mode) {
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        for (int i = 0; i < 5; i++) d.list(q);
        long start = System.nanoTime();
        for (int i = 0; i < 5; i++) Reads.list(d, q, Cancellation.none(), stats::set);
        System.out.printf("LIST_BENCH %s %s ms=%.3f keyPages=%d keys=%d%n", layout, mode,
                (System.nanoTime() - start) / 5e6, stats.get().keyPagesDecoded(), stats.get().keysDecoded());
    }
}
