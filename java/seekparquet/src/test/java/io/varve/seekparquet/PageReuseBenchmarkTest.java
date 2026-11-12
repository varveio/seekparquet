// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Generated fixture; setup and planning are outside the allocation measurement. */
class PageReuseBenchmarkTest {
    @TempDir Path tmp;
    private static volatile long checksum;

    @Test void allocations() throws Exception {
        assumeTrue(System.getenv("SEEKPARQUET_PAGE_BENCH") != null);
        var bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assumeTrue(bean.isThreadAllocatedMemorySupported());
        bean.setThreadAllocatedMemoryEnabled(true);
        int rows = 65_536;
        for (boolean dictionary : List.of(false, true)) {
            List<List<TestDatasets.TestRow>> parts = new ArrayList<>();
            for (int p = 0; p < 2; p++) {
                List<TestDatasets.TestRow> part = new ArrayList<>();
                for (int i = p * rows / 2; i < (p + 1) * rows / 2; i++) {
                    // The dictionary fixture has eight non-null binary entries and one-third nulls.
                    part.add(TestDatasets.TestRow.of(String.format("objects/%08d", i), dictionary ? i % 12 : i));
                }
                parts.add(part);
            }
            String encoding = dictionary ? "DICTIONARY" : "PLAIN";
            for (CompressionCodec codec : List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.ZSTD)) {
                var spec = new TestDatasets.Spec();
                spec.noteColumn = true;
                spec.dictionaryNote = dictionary;
                spec.rowsPerGroup = 4096;
                spec.rowsPerPage = 256;
                spec.valueCodec = codec;
                try (Dataset d = Dataset.open(TestDatasets.write(tmp.resolve(encoding + "-" + codec.name()), spec, parts))) {
                    Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size", "note"));
                    for (int i = 0; i < 20; i++) scan(plan, rows);
                    for (int trial = 0; trial < 3; trial++) {
                        long before = bean.getCurrentThreadAllocatedBytes();
                        long start = System.nanoTime();
                        for (int i = 0; i < 5; i++) scan(plan, rows);
                        long elapsed = System.nanoTime() - start;
                        long bytes = bean.getCurrentThreadAllocatedBytes() - before;
                        System.out.printf("PAGE_BENCH encoding=%s codec=%s trial=%d rows=%d bytes/row=%.2f ns/row=%.2f checksum=%d%n",
                                encoding, codec, trial, rows, bytes / (5.0 * rows), elapsed / (5.0 * rows), checksum);
                    }
                    for (int i = 0; i < 20; i++) borrowed(plan, rows);
                    for (int trial = 0; trial < 3; trial++) {
                        long before = bean.getCurrentThreadAllocatedBytes();
                        long start = System.nanoTime();
                        for (int i = 0; i < 5; i++) borrowed(plan, rows);
                        long elapsed = System.nanoTime() - start;
                        long bytes = bean.getCurrentThreadAllocatedBytes() - before;
                        System.out.printf("BATCH_BENCH encoding=%s codec=%s trial=%d rows=%d bytes/row=%.2f ns/row=%.2f checksum=%d%n",
                                encoding, codec, trial, rows, bytes / (5.0 * rows), elapsed / (5.0 * rows), checksum);
                    }
                }
            }
        }
    }

    /** The borrowed path: the typed accessors, no owning row. */
    private static void borrowed(Plan plan, int expected) {
        long[] totals = new long[2];
        plan.scan(b -> {
            BatchColumn size = b.column(0);
            BatchColumn note = b.column(1);
            for (int i = 0; i < b.size(); i++) {
                totals[0]++;
                totals[1] += size.getLong(i);
                if (note.isPresent() && !note.isNull(i)) totals[1] += note.byteLength(i);
            }
            return true;
        });
        assertEquals(expected, totals[0]);
        checksum = totals[1];
    }

    /** The owning path: a row per batch row. */
    private static void scan(Plan plan, int expected) {
        long[] totals = new long[2];
        Reads.scan(plan, row -> {
            totals[0]++;
            totals[1] += ((Value.Int64) row.values().getFirst()).value();
            if (row.values().get(1) instanceof Value.ByteArray b) totals[1] += b.value().length();
        });
        assertEquals(expected, totals[0]);
        checksum = totals[1];
    }
}
