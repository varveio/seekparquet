// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Corrupts bytes of valid fixtures at random (keeping every size) and reads the result in full. The
 * reader may succeed (CHECK-4) or fail, but only with a {@link SeekParquetException}: corrupt data
 * must never escape as an unrelated runtime exception or an allocation failure.
 */
class CorruptionTest {

    @TempDir
    Path tmp;

    @ParameterizedTest
    @ValueSource(strings = {"basic", "multipart", "projection", "binary-keys", "binary-bounds-truncated-2-byte"})
    void corruptPartsFailOnlyWithSeekParquetExceptions(String name) throws IOException {
        Path source = ConformanceFixturesTest.fixture("datasets/" + name);
        List<Path> files;
        try (Stream<Path> walk = Files.walk(source)) {
            files = walk.filter(Files::isRegularFile).toList();
        }
        Random random = new Random(name.hashCode());
        Limits small = new Limits(1 << 20, 1000, 1 << 20, 1 << 20, 4 << 20, 1 << 20);
        OpenOptions options = OpenOptions.DEFAULT.withLimits(small).withOpenConcurrency(1);
        for (int round = 0; round < 150; round++) {
            Path root = tmp.resolve(name + "-" + round);
            for (Path f : files) {
                Path target = root.resolve(source.relativize(f).toString());
                Files.createDirectories(target.getParent());
                Files.copy(f, target);
            }
            List<Path> parts = files.stream().filter(f -> f.toString().endsWith(".parquet")).map(f -> root.resolve(source.relativize(f).toString()))
                    .toList();
            Path victim = parts.get(random.nextInt(parts.size()));
            byte[] bytes = Files.readAllBytes(victim);
            int flips = 1 + random.nextInt(4);
            for (int i = 0; i < flips; i++) {
                bytes[random.nextInt(bytes.length)] = (byte) random.nextInt(256);
            }
            Files.write(victim, bytes);
            try (Dataset ds = Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root), options, Cancellation.none())) {
                List<String> columns = ds.columns().stream().filter(Column::projectable).map(Column::name).toList();
                ds.range(RangeQuery.builder().limit(100_000).columns(columns).build());
                ds.list(ListQuery.builder((byte) '/').limit(100_000).columns(columns).build());
                ds.range(RangeQuery.builder().start(Bytes.utf8("m")).limit(3).columns(columns).build());
            } catch (SeekParquetException expected) {
                // A refusal, an unsupported feature or a limit: all acceptable for corrupt data.
            } catch (RuntimeException | Error e) {
                fail("round " + round + " of " + name + " (" + victim.getFileName() + "): " + e, e);
            }
        }
    }
}
