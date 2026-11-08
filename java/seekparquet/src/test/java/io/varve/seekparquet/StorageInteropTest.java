// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageInteropTest {
    @TempDir Path tmp;

    @Test
    void parquetStreamsHaveIndependentPositionsAndDoNotOwnTheSource() throws Exception {
        Path root = TestDatasets.write(tmp, new TestDatasets.Spec(),
                List.of(List.of(TestDatasets.TestRow.of("a", 1))));
        try (PartSource source = LocalStorage.of(root).openPart("data/part-00000.parquet", 0, Cancellation.none())) {
            InputFile file = source;
            assertEquals(source.size(), file.getLength());
            try (SeekableInputStream first = file.newStream(); SeekableInputStream second = file.newStream()) {
                byte[] magic = new byte[4];
                first.readFully(magic);
                assertArrayEquals(new byte[] {'P', 'A', 'R', '1'}, magic);
                assertEquals(0, second.getPos());
                second.seek(file.getLength() - 4);
                ByteBuffer tail = ByteBuffer.allocateDirect(4);
                second.readFully(tail);
                byte[] bytes = new byte[4];
                tail.flip().get(bytes);
                assertArrayEquals(magic, bytes);
                assertEquals(-1, second.read());
                assertEquals(0, second.read(ByteBuffer.allocate(0)));
                assertThrows(EOFException.class, () -> second.readFully(new byte[1]));
                assertThrows(EOFException.class, () -> second.seek(-1));
            }
            // Adapting a parquet InputFile keeps read positions private to each concurrent call.
            try (PartSource adapted = PartSource.of(source.path(), file, source.version());
                    var executor = Executors.newFixedThreadPool(4)) {
                var reads = executor.invokeAll(java.util.stream.IntStream.range(0, 16)
                        .<java.util.concurrent.Callable<ByteBuffer>>mapToObj(i ->
                                () -> adapted.read(i % 2 == 0 ? 0 : source.size() - 4, 4, Cancellation.none())).toList());
                for (var read : reads) {
                    assertEquals(ByteBuffer.wrap(new byte[] {'P', 'A', 'R', '1'}), read.get());
                }
                Cancellation cancelled = Cancellation.create();
                cancelled.cancel();
                assertThrows(CancellationException.class, () -> adapted.read(0, 4, cancelled));
                assertThrows(EOFException.class, () -> adapted.read(source.size(), 1, Cancellation.none()));
            }
            assertEquals(4, source.read(0, 4, Cancellation.none()).remaining());
            SeekableInputStream closed = source.newStream();
            closed.close();
            assertThrows(IOException.class, closed::read);
        }
    }
}
