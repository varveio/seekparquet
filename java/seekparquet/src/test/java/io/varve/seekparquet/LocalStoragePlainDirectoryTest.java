// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartChangedException;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * LocalStorage on a filesystem whose directory streams are not secure, as the default one on Windows:
 * each part path is resolved segment by segment as a path, links are refused in every segment, and a
 * file's identity is checked around each open, by fileKey or (without one, as on Windows) by size and
 * times. Runs on every platform through {@link PlainDirectoryFileSystem}.
 */
class LocalStoragePlainDirectoryTest {

    @TempDir
    Path tmp;

    private static List<List<TestRow>> rows() {
        return List.of(
                List.of(TestRow.of("a", 1), TestRow.of("b", 2), TestRow.of("c", 3)),
                List.of(TestRow.of("d", 4), TestRow.of("e", 5)));
    }

    private Path dataset() throws IOException {
        return TestDatasets.write(tmp.resolve("ds"), new TestDatasets.Spec(), rows());
    }

    /** {@return LocalStorage over {@code root} through plain directory streams} */
    private static LocalStorage plain(PlainDirectoryFileSystem fs, Path root) throws IOException {
        Path wrapped = fs.wrap(root);
        if (Files.isDirectory(root)) {
            assertFalse(PlainDirectoryFileSystem.secure(wrapped), "the test file system must not offer secure directory streams");
        }
        return LocalStorage.of(wrapped);
    }

    /** Creates a symbolic link, or skips the test where that needs a privilege (Windows without Developer Mode). */
    private static void link(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (IOException | UnsupportedOperationException e) {
            assumeTrue(false, "symbolic links cannot be created here: " + e);
        }
    }

    private static List<String> keys(Dataset ds) {
        return ds.rows(Reads.all(1000)).stream().map(r -> new String(r.key().toByteArray())).toList();
    }

    @ParameterizedTest(name = "fileKeys={0}")
    @ValueSource(booleans = {true, false})
    void readsADatasetAsTheSecurePathDoes(boolean fileKeys) throws IOException {
        Path root = dataset();
        try (Dataset secure = Dataset.open(root); Dataset ds = Dataset.open(plain(new PlainDirectoryFileSystem(fileKeys), root))) {
            ds.verify();
            assertEquals(List.of("a", "b", "c", "d", "e"), keys(ds));
            assertEquals(secure.rows(Reads.builder().limit(100).columns("size").build()),
                    ds.rows(Reads.builder().limit(100).columns("size").build()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"basic", "multipart", "repeated-keys", "projection", "binary-keys", "empty", "named-key"})
    void readsTheConformanceFixtures(String name) throws IOException {
        Path root = ConformanceFixturesTest.fixture("datasets/" + name);
        try (Dataset secure = Dataset.open(root); Dataset ds = Dataset.open(plain(new PlainDirectoryFileSystem(false), root))) {
            ds.verify();
            List<String> columns = Reads.columns(ds).stream().filter(Column::projectable).map(Column::name).toList();
            Query all = Reads.builder().limit(1_000_000).columns(columns).build();
            assertEquals(secure.rows(all), ds.rows(all));
            assertEquals(ds.rowCount(), ds.rows(all).size());
        }
    }

    @Test
    void aMissingRootOrPartIsReportedAsMissing() throws IOException {
        PlainDirectoryFileSystem fs = new PlainDirectoryFileSystem(true);
        assertEquals(RefusalCode.NO_MANIFEST, assertThrows(RefusalException.class,
                () -> Dataset.open(plain(fs, tmp.resolve("nothing-here")))).code());
        Path root = dataset();
        Files.delete(root.resolve("data/part-00001.parquet"));
        try (Dataset ds = Dataset.open(plain(fs, root))) {
            assertEquals(RefusalCode.MISSING_PART, assertThrows(RefusalException.class, ds::verify).code());
        }
        assertThrows(NoSuchFileException.class, () -> plain(fs, root).openPart("data/part-00001.parquet", 0, Cancellation.none()));
    }

    @Test
    void aSymlinkedPartIsRefused() throws IOException {
        Path root = dataset();
        Path part = root.resolve("data/part-00000.parquet");
        Path outside = tmp.resolve("outside.parquet");
        Files.move(part, outside);
        link(part, outside);
        LocalStorage storage = plain(new PlainDirectoryFileSystem(true), root);
        IOException e = assertThrows(IOException.class, () -> storage.openPart("data/part-00000.parquet", 0, Cancellation.none()));
        assertTrue(e.getMessage().contains("is not a regular file"), e.getMessage());
        try (Dataset ds = Dataset.open(storage)) {
            assertInstanceOf(StorageException.class, assertThrows(SeekParquetException.class, () -> ds.rows(Reads.all(10))));
        }
    }

    @Test
    void aSymlinkedDirectoryIsRefused() throws IOException {
        Path root = dataset();
        Path moved = tmp.resolve("moved");
        Files.move(root.resolve("data"), moved);
        link(root.resolve("data"), moved);
        LocalStorage storage = plain(new PlainDirectoryFileSystem(true), root);
        IOException e = assertThrows(FileSystemException.class, () -> storage.openPart("data/part-00000.parquet", 0, Cancellation.none()));
        assertTrue(e.getMessage().contains("symbolic link"), e.getMessage());
        try (Dataset ds = Dataset.open(storage)) {
            assertInstanceOf(StorageException.class, assertThrows(SeekParquetException.class, () -> ds.rows(Reads.all(10))));
        }
    }

    @Test
    void aSymlinkedManifestIsRefused() throws IOException {
        Path root = dataset();
        Path moved = tmp.resolve("manifest-elsewhere.json");
        Files.move(root.resolve("manifest.json"), moved);
        link(root.resolve("manifest.json"), moved);
        LocalStorage storage = plain(new PlainDirectoryFileSystem(true), root);
        IOException e = assertThrows(IOException.class, () -> storage.readManifest(1 << 20, Cancellation.none()));
        assertTrue(e.getMessage().endsWith("manifest.json is not a regular file"), e.getMessage());
        assertInstanceOf(StorageException.class, assertThrows(SeekParquetException.class, () -> Dataset.open(storage)));
    }

    @ParameterizedTest(name = "fileKeys={0}")
    @ValueSource(booleans = {true, false})
    void aPartReplacedBetweenItsCheckAndItsOpenIsDetected(boolean fileKeys) throws IOException {
        Path root = dataset();
        Path part = root.resolve("data/part-00000.parquet");
        // Another part's bytes: a different file of a different size.
        Path replacement = tmp.resolve("replacement.parquet");
        Files.copy(root.resolve("data/part-00001.parquet"), replacement);
        PlainDirectoryFileSystem fs = new PlainDirectoryFileSystem(fileKeys);
        AtomicBoolean armed = new AtomicBoolean(true);
        fs.afterRead((p, a) -> {
            if (p.equals(part) && a.isRegularFile() && armed.getAndSet(false)) {
                Files.move(replacement, part, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
        });
        LocalStorage storage = plain(fs, root);
        assertThrows(PartChangedException.class, () -> storage.openPart("data/part-00000.parquet", 0, Cancellation.none()));
    }

    @ParameterizedTest(name = "fileKeys={0}")
    @ValueSource(booleans = {true, false})
    void aManifestReplacedBetweenItsCheckAndItsOpenIsDetected(boolean fileKeys) throws IOException {
        Path root = dataset();
        Path manifest = root.resolve("manifest.json");
        Path replacement = tmp.resolve("other-manifest.json");
        Files.writeString(replacement, Files.readString(manifest) + " ");
        PlainDirectoryFileSystem fs = new PlainDirectoryFileSystem(fileKeys);
        AtomicBoolean armed = new AtomicBoolean(true);
        fs.afterRead((p, a) -> {
            if (p.equals(manifest) && a.isRegularFile() && armed.getAndSet(false)) {
                Files.move(replacement, manifest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
        });
        IOException e = assertThrows(IOException.class, () -> plain(fs, root).readManifest(1 << 20, Cancellation.none()));
        assertTrue(e.getMessage().endsWith("manifest.json changed while opening"), e.getMessage());
    }

    @Test
    void aReopenChecksTheDirectoriesAgain() throws Exception {
        Path root = dataset();
        LocalStorage storage = plain(new PlainDirectoryFileSystem(true), root);
        try (PartSource source = storage.openPart("data/part-00000.parquet", 0, Cancellation.none())) {
            Path moved = tmp.resolve("moved");
            try {
                Files.move(root.resolve("data"), moved);
            } catch (IOException e) {
                assumeTrue(false, "a directory holding an open file cannot be renamed here: " + e);
            }
            link(root.resolve("data"), moved);
            // Force the shared channel to close as an interrupted read would: the reopen resolves the path
            // again, and finds a link where the directory was. (Through a pinned handle, it would reopen the
            // same file; see ReviewRegressionTest.)
            var field = source.getClass().getDeclaredField("channel");
            field.setAccessible(true);
            ((Channel) field.get(source)).close();
            assertThrows(PartChangedException.class, () -> source.read(0, 4, Cancellation.none()));
        }
    }

    @Test
    void aReopenOfAnUnchangedPartSucceeds() throws Exception {
        Path root = dataset();
        LocalStorage storage = plain(new PlainDirectoryFileSystem(false), root);
        try (PartSource source = storage.openPart("data/part-00000.parquet", 0, Cancellation.none())) {
            var field = source.getClass().getDeclaredField("channel");
            field.setAccessible(true);
            ((Channel) field.get(source)).close();
            assertEquals(4, source.read(0, 4, Cancellation.none()).remaining());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"a:b.parquet", "C:part.parquet", "CON", "nul.parquet", "Com1.txt", "part.", "part ", "a?b", "a*b", "a|b", "a<b", "a\"b"})
    void segmentsWindowsWouldReinterpretAreRefused(String segment) throws IOException {
        Path root = dataset();
        LocalStorage storage = plain(new PlainDirectoryFileSystem(true), root);
        IOException e = assertThrows(IOException.class, () -> storage.openPart(segment, 0, Cancellation.none()));
        assertTrue(e.getMessage().contains("cannot be resolved safely"), e.getMessage());
        IOException d = assertThrows(IOException.class, () -> storage.openPart(segment + "/part-00000.parquet", 0, Cancellation.none()));
        assertTrue(d.getMessage().contains("cannot be resolved safely"), d.getMessage());
    }

    /** Issue #45: on Windows the default file system has no secure directory streams, and still reads datasets. */
    @Test
    @EnabledOnOs(value = OS.WINDOWS, disabledReason = "the default file system has secure directory streams elsewhere")
    void theWindowsDefaultFileSystemReadsADataset() throws IOException {
        Path root = dataset();
        assertFalse(PlainDirectoryFileSystem.secure(root));
        try (Dataset ds = Dataset.open(root)) {
            ds.verify();
            assertEquals(List.of("a", "b", "c", "d", "e"), keys(ds));
        }
    }

    @Test
    @EnabledOnOs(value = OS.WINDOWS, disabledReason = "directory junctions are a Windows feature")
    void aJunctionedDirectoryIsRefusedOnWindows() throws Exception {
        Path root = dataset();
        Path moved = tmp.resolve("moved");
        Files.move(root.resolve("data"), moved);
        // A junction (a mount-point reparse point) needs no privilege to create, unlike a symbolic link.
        Process mklink = new ProcessBuilder("cmd", "/c", "mklink", "/J", root.resolve("data").toString(), moved.toString())
                .redirectErrorStream(true).start();
        String output = new String(mklink.getInputStream().readAllBytes());
        assertEquals(0, mklink.waitFor(), output);
        IOException e = assertThrows(IOException.class, () -> LocalStorage.of(root).openPart("data/part-00000.parquet", 0, Cancellation.none()));
        assertTrue(e.getMessage().contains("symbolic link, junction or other reparse point"), e.getMessage());
        try (Dataset ds = Dataset.open(root)) {
            assertInstanceOf(StorageException.class, assertThrows(SeekParquetException.class, () -> ds.rows(Reads.all(10))));
        }
    }

    @Test
    void plainRelativePathRulesAreThoseOfTheSecurePath() throws IOException {
        LocalStorage storage = plain(new PlainDirectoryFileSystem(true), dataset());
        for (String path : List.of("/abs", "../x", "data/../manifest.json", "a//b", "a/./b", "a\\b", "a\u0001b", "")) {
            IOException e = assertThrows(IOException.class, () -> storage.openPart(path, 0, Cancellation.none()), path);
            assertTrue(e.getMessage().contains("not a plain relative path"), e.getMessage());
        }
    }
}
