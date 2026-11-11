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
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardCopyOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * A part or manifest replaced by a FIFO between LocalStorage's regular-file check and its open (by
 * whoever can write the dataset directory) must not block a request beyond cancellation, interruption
 * or {@link Dataset#close()}. The swap is made deterministic by a file system that delegates to the
 * default one and replaces the entry right after LocalStorage reads its attributes. Every test is
 * bounded: a regression fails on its timeout instead of hanging the build.
 */
@EnabledOnOs(value = {OS.LINUX, OS.MAC}, disabledReason = "a FIFO in the dataset directory, made with mkfifo, is a POSIX hazard "
        + "of the secure-directory-stream route; Windows has neither")
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalStorageSwapTest {

    private static final long BLOCKED_MILLIS = 300;
    private static final long BOUND_MILLIS = 10_000;

    @TempDir
    Path tmp;

    Path root;
    /** A second name for the FIFO, by which the test can open it for writing to release a stranded open. */
    Path fifo;
    Path regular;
    final List<Thread> threads = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        root = TestDatasets.write(tmp.resolve("ds"), new TestDatasets.Spec(), List.of(
                List.of(TestRow.of("a", 1), TestRow.of("b", 2), TestRow.of("c", 3)),
                List.of(TestRow.of("d", 4), TestRow.of("e", 5))));
        fifo = tmp.resolve("fifo");
        Process mkfifo = new ProcessBuilder("mkfifo", fifo.toString()).start();
        assumeTrue(mkfifo.waitFor() == 0, "mkfifo is not available");
        regular = tmp.resolve("regular");
    }

    @AfterEach
    void releaseStrandedOpens() throws InterruptedException {
        // Opening the FIFO for writing releases every open blocked on it. A daemon thread, since the
        // open itself blocks when nothing is reading.
        Thread release = Thread.ofPlatform().daemon().start(() -> {
            try (FileOutputStream w = new FileOutputStream(fifo.toFile())) {
                // opened: every blocked reader is released
            } catch (IOException ignored) {
                // nothing to release
            }
        });
        release.join(1_000);
        for (Thread t : threads) {
            t.join(BOUND_MILLIS);
        }
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS);
        while (blockedOpens() > 0 && System.nanoTime() < until) {
            Thread.sleep(5);
        }
    }

    /** Replaces {@code target} with the FIFO, keeping the FIFO's other name. */
    void swapInFifo(Path target) throws IOException {
        Path stage = tmp.resolve("stage");
        Files.deleteIfExists(stage);
        Files.createLink(stage, fifo);
        Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** Puts the regular file kept under {@link #regular} back at {@code target}. */
    void restoreRegular(Path target) throws IOException {
        Path stage = tmp.resolve("stage");
        Files.deleteIfExists(stage);
        Files.createLink(stage, regular);
        Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** Runs after LocalStorage reads an entry's attributes, before it opens the entry. */
    interface AfterStat {
        void run(Path name, BasicFileAttributes attributes) throws IOException;
    }

    /** A hook that replaces the entry {@code name} with the FIFO the first time it is checked as a regular file. */
    AfterStat swapOnce(Path target) {
        String name = target.getFileName().toString();
        boolean[] armed = {true};
        return (n, a) -> {
            synchronized (armed) {
                if (n.toString().equals(name) && a.isRegularFile() && armed[0]) {
                    armed[0] = false;
                    swapInFifo(target);
                }
            }
        };
    }

    Thread start(Runnable r) {
        Thread t = Thread.ofPlatform().daemon().start(r);
        threads.add(t);
        return t;
    }

    /** Runs {@code request} on its own thread and {@return its outcome: a result or the throwable} */
    AtomicReference<Object> run(Supplier<Object> request, Thread[] thread) {
        AtomicReference<Object> outcome = new AtomicReference<>();
        thread[0] = start(() -> {
            try {
                outcome.set(request.get());
            } catch (Throwable e) {
                outcome.set(e);
            }
        });
        return outcome;
    }

    /** Asserts that {@code t} is still waiting on the blocked open, then that it ends after {@code release}. */
    static void blockedUntil(Thread t, Runnable release) throws InterruptedException {
        t.join(BLOCKED_MILLIS);
        assertTrue(t.isAlive(), "the request should be waiting on the FIFO");
        release.run();
        t.join(BOUND_MILLIS);
        assertFalse(t.isAlive(), "the request did not end");
    }

    static void closes(Dataset ds) throws InterruptedException {
        Thread closer = Thread.ofPlatform().daemon().start(ds::close);
        closer.join(BOUND_MILLIS);
        assertFalse(closer.isAlive(), "close() did not return");
    }

    @Test
    void cancellingEndsARequestBlockedOnAPartSwappedForAFifo() throws Exception {
        Path part = root.resolve("data/part-00000.parquet");
        Dataset ds = Dataset.open(storage(LocalStorage.of(root), LocalStorage.of(hookedRoot(root, swapOnce(part)))));
        Cancellation cancel = Cancellation.create();
        Thread[] t = new Thread[1];
        AtomicReference<Object> outcome = run(() -> ds.range(RangeQuery.all(100), cancel), t);
        blockedUntil(t[0], cancel::cancel);
        assertInstanceOf(CancellationException.class, outcome.get());
        closes(ds);
    }

    @Test
    void closeEndsARequestBlockedOnAPartSwappedForAFifo() throws Exception {
        Path part = root.resolve("data/part-00000.parquet");
        Dataset ds = Dataset.open(storage(LocalStorage.of(root), LocalStorage.of(hookedRoot(root, swapOnce(part)))));
        Thread[] t = new Thread[1];
        AtomicReference<Object> outcome = run(() -> ds.range(RangeQuery.all(100)), t);
        t[0].join(BLOCKED_MILLIS);
        assertTrue(t[0].isAlive(), "the request should be waiting on the FIFO");
        closes(ds);
        t[0].join(BOUND_MILLIS);
        assertInstanceOf(CancellationException.class, outcome.get());
    }

    @Test
    void interruptingEndsARequestBlockedOnAPartSwappedForAFifo() throws Exception {
        Path part = root.resolve("data/part-00000.parquet");
        Dataset ds = Dataset.open(storage(LocalStorage.of(root), LocalStorage.of(hookedRoot(root, swapOnce(part)))));
        Thread[] t = new Thread[1];
        AtomicReference<Object> outcome = run(() -> ds.range(RangeQuery.all(100)), t);
        blockedUntil(t[0], t[0]::interrupt);
        assertInstanceOf(CancellationException.class, outcome.get());
        closes(ds);
    }

    @Test
    void closeEndsAVerificationBlockedOnAPartSwappedForAFifo() throws Exception {
        Path part = root.resolve("data/part-00001.parquet");
        DatasetStorage storage = storage(LocalStorage.of(root), LocalStorage.of(hookedRoot(root, swapOnce(part))));
        Dataset ds = Dataset.open(storage, OpenOptions.DEFAULT.withVerifyConcurrency(4), Cancellation.none());
        Thread[] t = new Thread[1];
        AtomicReference<Object> outcome = run(() -> {
            ds.verify();
            return "verified";
        }, t);
        t[0].join(BLOCKED_MILLIS);
        assertTrue(t[0].isAlive(), "verification should be waiting on the FIFO");
        closes(ds);
        t[0].join(BOUND_MILLIS);
        assertInstanceOf(CancellationException.class, outcome.get());
    }

    @Test
    void cancellingEndsAnOpenBlockedOnAManifestSwappedForAFifo() throws Exception {
        Path manifest = root.resolve("manifest.json");
        LocalStorage storage = LocalStorage.of(hookedRoot(root, swapOnce(manifest)));
        Cancellation cancel = Cancellation.create();
        Thread[] t = new Thread[1];
        AtomicReference<Object> outcome = run(() -> Dataset.open(storage, OpenOptions.DEFAULT, cancel), t);
        blockedUntil(t[0], cancel::cancel);
        assertInstanceOf(CancellationException.class, outcome.get());
    }

    @Test
    void strandedOpensAreBounded() throws Exception {
        Path part = root.resolve("data/part-00000.parquet");
        Files.createLink(regular, part);
        // While swapping, each check of the part as a regular file swaps the FIFO in; the test puts the
        // file back between requests.
        AtomicBoolean swapping = new AtomicBoolean(true);
        AfterStat everyTime = (n, a) -> {
            if (swapping.get() && n.toString().equals("part-00000.parquet") && a.isRegularFile()) {
                swapInFifo(part);
            }
        };
        DatasetStorage storage = storage(LocalStorage.of(root), LocalStorage.of(hookedRoot(root, everyTime)));
        long before = blockedOpens(); // opens other tests left that are still finishing
        for (int i = 0; i < LocalStorage.MAX_STRANDED_OPENS; i++) {
            restoreRegular(part);
            try (Dataset ds = Dataset.open(storage)) {
                Cancellation cancel = Cancellation.create();
                Thread[] t = new Thread[1];
                AtomicReference<Object> outcome = run(() -> ds.range(RangeQuery.all(100), cancel), t);
                // Once its open is blocked on the FIFO, cancelling strands it.
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS);
                while (blockedOpens() < before + i + 1) {
                    assertTrue(System.nanoTime() < until, "the open never blocked");
                    Thread.sleep(1);
                }
                cancel.cancel();
                t[0].join(BOUND_MILLIS);
                assertInstanceOf(CancellationException.class, outcome.get());
            }
        }
        restoreRegular(part);
        try (Dataset ds = Dataset.open(storage)) {
            StorageException e = assertThrows(StorageException.class, () -> ds.range(RangeQuery.all(100)));
            assertTrue(e.getCause().getMessage().contains("blocked"), e.getCause().getMessage());
        }
        // Releasing the stranded opens frees their slots: the part reads again through the same storage.
        swapping.set(false);
        releaseStrandedOpens();
        restoreRegular(part);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS);
        while (true) {
            try (Dataset ds = Dataset.open(storage)) {
                assertEquals(5, ds.range(RangeQuery.all(100)).size());
                break;
            } catch (StorageException e) {
                assertTrue(System.nanoTime() < deadline, "stranded opens were not released");
                Thread.sleep(20);
            }
        }
    }

    // A file system delegating to the default one, whose secure directory streams run a hook after
    // reading an entry's attributes. Only the root path LocalStorage is given belongs to it.

    static final class HookedStream implements SecureDirectoryStream<Path> {
        final SecureDirectoryStream<Path> d;
        final AfterStat hook;

        HookedStream(SecureDirectoryStream<Path> d, AfterStat hook) {
            this.d = d;
            this.hook = hook;
        }

        @Override
        public SecureDirectoryStream<Path> newDirectoryStream(Path p, LinkOption... o) throws IOException {
            return new HookedStream(d.newDirectoryStream(p, o), hook);
        }

        @Override
        public SeekableByteChannel newByteChannel(Path p, Set<? extends OpenOption> o, FileAttribute<?>... a) throws IOException {
            return d.newByteChannel(p, o, a);
        }

        @Override
        public void deleteFile(Path p) throws IOException {
            d.deleteFile(p);
        }

        @Override
        public void deleteDirectory(Path p) throws IOException {
            d.deleteDirectory(p);
        }

        @Override
        public void move(Path s, SecureDirectoryStream<Path> t, Path tp) throws IOException {
            d.move(s, t, tp);
        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Class<V> c) {
            return d.getFileAttributeView(c);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Path p, Class<V> c, LinkOption... o) {
            BasicFileAttributeView v = d.getFileAttributeView(p, BasicFileAttributeView.class, o);
            return (V) new BasicFileAttributeView() {
                @Override
                public String name() {
                    return v.name();
                }

                @Override
                public BasicFileAttributes readAttributes() throws IOException {
                    BasicFileAttributes a = v.readAttributes();
                    hook.run(p, a);
                    return a;
                }

                @Override
                public void setTimes(FileTime m, FileTime a, FileTime c) throws IOException {
                    v.setTimes(m, a, c);
                }
            };
        }

        @Override
        public Iterator<Path> iterator() {
            return d.iterator();
        }

        @Override
        public void close() throws IOException {
            d.close();
        }
    }

    static Path hookedRoot(Path real, AfterStat hook) {
        FileSystemProvider provider = new FileSystemProvider() {
            @Override public String getScheme() { return "hooked"; }
            @Override public FileSystem newFileSystem(URI u, Map<String, ?> e) { throw new UnsupportedOperationException(); }
            @Override public FileSystem getFileSystem(URI u) { throw new UnsupportedOperationException(); }
            @Override public Path getPath(URI u) { throw new UnsupportedOperationException(); }
            @Override public SeekableByteChannel newByteChannel(Path p, Set<? extends OpenOption> o, FileAttribute<?>... a) {
                throw new UnsupportedOperationException();
            }
            @Override public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> f) throws IOException {
                return new HookedStream((SecureDirectoryStream<Path>) Files.newDirectoryStream(real), hook);
            }
            @Override public void createDirectory(Path d, FileAttribute<?>... a) { throw new UnsupportedOperationException(); }
            @Override public void delete(Path p) { throw new UnsupportedOperationException(); }
            @Override public void copy(Path s, Path t, CopyOption... o) { throw new UnsupportedOperationException(); }
            @Override public void move(Path s, Path t, CopyOption... o) { throw new UnsupportedOperationException(); }
            @Override public boolean isSameFile(Path a, Path b) { throw new UnsupportedOperationException(); }
            @Override public boolean isHidden(Path p) { throw new UnsupportedOperationException(); }
            @Override public FileStore getFileStore(Path p) { throw new UnsupportedOperationException(); }
            @Override public void checkAccess(Path p, AccessMode... m) { throw new UnsupportedOperationException(); }
            @Override public <V extends FileAttributeView> V getFileAttributeView(Path p, Class<V> c, LinkOption... o) {
                throw new UnsupportedOperationException();
            }
            @Override public <A extends BasicFileAttributes> A readAttributes(Path p, Class<A> c, LinkOption... o) {
                throw new UnsupportedOperationException();
            }
            @Override public Map<String, Object> readAttributes(Path p, String a, LinkOption... o) { throw new UnsupportedOperationException(); }
            @Override public void setAttribute(Path p, String a, Object v, LinkOption... o) { throw new UnsupportedOperationException(); }
        };
        FileSystem fs = new FileSystem() {
            @Override public FileSystemProvider provider() { return provider; }
            @Override public void close() {}
            @Override public boolean isOpen() { return true; }
            @Override public boolean isReadOnly() { return true; }
            @Override public String getSeparator() { return "/"; }
            @Override public Iterable<Path> getRootDirectories() { return List.of(); }
            @Override public Iterable<FileStore> getFileStores() { return List.of(); }
            @Override public Set<String> supportedFileAttributeViews() { return Set.of("basic"); }
            @Override public Path getPath(String f, String... m) { throw new UnsupportedOperationException(); }
            @Override public PathMatcher getPathMatcher(String s) { throw new UnsupportedOperationException(); }
            @Override public UserPrincipalLookupService getUserPrincipalLookupService() { throw new UnsupportedOperationException(); }
            @Override public WatchService newWatchService() { throw new UnsupportedOperationException(); }
        };
        // The root path: LocalStorage normalizes it, resolves names against it for messages, and lists it.
        return (Path) Proxy.newProxyInstance(Path.class.getClassLoader(), new Class<?>[] {Path.class}, (proxy, m, args) -> switch (m.getName()) {
            case "toAbsolutePath", "normalize" -> proxy;
            case "getFileSystem" -> fs;
            case "resolve" -> args[0] instanceof Path p ? real.resolve(p) : real.resolve((String) args[0]);
            case "toString" -> real.toString();
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException(m.getName());
        });
    }

    /** {@return how many of LocalStorage's open threads are blocked opening a file} */
    static long blockedOpens() {
        return Thread.getAllStackTraces().entrySet().stream()
                .filter(e -> e.getKey().getName().equals("seekparquet-open"))
                .filter(e -> e.getValue().length > 0 && e.getValue()[0].getMethodName().startsWith("openat"))
                .count();
    }

    /** Reads the manifest through {@code manifests} and parts through {@code parts}. */
    static DatasetStorage storage(DatasetStorage manifests, DatasetStorage parts) {
        return new DatasetStorage() {
            @Override
            public byte[] readManifest(int max, Cancellation c) throws IOException {
                return manifests.readManifest(max, c);
            }

            @Override
            public PartSource openPart(String path, long size, Cancellation c) throws IOException {
                return parts.openPart(path, size, c);
            }
        };
    }
}
