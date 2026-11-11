// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A dataset held in memory, read as an object store would serve it: every part read can wait a set delay,
 * as a round trip would, ending early when its cancellation is cancelled. It counts the reads, their bytes,
 * the reads in flight and the most in flight at once, and notes the threads that read ahead for a listing.
 * Nothing stays open on disk, so tests can change the bytes freely on any platform.
 */
final class MemoryStorage implements DatasetStorage {

    /** Called as each part read begins, on the reading thread; it may throw to fail the read. */
    @FunctionalInterface
    interface Hook {
        void before(String path, long position, int length) throws IOException;
    }

    private final byte[] manifest;
    final Map<String, byte[]> parts = new ConcurrentHashMap<>();
    /** The wait of each read, in nanoseconds; {@link Long#MAX_VALUE} waits until the read's cancellation. */
    volatile long delayNanos;
    /** When set, the wait of a read made by a read-ahead thread instead of {@link #delayNanos}. */
    volatile Long readAheadDelayNanos;
    volatile Hook hook;
    final AtomicLong reads = new AtomicLong();
    final AtomicLong bytes = new AtomicLong();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();
    final AtomicInteger readAheadInFlight = new AtomicInteger();
    final AtomicInteger maxReadAheadInFlight = new AtomicInteger();
    /** When set, a read made by a read-ahead thread waits until it is opened (or the read's cancellation is cancelled). */
    volatile java.util.concurrent.CompletableFuture<Void> readAheadGate;
    /** The read-ahead threads that read. */
    final Set<Thread> readAheadThreads = ConcurrentHashMap.newKeySet();

    private MemoryStorage(byte[] manifest) {
        this.manifest = manifest;
    }

    /** {@return the dataset written under {@code root}, read into memory} */
    static MemoryStorage of(Path root) {
        try {
            MemoryStorage s = new MemoryStorage(Files.readAllBytes(root.resolve("manifest.json")));
            try (var files = Files.list(root.resolve("data"))) {
                for (Path p : (Iterable<Path>) files::iterator) {
                    s.parts.put("data/" + p.getFileName(), Files.readAllBytes(p));
                }
            }
            return s;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@return a storage over the same manifest with copies of the parts, counting afresh} */
    MemoryStorage copy() {
        MemoryStorage s = new MemoryStorage(manifest);
        parts.forEach((k, v) -> s.parts.put(k, v.clone()));
        return s;
    }

    /** Zeroes the counts, for a measurement after opening. */
    void reset() {
        reads.set(0);
        bytes.set(0);
        maxInFlight.set(0);
    }

    static boolean onReadAheadThread() {
        return Thread.currentThread().getName().startsWith("seekparquet-read-ahead-");
    }

    @Override
    public byte[] readManifest(int maxBytes, Cancellation cancellation) {
        return manifest.clone();
    }

    @Override
    public PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException {
        byte[] raw = parts.get(path);
        if (raw == null) {
            throw new NoSuchFileException(path);
        }
        return new PartSource() {
            @Override public String path() { return path; }
            @Override public long size() { return raw.length; }
            @Override public Optional<String> version() { return Optional.empty(); }
            @Override public void close() { }

            @Override
            public ByteBuffer read(long position, int length, Cancellation c) throws IOException {
                if (position < 0 || length < 0 || position > raw.length - length) {
                    throw new EOFException("read outside the part");
                }
                boolean ahead = onReadAheadThread();
                reads.incrementAndGet();
                bytes.addAndGet(length);
                enter(inFlight, maxInFlight);
                if (ahead) {
                    readAheadThreads.add(Thread.currentThread());
                    enter(readAheadInFlight, maxReadAheadInFlight);
                }
                try {
                    Hook h = hook;
                    if (h != null) {
                        h.before(path, position, length);
                    }
                    java.util.concurrent.CompletableFuture<Void> gate = readAheadGate;
                    if (ahead && gate != null) {
                        awaitGate(c, gate);
                    }
                    Long special = readAheadDelayNanos;
                    pause(c, ahead && special != null ? special : delayNanos);
                    return ByteBuffer.wrap(raw, (int) position, length).asReadOnlyBuffer();
                } finally {
                    if (ahead) {
                        leave(readAheadInFlight);
                    }
                    leave(inFlight);
                }
            }
        };
    }

    /**
     * Counts a read in: the maximum is raised before the count, under one lock, so that whoever sees the count
     * at n sees a maximum of at least n.
     */
    private synchronized void enter(AtomicInteger count, AtomicInteger max) {
        int n = count.get() + 1;
        max.accumulateAndGet(n, Math::max);
        count.set(n);
    }

    private synchronized void leave(AtomicInteger count) {
        count.decrementAndGet();
    }

    private static void pause(Cancellation c, long nanos) throws IOException {
        c.throwIfCancelled();
        if (nanos <= 0) {
            return;
        }
        CountDownLatch cancelled = new CountDownLatch(1);
        try (Cancellation.Registration r = c.onCancel(cancelled::countDown)) {
            boolean ended = nanos == Long.MAX_VALUE ? awaitForever(cancelled) : cancelled.await(nanos, TimeUnit.NANOSECONDS);
            if (ended) {
                throw new CancellationException("cancelled");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted");
        }
    }

    /** Waits for {@code gate} to open, or {@code c} to be cancelled. */
    private static void awaitGate(Cancellation c, java.util.concurrent.CompletableFuture<Void> gate) throws IOException {
        var cancelled = new java.util.concurrent.CompletableFuture<Void>();
        try (Cancellation.Registration r = c.onCancel(() -> cancelled.complete(null))) {
            java.util.concurrent.CompletableFuture.anyOf(gate, cancelled).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IOException(e.getCause());
        }
        c.throwIfCancelled();
    }

    private static boolean awaitForever(CountDownLatch latch) throws InterruptedException {
        latch.await();
        return true;
    }
}
