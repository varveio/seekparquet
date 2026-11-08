// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.varve.seekparquet.storage.DatasetStorage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A dataset's parts in manifest order, each opened and checked the first time a request touches it
 * (CHECK-1). Requests route by the manifest's exact bounds and row counts, which need no part file.
 * Thread-safe: concurrent first touches of a part wait for one check.
 */
final class Parts {

    /** How long a request waits for another's check of a part before looking at its cancellation. */
    private static final long WAIT_MILLIS = 10;

    private final DatasetStorage storage;
    private final Manifest manifest;
    private final Limits limits;
    private final IndexMemory indexMemory;
    private final Slot[] slots;
    /** COL-5: each top-level column's signature in the checked parts, and the first part holding it. */
    private final Map<String, Signature> signatures = new HashMap<>();

    private record Signature(String signature, String part) {}

    /** One part: once checked, the open part; or the refusal its check reached, kept for the dataset's lifetime. */
    private static final class Slot {
        final ReentrantLock check = new ReentrantLock();
        volatile PartFile part;
        /** Guarded by {@code check}. */
        RuntimeException refusal;
    }

    Parts(DatasetStorage storage, Manifest manifest, Limits limits) {
        this.storage = storage;
        this.manifest = manifest;
        this.limits = limits;
        this.indexMemory = new IndexMemory(limits.maxIndexMemoryBytes());
        this.slots = new Slot[manifest.parts().size()];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new Slot();
        }
    }

    int size() {
        return slots.length;
    }

    /** {@return part i's manifest entry} */
    ManifestPart entry(int i) {
        return manifest.parts().get(i);
    }

    /** {@return part i if a request has checked it, without checking it} */
    PartFile checked(int i) {
        return slots[i].part;
    }

    /**
     * Returns part i once it is checked, checking it if no request has yet: it exists with the
     * manifest's size, its footer, row counts, stamps, key column and row groups pass CHECK-1, and its
     * columns agree with those of every part checked before it (COL-5). A refusal (or an unsupported
     * feature) reached while the request is live is kept: the part stays refused. Other failures
     * (storage, a limit, cancellation), and any failure once the request is cancelled, are not kept,
     * so a later request checks the part again.
     */
    PartFile get(int i, ReadContext ctx) {
        Slot slot = slots[i];
        PartFile part = slot.part;
        if (part != null) {
            return part;
        }
        lock(slot.check, ctx);
        try {
            if (slot.part != null) {
                return slot.part;
            }
            if (slot.refusal != null) {
                throw slot.refusal;
            }
            try {
                part = check(i, ctx);
            } catch (RefusalException | UnsupportedFeatureException e) {
                ctx.checkCancelled(); // a check cut short is not an outcome: the part stays unchecked
                slot.refusal = e;
                throw e;
            }
            slot.part = part;
            return part;
        } finally {
            slot.check.unlock();
        }
    }

    private static void lock(ReentrantLock lock, ReadContext ctx) {
        try {
            while (!lock.tryLock(WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                ctx.checkCancelled();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PartFile.cancelled(e);
        }
    }

    /** Opens and checks part i, COL-5 last so that only parts that pass every other check set the signatures. */
    private PartFile check(int i, ReadContext ctx) {
        PartFile part = PartFile.open(storage, manifest, i, ctx, indexMemory);
        try {
            synchronized (signatures) {
                for (PartSchema.Field f : part.schema.fields) {
                    Signature want = signatures.get(f.name());
                    if (want != null && !want.signature().equals(f.signature())) {
                        throw new RefusalException(RefusalCode.INCONSISTENT_COLUMN, part.name, "column " + f.name()
                                + " differs in physical type, repetition or nesting from part " + want.part());
                    }
                }
                for (PartSchema.Field f : part.schema.fields) {
                    signatures.putIfAbsent(f.name(), new Signature(f.signature(), part.name));
                }
            }
        } catch (RuntimeException | Error e) {
            try {
                part.close();
            } catch (IOException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        return part;
    }

    /**
     * Checks every part no request has checked, {@code concurrency} at a time on virtual threads, and
     * throws the failure of the first part in manifest order that failed. Once a part fails, no further
     * part is started; checks in progress finish. An interrupt cancels the checks in progress through
     * their cancellation rather than by interrupting them, and ends with {@link CancellationException}.
     */
    void checkAll(int concurrency, Cancellation cancellation) {
        int n = slots.length;
        if (concurrency == 1 || n <= 1) {
            for (int i = 0; i < n; i++) {
                get(i, new ReadContext(cancellation, limits));
            }
            return;
        }
        Throwable[] failures = new Throwable[n];
        Cancellation scope = Cancellation.linkedTo(cancellation);
        AtomicInteger next = new AtomicInteger();
        AtomicBoolean failed = new AtomicBoolean();
        List<Future<?>> workers = new ArrayList<>();
        boolean interrupted = false;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int worker = 0; worker < Math.min(n, concurrency); worker++) {
                workers.add(executor.submit(() -> {
                    for (int i; !failed.get() && (i = next.getAndIncrement()) < n;) {
                        try {
                            get(i, new ReadContext(scope, limits));
                        } catch (Throwable t) {
                            failures[i] = t;
                            failed.set(true);
                        }
                    }
                }));
            }
            // Waiting for every worker publishes their failures.
            for (Future<?> worker : workers) {
                while (true) {
                    try {
                        worker.get();
                        break;
                    } catch (ExecutionException e) {
                        throw new IllegalStateException(e.getCause()); // a worker catches every failure
                    } catch (InterruptedException e) {
                        interrupted = true;
                        scope.cancel();
                    }
                }
            }
        } finally {
            scope.detach();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (interrupted) {
            throw new CancellationException("interrupted");
        }
        for (Throwable t : failures) {
            if (t instanceof RuntimeException e) {
                throw e;
            }
            if (t instanceof Error e) {
                throw e;
            }
        }
    }

    /** Closes every checked part. {@return the first failure, later ones suppressed in it, or null} */
    IOException close() {
        IOException failure = null;
        for (Slot slot : slots) {
            PartFile part = slot.part;
            if (part == null) {
                continue;
            }
            try {
                part.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        return failure;
    }
}
