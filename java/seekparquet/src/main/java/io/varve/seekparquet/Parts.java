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
    /** The parts' first-row ordinals, computed on first use (a racy but idempotent publication). */
    private volatile long[] starts;
    /** COL-5: each top-level column's signature in the checked parts, and the first part holding it. */
    private final Map<String, Signature> signatures = new HashMap<>();

    private record Signature(String signature, String part) {}

    /** One part: once checked, the open part; or the refusal its check reached, kept for the dataset's lifetime. */
    private static final class Slot {
        final ReentrantLock check = new ReentrantLock();
        volatile PartFile part;
        /** Written under {@code check}. */
        volatile RuntimeException refusal;
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

    /**
     * {@return the dataset ordinal of part i's first row, by the manifest's row counts; the dataset's row
     * count for i = size()}
     *
     * @throws ArithmeticException if the dataset holds more than {@link Long#MAX_VALUE} rows
     */
    long start(int i) {
        long[] s = starts;
        if (s == null) {
            s = new long[slots.length + 1];
            for (int j = 0; j < slots.length; j++) {
                s[j + 1] = Math.addExact(s[j], entry(j).rows());
            }
            starts = s;
        }
        return s[i];
    }

    /** {@return the part holding dataset ordinal {@code ordinal}, below the dataset's row count} */
    int partAt(long ordinal) {
        int lo = 0;
        int hi = slots.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (start(mid + 1) > ordinal) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
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
        return get(i, ctx, null);
    }

    /** The part's own checks, made ahead of its turn by {@link #checkAll}: the opened part, or the failure they reached. */
    private record Staged(PartFile part, RuntimeException failure) {
        PartFile take() {
            if (failure != null) {
                throw failure;
            }
            return part;
        }

        void discard() {
            if (part != null) {
                try {
                    part.close();
                } catch (IOException | RuntimeException ignored) {
                    // An unused duplicate of a part; its failure to close says nothing about the dataset.
                }
            }
        }
    }

    /** {@link #get(int, ReadContext)}, taking the part's own checks from {@code staged} when given; an unused staged part is closed. */
    private PartFile get(int i, ReadContext ctx, Staged staged) {
        Slot slot = slots[i];
        boolean used = false;
        try {
            PartFile part = slot.part;
            if (part != null) {
                return part;
            }
            lock(slot.check, ctx);
            try {
                // A waiter cancelled meanwhile ends with its cancellation, not with another request's outcome.
                ctx.checkCancelled();
                if (slot.part != null) {
                    return slot.part;
                }
                if (slot.refusal != null) {
                    throw slot.refusal;
                }
                used = true;
                try {
                    part = admit(staged != null ? staged.take() : PartFile.open(storage, manifest, i, ctx, indexMemory));
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
        } finally {
            if (!used && staged != null) {
                staged.discard();
            }
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

    /** COL-5, checked last so that only parts that pass every other check set the signatures. Closes the part on failure. */
    private PartFile admit(PartFile part) {
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
     * Checks every part no request has checked, with the outcome checking them one by one in manifest
     * order would give: the parts' own checks (their footers) run {@code concurrency} at a time on
     * virtual threads, then each part is admitted in manifest order, COL-5 included, until the first
     * failure, which is thrown. Once a part's own checks fail, no further part is started; parts after
     * the failure that were opened ahead are closed unchecked. An interrupt cancels the checks in
     * progress through their cancellation rather than by interrupting them, and ends with {@link
     * CancellationException}.
     */
    void checkAll(int concurrency, Cancellation cancellation) {
        int n = slots.length;
        ReadContext ctx = new ReadContext(cancellation, limits);
        if (concurrency == 1 || n <= 1) {
            for (int i = 0; i < n; i++) {
                get(i, ctx);
            }
            return;
        }
        Staged[] staged = new Staged[n];
        Cancellation scope = Cancellation.linkedTo(cancellation);
        AtomicInteger next = new AtomicInteger();
        AtomicBoolean failed = new AtomicBoolean();
        List<Future<?>> workers = new ArrayList<>();
        boolean interrupted = false;
        Throwable error = null;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int worker = 0; worker < Math.min(n, concurrency); worker++) {
                workers.add(executor.submit(() -> {
                    for (int i; !failed.get() && (i = next.getAndIncrement()) < n;) {
                        if (slots[i].part != null || slots[i].refusal != null) {
                            continue; // its outcome is known: admitting it in turn reuses it
                        }
                        try {
                            staged[i] = new Staged(PartFile.open(storage, manifest, i, new ReadContext(scope, limits), indexMemory), null);
                        } catch (RuntimeException e) {
                            staged[i] = new Staged(null, e);
                            failed.set(true);
                        }
                    }
                }));
            }
            // Waiting for every worker publishes what they staged.
            for (Future<?> worker : workers) {
                while (true) {
                    try {
                        worker.get();
                        break;
                    } catch (ExecutionException e) {
                        error = e.getCause();
                        failed.set(true);
                        break;
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
        int i = 0;
        try {
            if (interrupted) {
                throw new CancellationException("interrupted");
            }
            if (error instanceof Error e) {
                throw e;
            }
            if (error != null) {
                throw new IllegalStateException(error);
            }
            for (; i < n; i++) {
                Staged s = staged[i];
                staged[i] = null; // get closes it when unused
                get(i, ctx, s);
            }
        } finally {
            for (; i < n; i++) {
                if (staged[i] != null) {
                    staged[i].discard();
                }
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
