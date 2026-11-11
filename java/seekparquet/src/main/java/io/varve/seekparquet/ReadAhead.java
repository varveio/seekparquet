// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One listing's read-ahead of key pages ({@link ListQuery#readAhead()}). The listing's {@link KeyCursor}
 * predicts, from the checked key page index of the row group it is in, the key pages it will decode next,
 * and this reads their raw extents (header and payload, as their OffsetIndex entries locate them)
 * concurrently, each on a virtual thread of its own: at most {@code slots} reads in flight, at most
 * {@code slots} MiB held and at most {@code 4 * slots} pages pending. When the cursor decodes a page, {@link
 * PartFile#keyPage} takes the bytes read ahead for exactly that extent instead of reading it, and checks them
 * exactly as bytes it reads itself (CHECK-2): nothing is trusted because it was read ahead. A read ahead
 * that failed, or threw, is not used: the page is read again as if it had not been read ahead. A
 * prediction is a hint: a page predicted wrongly costs its bytes, never a result.
 *
 * <p>Only the request's thread calls its methods. A read's thread publishes its outcome through the page's
 * {@link CompletableFuture} and lowers the {@link AtomicInteger} of reads in flight before it does; every
 * other field is confined to the request's thread. The reads run under a {@link Cancellation} linked to
 * the request's, so that cancelling the request, or closing the dataset, ends them, and {@link #close()}
 * cancels it and waits for every read before the listing returns: none outlives the request, and the
 * dataset closes no part a read ahead still uses.
 */
final class ReadAhead implements AutoCloseable {

    /** The byte budget of each slot: a listing holds at most {@code slots} times this of pages read and not used. */
    static final int SLOT_BYTES = 1 << 20;

    /**
     * When positive, the read-ahead of a listing that asks for none. Tests set it to run every listing with
     * read-ahead; it is zero otherwise.
     */
    static volatile int testDefault;

    /** When set, runs as a read-ahead begins to close, before it cancels its reads and waits for them. For tests. */
    static volatile Runnable testClosing;

    /** When set, receives each read-ahead as it closes. For tests. */
    static volatile Consumer<ReadAhead> testClosed;

    private final ReadContext ctx;
    private final Cancellation scope;
    private final Cancellation.Registration wake;
    /** Completed when {@link #scope} is cancelled: a {@link #take} waiting for a read stops waiting. */
    private final CompletableFuture<Void> cancelled = new CompletableFuture<>();
    private final ExecutorService threads;
    final int slots;
    private final long budget;
    private final int maxPending;
    /** The largest extent a key page read reads ({@link Limits#maxPageBytes()}): a larger one is not read ahead. */
    private final long maxPage;
    /** Reads not yet ended: raised by the request's thread, lowered by the read's thread before it publishes. */
    private final AtomicInteger inFlight = new AtomicInteger();
    /** The bytes of the pages pending: in flight, or read and not yet taken or dropped. */
    private long held;
    /** The pages pending, by extent. */
    private final Map<Extent, Page> pending = new HashMap<>();
    /**
     * The pages pending in the order they were read ahead, with pages taken since left in place until they
     * reach the front or the queue is compacted: pages are used in the order the cursor reaches them, so
     * {@link #drop} looks at the front only.
     */
    private final ArrayDeque<Page> queue = new ArrayDeque<>();
    /** The pages read ahead and retired (taken or dropped), and those of them not used, for the request's counts. */
    private long reads, readBytes, unused, unusedBytes;
    private boolean closed;
    /** The page the last prediction after a row started from: a cursor stepping through rows predicts again only on a new page. */
    private PartFile lastPart;
    private int lastGroup = -1;
    private int lastPage = -1;

    /** The queue entries looked at by {@link #drop} and compaction. For tests: it grows with the pages, not their square. */
    long queueSteps;
    /** The most pages pending at once. For tests. */
    int mostPending;

    /** A page's extent in a part. */
    private record Extent(PartFile part, long offset, int size) {}

    /** The outcome of a read ahead: its bytes, or null when it failed; whether it reached the storage. */
    private record Outcome(byte[] raw, boolean read) {}

    /** One page read ahead: its row group and page locate it in the cursor's walk. */
    private static final class Page {
        final Extent extent;
        final int group;
        final int page;
        /**
         * The read's outcome, completed by its thread; set to null when the page is retired, so that a page
         * taken or dropped holds no bytes while it waits in the queue (only {@link #held} bytes stay reachable).
         */
        CompletableFuture<Outcome> outcome = new CompletableFuture<>();
        /** Taken or dropped: no longer pending, and counted. */
        boolean gone;

        Page(Extent extent, int group, int page) {
            this.extent = extent;
            this.group = group;
            this.page = page;
        }
    }

    private ReadAhead(ReadContext ctx, int slots) {
        this.ctx = ctx;
        this.slots = slots;
        this.budget = (long) slots * SLOT_BYTES;
        this.maxPending = 4 * slots;
        this.maxPage = ctx.limits.maxPageBytes();
        this.scope = Cancellation.linkedTo(ctx.cancellation);
        this.wake = scope.onCancel(() -> cancelled.complete(null));
        this.threads = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("seekparquet-read-ahead-", 0).factory());
    }

    /** {@return the read-ahead of a listing asking for {@code requested} pages, or null for none} */
    static ReadAhead open(ReadContext ctx, int requested) {
        int n = requested > 0 ? requested : testDefault;
        return n <= 0 ? null : new ReadAhead(ctx, Math.min(n, ListQuery.MAX_READ_AHEAD));
    }

    /**
     * Starts a prediction from page {@code page} of physical row group {@code group} of {@code part}, the
     * page the cursor is on, and forgets the pages read ahead that the cursor has passed. {@return false when
     * there is nothing to predict}: after a row ({@code afterRow}), on the page the last prediction started from.
     */
    boolean predictFrom(PartFile part, int group, int page, boolean afterRow) {
        if (closed || afterRow && part == lastPart && group == lastGroup && page == lastPage) {
            return false;
        }
        lastPart = part;
        lastGroup = group;
        lastPage = page;
        drop(part, group, page);
        return true;
    }

    /**
     * Reads ahead page {@code page} of physical row group {@code group} of {@code part}, the {@code size}
     * bytes at {@code offset}, unless it is pending already. {@return false when there is no room for it}:
     * every slot in flight, the budget held, the most pages pending, or a page too large.
     */
    boolean fetch(PartFile part, int group, int page, long offset, int size) {
        if (closed) {
            return false;
        }
        Extent extent = new Extent(part, offset, size);
        if (pending.containsKey(extent)) {
            return true;
        }
        if (size <= 0 || size > maxPage || held + size > budget || inFlight.get() >= slots || pending.size() >= maxPending) {
            return false;
        }
        Page e = new Page(extent, group, page);
        if (queue.size() >= 4 * maxPending) {
            queueSteps += queue.size();
            queue.removeIf(p -> p.gone);
        }
        pending.put(extent, e);
        queue.addLast(e);
        mostPending = Math.max(mostPending, pending.size());
        held += size;
        inFlight.incrementAndGet();
        try {
            // The read's thread is given the extent and the future, not the page, which the request's thread retires.
            CompletableFuture<Outcome> outcome = e.outcome;
            threads.execute(() -> read(extent, outcome));
        } catch (RejectedExecutionException failure) {
            // No thread for it: the page is read when the cursor reaches it.
            inFlight.decrementAndGet();
            e.outcome.complete(new Outcome(null, false));
        }
        return true;
    }

    /** Runs on the read's own thread. Whatever the storage throws, the outcome is published, and the read counted as ended. */
    private void read(Extent x, CompletableFuture<Outcome> outcome) {
        byte[] raw = null;
        boolean read = false;
        try {
            if (!scope.isCancelled()) {
                read = true;
                ByteBuffer b = x.part.readAhead(x.offset, x.size, scope);
                if (b != null && b.remaining() == x.size) {
                    raw = new byte[x.size];
                    b.duplicate().get(raw);
                }
            }
        } catch (Throwable failure) {
            // A failed read ahead, whatever the failure: the cursor reads the page again when it reaches it,
            // and meets the failure, if it repeats, as it would without a read-ahead.
            raw = null;
        } finally {
            inFlight.decrementAndGet();
            outcome.complete(new Outcome(raw, read));
        }
    }

    /**
     * {@return the bytes read ahead for the {@code size} bytes at {@code offset} in {@code part}}, waiting for
     * their read to end, or null when none were read ahead, their read failed, or the request was cancelled
     * meanwhile: the caller then reads the extent itself (and fails with the request).
     */
    byte[] take(PartFile part, long offset, int size) {
        if (pending.isEmpty()) {
            return null;
        }
        Page e = pending.get(new Extent(part, offset, size));
        if (e == null) {
            return null;
        }
        if (!e.outcome.isDone()) {
            try {
                CompletableFuture.anyOf(e.outcome, cancelled).get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException impossible) {
                return null;
            }
            if (!e.outcome.isDone()) {
                return null;
            }
        }
        byte[] raw = e.outcome.join().raw;
        retire(e, raw != null);
        return raw;
    }

    /** Removes {@code e}, which is pending and whose read has ended, and counts it. */
    private void retire(Page e, boolean used) {
        pending.remove(e.extent);
        held -= e.extent.size;
        e.gone = true;
        Outcome o = e.outcome.getNow(null);
        // Release the bytes now: the queue may keep the page until it reaches the front or is compacted.
        e.outcome = null;
        if (o == null || !o.read) {
            return;
        }
        reads++;
        readBytes += e.extent.size;
        if (!used) {
            unused++;
            unusedBytes += e.extent.size;
        }
    }

    /**
     * Forgets the pages read ahead that the cursor, now on page {@code page} of row group {@code group} of
     * {@code part}, has passed: pages of other row groups and pages at or before it, once their reads have
     * ended. They count as unused. Only the front of the queue is looked at, so a call costs the pages it
     * removes.
     */
    private void drop(PartFile part, int group, int page) {
        while (!queue.isEmpty()) {
            Page e = queue.peekFirst();
            queueSteps++;
            if (!e.gone) {
                if (e.extent.part == part && e.group == group && e.page > page || !e.outcome.isDone()) {
                    break;
                }
                retire(e, false);
            }
            queue.pollFirst();
        }
    }

    /**
     * Ends the reads still in flight, waits for every one, and adds the read-ahead's reads to the request's
     * counts: every read made in {@code sourceReads}, {@code sourceBytes} and {@code readAheadReads}; those not
     * used in {@code readAheadUnused} and {@code readAheadUnusedBytes}. It waits through an interrupt, which it
     * keeps, and interrupts no read: a read ahead is one bounded page read, ended by the cancellation where the
     * storage observes it. Calling it again does nothing.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Runnable closing = testClosing;
        if (closing != null) {
            closing.run();
        }
        try {
            scope.cancel();
        } catch (RuntimeException listener) {
            // A storage listener that throws breaks its contract; the read it meant to abort still ends.
        }
        threads.shutdown();
        boolean interrupted = false;
        while (true) {
            try {
                if (threads.awaitTermination(1, TimeUnit.DAYS)) {
                    break;
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        wake.close();
        scope.detach();
        for (Page e : pending.values().toArray(Page[]::new)) {
            retire(e, false);
        }
        queue.clear();
        ReadStats.Counter st = ctx.stats;
        st.sourceReads += reads;
        st.sourceBytes += readBytes;
        st.readAheadReads += reads;
        st.readAheadUnused += unused;
        st.readAheadUnusedBytes += unusedBytes;
        Consumer<ReadAhead> hook = testClosed;
        if (hook != null) {
            hook.accept(this);
        }
    }

    /**
     * {@return the bytes of pages read ahead that this read-ahead still references} For tests: at most
     * {@link #held}, whatever the queue holds.
     */
    long retainedBytes() {
        java.util.Set<Page> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        seen.addAll(queue);
        seen.addAll(pending.values());
        long bytes = 0;
        for (Page e : seen) {
            Outcome o = e.outcome == null ? null : e.outcome.getNow(null);
            if (o != null && o.raw != null) {
                bytes += o.raw.length;
            }
        }
        return bytes;
    }

    /** {@return the bytes of the pages pending} For tests. */
    long held() {
        return held;
    }

    /** {@return the reads in flight} For tests: zero once closed. */
    int inFlight() {
        return inFlight.get();
    }
}
