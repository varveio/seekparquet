// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * An explicit cancellation signal for opening a dataset or running a request.
 *
 * <p>A caller creates one with {@link #create()}, passes it to {@link Dataset} methods, and calls
 * {@link #cancel()} from any thread to end the work: the reader checks it between storage reads and
 * pages, and passes it to every {@link io.varve.seekparquet.storage.PartSource#read storage read}, so
 * that a storage adapter can abort an I/O call already in flight through {@link #onCancel}. Work that
 * observes the cancellation ends with {@link CancellationException}.
 *
 * <p>Thread interruption is honoured as well: a request whose thread is interrupted ends with {@link
 * CancellationException}, and the interrupt flag stays set.
 *
 * <p>A cancellation can be {@linkplain #linkedTo linked} to others, so that cancelling any of them
 * cancels it (a request scope tied to a service's shutdown, say). Instances are thread-safe. Once
 * cancelled, a cancellation stays cancelled.
 */
public final class Cancellation {

    private static final Cancellation NONE = new Cancellation(false);

    /** A registration that holds nothing. */
    private static final Registration NOOP = () -> {};

    private final boolean cancellable;
    private final List<Runnable> listeners = new ArrayList<>();
    private volatile String reason;
    /** The listeners this holds on the parents it is linked to; guarded by {@code listeners}. */
    private List<Registration> links = List.of();

    private Cancellation(boolean cancellable) {
        this.cancellable = cancellable;
    }

    /** {@return a cancellation that is never cancelled} */
    public static Cancellation none() {
        return NONE;
    }

    /** {@return a new cancellation, not yet cancelled} */
    public static Cancellation create() {
        return new Cancellation(true);
    }

    /**
     * Returns a new cancellation that is cancelled as soon as any of {@code parents} is (and at once,
     * if one already is). It can also be cancelled on its own, without affecting its parents.
     *
     * <p>The link holds a listener on each parent until the parent is cancelled; release it with
     * {@link #detach()} when a long-lived parent outlives this cancellation.
     *
     * @param parents the cancellations to follow
     * @return the linked cancellation
     */
    static Cancellation linkedTo(Cancellation... parents) {
        Cancellation child = create();
        List<Registration> links = new ArrayList<>(parents.length);
        for (Cancellation parent : parents) {
            Objects.requireNonNull(parent, "parent");
            links.add(parent.onCancel(() -> child.cancel(parent.reason)));
        }
        synchronized (child.listeners) {
            child.links = links;
        }
        return child;
    }

    /**
     * Releases the listeners this cancellation holds on the parents it was {@linkplain #linkedTo
     * linked} to. Afterwards it no longer follows them. Does nothing for an unlinked cancellation.
     */
    void detach() {
        List<Registration> l;
        synchronized (listeners) {
            l = links;
            links = List.of();
        }
        l.forEach(Registration::close);
    }

    /** Cancels: every listener runs once, on this thread, and later checks fail. */
    public void cancel() {
        cancel("cancelled");
    }

    private void cancel(String why) {
        if (!cancellable) {
            throw new UnsupportedOperationException("Cancellation.none() cannot be cancelled");
        }
        List<Runnable> run;
        synchronized (listeners) {
            if (reason != null) {
                return;
            }
            reason = why == null ? "cancelled" : why;
            run = new ArrayList<>(listeners);
            listeners.clear();
        }
        RuntimeException failure = null;
        for (Runnable r : run) {
            try {
                r.run();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Cancels with a reason that later {@link CancellationException}s carry. */
    void cancelWithReason(String why) {
        cancel(why);
    }

    /** {@return whether this has been cancelled. Thread interruption is not reflected here} */
    public boolean isCancelled() {
        return reason != null;
    }

    /**
     * Throws {@link CancellationException} if this has been cancelled or the current thread is
     * interrupted (leaving the interrupt flag set).
     *
     * @throws CancellationException if the work should stop
     */
    public void throwIfCancelled() {
        String why = reason;
        if (why != null) {
            throw new CancellationException(why);
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("interrupted");
        }
    }

    /**
     * Registers {@code listener} to run when this is cancelled: at once, on this thread, if it
     * already is; otherwise once, on the thread that cancels. Storage adapters use it to abort a
     * blocked call. A listener must be quick and must not throw.
     *
     * @param listener the action to run
     * @return a registration that removes the listener when closed
     */
    public Registration onCancel(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        if (!cancellable) {
            return NOOP;
        }
        synchronized (listeners) {
            if (reason == null) {
                listeners.add(listener);
                return () -> {
                    synchronized (listeners) {
                        listeners.remove(listener);
                    }
                };
            }
        }
        listener.run();
        return NOOP;
    }

    @Override
    public String toString() {
        return !cancellable ? "Cancellation.none()" : reason == null ? "Cancellation[active]" : "Cancellation[" + reason + "]";
    }

    /** A listener registration; closing it removes the listener. Closing twice is harmless. */
    @FunctionalInterface
    public interface Registration extends AutoCloseable {
        /** Removes the listener, if it has not run yet. */
        @Override
        void close();
    }
}
