// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * How one operation runs, apart from what it reads: its cancellation and where its read statistics go.
 * {@link Dataset#rows}, {@link Dataset#scan}, {@link Dataset#get}, {@link Dataset#plan}, {@link Dataset#list},
 * {@link Dataset#verify(ReadOptions)}, {@link Dataset#columns(int, ReadOptions)}, {@link Plan#scan} and {@link
 * Plan#split} take one; opening a dataset and {@link Dataset#buildListIndex} do not (they take a {@link
 * Cancellation}, and building reports its own {@link ListIndexStats}). The Go reader passes a {@code
 * context.Context} and a {@code *ReadStats} instead.
 *
 * <p>{@code statistics} receives the operation's {@link ReadStats} exactly once, when it ends, whether it
 * succeeds or fails, including a failure before it read anything (zero counts), and after the operation
 * has released what it held (its read lease on the dataset, its reads ahead): an immutable snapshot it may
 * keep. It is called on the thread that called the operation. An exception the consumer throws when the
 * operation failed is added to the operation's exception as suppressed, never replacing it, and an invalid
 * argument of the operation is such a failure: the consumer still receives zero counts once. When the
 * operation succeeded the consumer's exception propagates instead of the result. Since the operation has
 * released the dataset, the consumer may close it, unless the operation runs inside an enclosing request on
 * the same thread (from a {@link BatchVisitor}), whose lease the close would wait for: that close fails with
 * {@link IllegalStateException}. A consumer that blocks holds back neither another thread's requests nor a
 * concurrent close. A {@code null} {@code ReadOptions} is a {@link NullPointerException} with nothing to
 * deliver to.
 *
 * @param cancellation ends the operation early when cancelled
 * @param statistics receives the operation's read statistics
 */
public record ReadOptions(Cancellation cancellation, Consumer<? super ReadStats> statistics) {

    private static final Consumer<ReadStats> DISCARD = stats -> {};

    /** No cancellation and no statistics. */
    public static final ReadOptions DEFAULT = new ReadOptions(Cancellation.none(), DISCARD);

    /**
     * Creates options.
     *
     * @param cancellation the cancellation; {@link Cancellation#none()} for none
     * @param statistics the statistics consumer
     */
    public ReadOptions {
        Objects.requireNonNull(cancellation, "cancellation");
        Objects.requireNonNull(statistics, "statistics");
    }

    /**
     * Returns options with {@code cancellation} and no statistics.
     *
     * @param cancellation the cancellation
     * @return the options
     */
    public static ReadOptions of(Cancellation cancellation) {
        return new ReadOptions(cancellation, DISCARD);
    }

    /**
     * Returns options with no cancellation, delivering statistics to {@code statistics}.
     *
     * @param statistics the statistics consumer
     * @return the options
     */
    public static ReadOptions of(Consumer<? super ReadStats> statistics) {
        return new ReadOptions(Cancellation.none(), statistics);
    }
}
