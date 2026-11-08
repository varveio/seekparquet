// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An open SeekParquet dataset: a manifest and its parts, ready for range reads and delimited
 * listings.
 *
 * <pre>{@code
 * try (Dataset dataset = Dataset.open(LocalStorage.of(Path.of("/data/listing")))) {
 *     for (Row row : dataset.range(RangeQuery.prefix(Bytes.utf8("photos/"), 100))) {
 *         System.out.println(row.key().toStringUtf8());
 *     }
 * }
 * }</pre>
 *
 * <h2>What opening checks</h2>
 *
 * Opening checks the manifest (§3, CHECK-1) and reads nothing else: the manifest's exact part bounds
 * and row counts route every request. A part is checked the first time a request touches it, before
 * any of its data is used: that it exists with the manifest's exact size; that its footer is
 * readable, self-contained and unencrypted, with row counts that add up to the footer's and the
 * manifest's; the footer stamps; the key column (KEY-1, KEY-5); that its columns agree with those of
 * every part checked before it (COL-5); and that every non-empty row group is sorted by key (RG-1). A
 * request whose bounds the manifest places before a part does not touch it. A part's refusal ends the
 * request that touched it and is kept for as long as the dataset is open, so every later request that
 * touches the part is refused with the same code; storage failures and cancellation are not kept.
 * Concurrent first touches of a part wait for one check. {@link #verify()} checks every part now.
 *
 * <h2>What reads check</h2>
 *
 * Every request checks what it reads (CHECK-2): each row group's key page index before its first use
 * (RG-2 to RG-4), and each key page it decodes (row counts, non-empty and, for string keys, UTF-8
 * keys, order according to the manifest within the page and against every other page the request decoded, page
 * bounds). A failure ends the request with a {@link RefusalException}. A successful request does not
 * certify rows it did not decode (CHECK-4): that is validation's job.
 *
 * <h2>Thread safety and lifecycle</h2>
 *
 * A {@code Dataset} is immutable after opening, apart from its checked parts and caches of checked
 * page indexes, and safe for concurrent requests from many threads. Results are immutable. {@link
 * #close()} cancels the requests in progress (they end with {@link CancellationException}), waits for
 * them to return, and closes every part source it opened; requests after it fail with {@link
 * IllegalStateException}. The {@link DatasetStorage} is not closed: it belongs to the caller, and
 * must stay usable until the dataset is closed, since parts are opened when first touched.
 */
public final class Dataset implements AutoCloseable {

    private final Manifest manifest;
    private final Parts parts;
    private final Limits limits;
    private final int verifyConcurrency;

    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final Cancellation closing = Cancellation.create();
    private boolean closed;

    private Dataset(Manifest manifest, Parts parts, OpenOptions options) {
        this.manifest = manifest;
        this.parts = parts;
        this.limits = options.limits();
        this.verifyConcurrency = options.verifyConcurrency();
    }

    /**
     * Opens the dataset in the local directory {@code root}, with default options.
     *
     * @param root the dataset root
     * @return the open dataset
     * @throws RefusalException if the dataset breaks the specification
     * @throws UnsupportedFeatureException if it uses a feature this reader does not implement
     * @throws LimitExceededException if it declares a size above the default limits
     * @throws StorageException if reading the directory fails
     */
    public static Dataset open(Path root) {
        return open(LocalStorage.of(root));
    }

    /**
     * Opens a dataset with default options.
     *
     * @param storage the dataset root
     * @return the open dataset
     * @throws RefusalException if the dataset breaks the specification
     * @throws UnsupportedFeatureException if it uses a feature this reader does not implement
     * @throws LimitExceededException if it declares a size above the default limits
     * @throws StorageException if reading storage fails
     */
    public static Dataset open(DatasetStorage storage) {
        return open(storage, OpenOptions.DEFAULT, Cancellation.none());
    }

    /**
     * Opens a dataset, checking its manifest (CHECK-1). No part is opened until a request touches it.
     *
     * @param storage the dataset root
     * @param options the options
     * @param cancellation ends the opening early when cancelled
     * @return the open dataset
     * @throws RefusalException if the manifest breaks the specification
     * @throws UnsupportedFeatureException if it uses a feature this reader does not implement
     * @throws LimitExceededException if it declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled or interrupted
     */
    public static Dataset open(DatasetStorage storage, OpenOptions options, Cancellation cancellation) {
        Limits limits = options.limits();
        cancellation.throwIfCancelled();
        byte[] raw;
        try {
            raw = storage.readManifest(limits.maxManifestBytes(), cancellation);
        } catch (NoSuchFileException e) {
            throw new RefusalException(RefusalCode.NO_MANIFEST, null, "the dataset root has no manifest.json");
        } catch (InterruptedIOException e) {
            if (e instanceof java.net.SocketTimeoutException) {
                throw new StorageException("read manifest.json", e);
            }
            throw PartFile.cancelled(e);
        } catch (IOException e) {
            throw new StorageException("read manifest.json", e);
        }
        if (raw.length > limits.maxManifestBytes()) {
            throw new LimitExceededException("maxManifestBytes", raw.length, limits.maxManifestBytes(), "manifest.json");
        }
        Manifest manifest = Manifest.parse(raw);
        int n = manifest.parts().size();
        if (n > limits.maxParts()) {
            throw new LimitExceededException("maxParts", n, limits.maxParts(), "manifest.json");
        }
        return new Dataset(manifest, new Parts(storage, manifest, limits), options);
    }

    /**
     * Checks every part now, as the first request to touch each part would (CHECK-1), then decodes
     * every row group's first key page and each part's last, and compares each part's first and last
     * key with the manifest's bounds (CHECK-3). Parts already checked by a request are not checked
     * again; {@link OpenOptions#verifyConcurrency()} parts are checked at once. It reads one footer per
     * unchecked part, plus the key page index and one or two key pages per row group, never every row.
     *
     * @throws RefusalException if a part breaks the specification: the first such part in manifest
     *     order among those checked
     * @throws UnsupportedFeatureException if a part uses a feature this reader does not implement
     * @throws LimitExceededException if a part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public void verify() {
        verify(Cancellation.none());
    }

    /**
     * Checks every part now, ending early when {@code cancellation} is cancelled; see {@link
     * #verify()}.
     *
     * @param cancellation the request's cancellation
     * @throws RefusalException if a part breaks the specification
     * @throws UnsupportedFeatureException if a part uses a feature this reader does not implement
     * @throws LimitExceededException if a part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public void verify(Cancellation cancellation) {
        try (Request request = begin(cancellation)) {
            parts.checkAll(verifyConcurrency, request.cancellation);
            ReadContext ctx = request.ctx;
            for (int i = 0; i < parts.size(); i++) {
                verifyKeyPages(parts.get(i, ctx), ctx);
            }
        }
    }

    /** CHECK-3: decodes each row group's first key page and the part's last, against the manifest bounds. */
    private static void verifyKeyPages(PartFile part, ReadContext ctx) {
        for (int s = 0; s < part.groups.length; s++) {
            ctx.checkCancelled();
            int physical = part.groups[s];
            ctx.enterGroup(part.ordinal, physical);
            KeyIndex index = part.keyIndex(physical, ctx);
            byte[][] keys = part.keyPage(physical, index, 0, ctx);
            ctx.checkKeyPage(part, physical, 0, keys);
            if (s == 0 && !Arrays.equals(keys[0], part.entry.minKey().unsafeArray())) {
                throw new RefusalException(RefusalCode.BOUNDS_MISMATCH, part.name, "the first key differs from the manifest's min_key");
            }
            if (s == part.groups.length - 1) {
                if (index.pages() > 1) {
                    keys = part.keyPage(physical, index, index.pages() - 1, ctx);
                    ctx.checkKeyPage(part, physical, index.pages() - 1, keys);
                }
                if (!Arrays.equals(keys[keys.length - 1], part.entry.maxKey().unsafeArray())) {
                    throw new RefusalException(RefusalCode.BOUNDS_MISMATCH, part.name, "the last key differs from the manifest's max_key");
                }
            }
        }
    }

    /** {@return the dataset's manifest} */
    public Manifest manifest() {
        return manifest;
    }

    /**
     * Returns the number of rows recorded by the manifest as a signed 64-bit integer.
     *
     * @return the dataset row total
     * @throws ArithmeticException if the total exceeds {@link Long#MAX_VALUE}; use
     *     {@code manifest().rowsExact()} for the exact total
     */
    public long rows() {
        return manifest.rows();
    }

    /**
     * {@return the top-level columns of the parts checked so far, the key included} They are in the
     * order they first appear, parts taken in manifest order. After {@link #verify()} every part is
     * checked.
     */
    public List<Column> columns() {
        Map<String, Column> columns = new LinkedHashMap<>();
        for (int i = 0; i < parts.size(); i++) {
            PartFile part = parts.checked(i);
            if (part == null) {
                continue;
            }
            for (PartSchema.Field f : part.schema.byName.values()) {
                Column prior = columns.get(f.name());
                columns.put(f.name(), new Column(f.name(), f.type(), f.typeLength(), f.repetition(), prior == null ? 1 : prior.parts() + 1));
            }
        }
        return List.copyOf(columns.values());
    }

    /**
     * Returns the description of a top-level column, when some part checked so far has it.
     *
     * @param name the column name
     * @return the column, or empty when no checked part has it
     */
    public Optional<Column> column(String name) {
        for (Column c : columns()) {
            if (c.name().equals(name)) {
                return Optional.of(c);
            }
        }
        return Optional.empty();
    }

    /**
     * Reads a range (spec §6.2).
     *
     * @param query the query
     * @return the rows, in ascending key order, at most {@code query.limit()}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> range(RangeQuery query) {
        return range(query, Cancellation.none());
    }

    /**
     * Reads a range (spec §6.2), ending early when {@code cancellation} is cancelled.
     *
     * @param query the query
     * @param cancellation the request's cancellation
     * @return the rows, in ascending key order, at most {@code query.limit()}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> range(RangeQuery query, Cancellation cancellation) {
        checkResultLimit(query.limit());
        checkProjection(query.columns());
        try (Request request = begin(cancellation)) {
            return Queries.range(parts, query, request.ctx);
        }
    }

    /**
     * Lists keys with delimiter rollups (spec §6.3).
     *
     * @param query the query
     * @return the entries, in ascending byte order, at most {@code query.limit()}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public List<ListEntry> list(ListQuery query) {
        return list(query, Cancellation.none());
    }

    /**
     * Lists keys with delimiter rollups (spec §6.3), ending early when {@code cancellation} is
     * cancelled.
     *
     * @param query the query
     * @param cancellation the request's cancellation
     * @return the entries, in ascending byte order, at most {@code query.limit()}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<ListEntry> list(ListQuery query, Cancellation cancellation) {
        checkResultLimit(query.limit());
        checkProjection(query.columns());
        try (Request request = begin(cancellation)) {
            return Queries.list(parts, query, request.ctx);
        }
    }

    private void checkResultLimit(int limit) {
        if (limit > limits.maxResultEntries()) {
            throw new LimitExceededException("maxResultEntries", limit, limits.maxResultEntries(), "query");
        }
    }

    /** Looks up the equal-key group, returning at most {@code limit} rows in file order.
     * Use {@link RangeQuery#next(Row)} to continue a lookup range after a page.
     * @param key the exact key
     * @param limit the maximum rows returned
     * @param columns the columns to project
     * @return the group's rows, possibly limited
     */
    public List<Row> lookup(Bytes key, int limit, String... columns) {
        return range(RangeQuery.lookup(key, limit).toBuilder().columns(columns).build());
    }

    /**
     * COL-3: a nested, repeated or INT96 column of a checked part is rejected, never answered as
     * absent. A part checked later is held to the same rule when a row of it is projected.
     */
    private void checkProjection(List<String> names) {
        for (int i = 0; i < parts.size(); i++) {
            PartFile part = parts.checked(i);
            if (part != null) {
                part.checkProjection(names);
            }
        }
    }

    /** A request in progress: it holds a read lease that close waits for. */
    private final class Request implements AutoCloseable {
        final ReadContext ctx;
        final Cancellation cancellation;

        Request(Cancellation caller) {
            this.cancellation = Cancellation.linkedTo(caller, closing);
            this.ctx = new ReadContext(cancellation, limits);
        }

        @Override
        public void close() {
            cancellation.detach();
            lifecycle.readLock().unlock();
        }
    }

    private Request begin(Cancellation caller) {
        caller.throwIfCancelled();
        lifecycle.readLock().lock();
        if (closed) {
            lifecycle.readLock().unlock();
            throw new IllegalStateException("the dataset is closed");
        }
        try {
            return new Request(caller);
        } catch (RuntimeException | Error e) {
            lifecycle.readLock().unlock();
            throw e;
        }
    }

    /**
     * Closes the dataset: cancels the requests in progress, waits for them to return, and closes every
     * part source it opened. Calling it again does nothing.
     *
     * @throws StorageException if closing a part source fails (every source is still closed)
     */
    @Override
    public void close() {
        if (!closing.isCancelled()) {
            closing.cancelWithReason("the dataset was closed");
        }
        lifecycle.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = parts.close();
            if (failure != null) {
                throw new StorageException("close the dataset's parts", failure);
            }
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return "Dataset[" + manifest.schemaId() + ", " + parts.size() + " parts]";
    }
}
