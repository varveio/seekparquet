// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Opening applies the CHECK-1 checks of the specification: the manifest (§3); that every part
 * exists with the manifest's exact size; that every footer is readable, self-contained and
 * unencrypted, with row counts that add up to the footer's and the manifest's; the footer stamps;
 * the key column (KEY-1, KEY-5); that columns agree across parts (COL-5); and that every non-empty
 * row group is sorted by key (RG-1). It reads the manifest and one footer per part, nothing else,
 * unless {@link OpenOptions#verifyPartBounds()} asks it to compare each part's first and last key
 * with the manifest.
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
 * A {@code Dataset} is immutable after opening, apart from caches of checked page indexes, and safe
 * for concurrent requests from many threads. Results are immutable. {@link #close()} cancels the
 * requests in progress (they end with {@link CancellationException}), waits for them to return, and
 * closes every part source; requests after it fail with {@link IllegalStateException}. The {@link
 * DatasetStorage} is not closed: it belongs to the caller.
 */
public final class Dataset implements AutoCloseable {

    private final Manifest manifest;
    private final PartFile[] parts;
    private final Limits limits;
    private final List<Column> columns;
    /** Every column name some part has, mapped to whether it can be projected (COL-3). */
    private final Map<String, Boolean> projectable;

    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final Cancellation closing = Cancellation.create();
    private boolean closed;

    private Dataset(Manifest manifest, PartFile[] parts, Limits limits, List<Column> columns, Map<String, Boolean> projectable) {
        this.manifest = manifest;
        this.parts = parts;
        this.limits = limits;
        this.columns = columns;
        this.projectable = projectable;
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
     * Opens a dataset (CHECK-1). On failure, every part source opened so far is closed.
     *
     * @param storage the dataset root
     * @param options the options
     * @param cancellation ends the opening early when cancelled
     * @return the open dataset
     * @throws RefusalException if the dataset breaks the specification
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
        PartFile[] parts = openParts(storage, manifest, options, cancellation);
        try {
            return assemble(manifest, parts, options, cancellation);
        } catch (RuntimeException | Error e) {
            closeAll(Arrays.asList(parts), e);
            throw e;
        }
    }

    private static PartFile[] openParts(DatasetStorage storage, Manifest manifest, OpenOptions options, Cancellation cancellation) {
        int n = manifest.parts().size();
        PartFile[] parts = new PartFile[n];
        IndexMemory indexMemory = new IndexMemory(options.limits().maxIndexMemoryBytes());
        if (options.openConcurrency() == 1 || n <= 1) {
            for (int i = 0; i < n; i++) {
                try {
                    parts[i] = PartFile.open(storage, manifest, i, new ReadContext(cancellation, options.limits()), indexMemory);
                } catch (RuntimeException | Error e) {
                    closeAll(Arrays.asList(parts), e);
                    throw e;
                }
            }
            return parts;
        }
        // A bounded number of workers own their results until every worker has drained.
        // Never cancel a Future: cancellation can discard a successfully opened resource.
        Cancellation scope = Cancellation.linkedTo(cancellation);
        AtomicInteger next = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        Throwable failure = null;
        boolean interrupted = false;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int worker = 0; worker < Math.min(n, options.openConcurrency()); worker++) {
                futures.add(executor.submit(() -> {
                    for (int ordinal; (ordinal = next.getAndIncrement()) < n;) {
                        scope.throwIfCancelled();
                        parts[ordinal] = PartFile.open(storage, manifest, ordinal, new ReadContext(scope, options.limits()), indexMemory);
                    }
                }));
            }
            for (Future<?> future : futures) {
                boolean done = false;
                while (!done) {
                    try {
                        future.get();
                        done = true;
                    } catch (ExecutionException e) {
                        if (failure == null) {
                            failure = e.getCause();
                        }
                        scope.cancel();
                        done = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                        scope.cancel();
                        if (failure == null) {
                            failure = new CancellationException("interrupted");
                        }
                    }
                }
            }
        } finally {
            scope.detach();
            if (failure != null) {
                closeAll(Arrays.asList(parts), failure);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (failure instanceof RuntimeException re) {
            throw re;
        }
        if (failure instanceof Error err) {
            throw err;
        }
        if (failure != null) {
            throw new IllegalStateException(failure);
        }
        return parts;
    }

    private static Dataset assemble(Manifest manifest, PartFile[] parts, OpenOptions options, Cancellation cancellation) {
        // COL-5 for every column, on open.
        Map<String, String> signatures = new HashMap<>();
        Map<String, String> owner = new HashMap<>();
        Map<String, Column> columns = new LinkedHashMap<>();
        Map<String, Boolean> projectable = new HashMap<>();
        for (PartFile part : parts) {
            for (PartSchema.Field f : part.schema.fields) {
                String want = signatures.putIfAbsent(f.name(), f.signature());
                if (want == null) {
                    owner.put(f.name(), part.name);
                } else if (!want.equals(f.signature())) {
                    throw new RefusalException(RefusalCode.INCONSISTENT_COLUMN, part.name, "column " + f.name()
                            + " differs in physical type, repetition or nesting from part " + owner.get(f.name()));
                }
            }
            for (String name : part.schema.byName.keySet()) {
                PartSchema.Field f = part.schema.byName.get(name);
                Column prior = columns.get(name);
                columns.put(name, new Column(name, f.type(), f.typeLength(), f.repetition(), prior == null ? 1 : prior.parts() + 1));
                boolean ok = part.projectable.containsKey(name);
                projectable.merge(name, ok, Boolean::logicalAnd);
            }
        }
        if (options.verifyPartBounds()) {
            for (PartFile part : parts) {
                verifyBounds(part, new ReadContext(cancellation, options.limits()));
            }
        }
        return new Dataset(manifest, parts, options.limits(), List.copyOf(columns.values()), Map.copyOf(projectable));
    }

    /** CHECK-3: compares a part's decoded first and last keys with its manifest bounds. */
    private static void verifyBounds(PartFile part, ReadContext ctx) {
        int firstGroup = part.groups[0];
        KeyIndex first = part.keyIndex(firstGroup, ctx);
        byte[][] keys = part.keyPage(firstGroup, first, 0, ctx);
        if (!Arrays.equals(keys[0], part.entry.minKey().unsafeArray())) {
            throw new RefusalException(RefusalCode.BOUNDS_MISMATCH, part.name, "the first key differs from the manifest's min_key");
        }
        int lastGroup = part.groups[part.groups.length - 1];
        KeyIndex last = part.keyIndex(lastGroup, ctx);
        if (lastGroup != firstGroup || last.pages() > 1) {
            byte[][] lastKeys = part.keyPage(lastGroup, last, last.pages() - 1, ctx);
            int order = Arrays.compareUnsigned(keys[keys.length - 1], lastKeys[0]);
            if (order > 0 || (part.unique && order == 0)) {
                throw new RefusalException(RefusalCode.KEY_ORDER, part.name, "the last key page does not follow the first key page");
            }
            keys = lastKeys;
        }
        if (!Arrays.equals(keys[keys.length - 1], part.entry.maxKey().unsafeArray())) {
            throw new RefusalException(RefusalCode.BOUNDS_MISMATCH, part.name, "the last key differs from the manifest's max_key");
        }
    }

    private static void closeAll(List<PartFile> parts, Throwable failure) {
        for (PartFile p : parts) {
            if (p == null) {
                continue;
            }
            try {
                p.close();
            } catch (IOException | RuntimeException e) {
                if (failure != null) {
                    failure.addSuppressed(e);
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
     * {@return the top-level columns of the parts, the key included} They are in the order they
     * first appear, parts taken in manifest order.
     */
    public List<Column> columns() {
        return columns;
    }

    /**
     * Returns the description of a top-level column, when some part has it.
     *
     * @param name the column name
     * @return the column, or empty when no part has it
     */
    public Optional<Column> column(String name) {
        for (Column c : columns) {
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
     * @throws RefusalException if data the request decodes breaks the specification
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
     * @throws RefusalException if data the request decodes breaks the specification
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
     * @throws RefusalException if data the request decodes breaks the specification
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
     * @throws RefusalException if data the request decodes breaks the specification
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

    /** COL-3: a nested, repeated or INT96 column is rejected, never answered as absent. */
    private void checkProjection(List<String> names) {
        for (String name : names) {
            Boolean ok = projectable.get(name);
            if (ok != null && !ok) {
                throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_PROJECTION,
                        "column " + name + " is nested, repeated, of type INT96, or not unique in its part");
            }
        }
    }

    /** A request in progress: it holds a read lease that close waits for. */
    private final class Request implements AutoCloseable {
        final ReadContext ctx;
        private final Cancellation cancellation;

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
     * part source. Calling it again does nothing.
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
            IOException failure = null;
            for (PartFile p : parts) {
                try {
                    p.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw new StorageException("close the dataset's parts", failure);
            }
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return "Dataset[" + manifest.schemaId() + ", " + parts.length + " parts]";
    }
}
