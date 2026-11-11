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
import java.util.function.Consumer;

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
 * IllegalStateException}. A scan's action must not close the dataset itself: see {@link #close()}.
 *
 * <p>Each part a request touches stays open, with its checked footer in memory, until the dataset
 * closes; no limit bounds how many. {@link Limits#maxParts()} bounds the manifest and the index memory
 * budget the retained page indexes, while {@code verifyConcurrency} bounds how many parts {@link
 * #verify()} checks at a time, not how many stay open. A long-lived dataset that touches many parts,
 * or {@code verify()}, which touches them all, holds a source for each ({@link
 * io.varve.seekparquet.storage.LocalStorage} keeps a file channel and a directory handle per part): size
 * file descriptors and memory for the most parts one dataset will touch. A single pass over more parts
 * than that fails when the process runs out, and a fresh dataset for it does not help: there is no cap
 * that refuses it early, and no eviction. The {@link DatasetStorage} is not closed: it belongs to the caller, and
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
    private final java.util.concurrent.locks.ReentrantLock listBuild = new java.util.concurrent.locks.ReentrantLock();

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
            ctx.checkKeyPage(part, physical, 0, index.firstRow[0], keys);
            if (s == 0 && !Arrays.equals(keys[0], part.entry.minKey().unsafeArray())) {
                throw new RefusalException(RefusalCode.BOUNDS_MISMATCH, part.name, "the first key differs from the manifest's min_key", physical, -1, null);
            }
            if (s == part.groups.length - 1) {
                if (index.pages() > 1) {
                    keys = part.keyPage(physical, index, index.pages() - 1, ctx);
                    ctx.checkKeyPage(part, physical, index.pages() - 1, index.firstRow[index.pages() - 1], keys);
                }
                if (!Arrays.equals(keys[keys.length - 1], part.entry.maxKey().unsafeArray())) {
                    throw new RefusalException(RefusalCode.BOUNDS_MISMATCH, part.name, "the last key differs from the manifest's max_key", physical, -1, null);
                }
            }
        }
    }

    /**
     * Builds the optional slash listing index with default settings.
     * @return the build statistics
     */
    public ListIndexStats buildListIndex() {
        return buildListIndex(ListIndexOptions.DEFAULT, Cancellation.none());
    }

    /**
     * Decodes each key page once and atomically publishes a bounded index of directory runs.
     * Complete groups are admitted in dataset order, stopping before the first over-budget group.
     * Groups with data refusals or unsupported codecs/encodings remain on the ordinary read path.
     * Storage failures and resource limits fail
     * the attempt without publication and permit retry. Build-only indexes and first keys do not
     * consume the shared index-memory cache.
     * Concurrent lists use either the old path or the completed index. Cancellation discards this
     * attempt and permits retry. After a nonempty successful build, including budget exhaustion, subsequent
     * calls return its statistics with the effective {@link ListIndexStats#options() options}
     * and {@link ListIndexStats#optionsMismatch()} flag.
     * Empty outcomes are cached for the most recent options and rebuilt when those change.
     * {@link ListIndexStats#hasIndex()} reports whether any groups are indexed. Opening never builds it.
     *
     * @param options retained byte budget, worker count and per-page change cap
     * @param cancellation ends the build without publishing it
     * @return the build statistics
     * @throws CancellationException if cancelled, interrupted, or closed meanwhile
     * @throws IllegalStateException if already closed
     */
    public ListIndexStats buildListIndex(ListIndexOptions options, Cancellation cancellation) {
        try (Request request = begin(cancellation)) {
            while (!listBuild.tryLock()) {
                request.ctx.checkCancelled();
                try { Thread.sleep(10); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new CancellationException("interrupted"); }
            }
            try {
                request.ctx.checkCancelled();
                if (parts.listIndex == null || (parts.listIndex.groups.length == 0 && !parts.listIndex.stats.options().equals(options))) {
                    ListBoundaryIndex built = ListBoundaryIndex.build(parts, options, request.ctx);
                    request.ctx.checkCancelled();
                    parts.listIndex = built;
                    return built.stats;
                }
                return parts.listIndex.stats.forOptions(options);
            } finally {
                listBuild.unlock();
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
     * order they first appear, parts taken in manifest order, with the logical type of the first checked
     * part that has the column (COL-5 does not compare logical types). It checks no part: after {@link
     * #verify()} every part is checked. {@link #columns(int)} describes one part from its own footer,
     * checking it first if no read has.
     */
    public List<Column> columns() {
        Map<String, Column> columns = new LinkedHashMap<>();
        String key = manifest.key().column();
        for (int i = 0; i < parts.size(); i++) {
            PartFile part = parts.checked(i);
            if (part == null) {
                continue;
            }
            for (PartSchema.Field f : part.schema.byName.values()) {
                Column prior = columns.get(f.name());
                Column c = prior != null ? prior : PartSchema.column(f, key);
                columns.put(f.name(), new Column(c.name(), c.type(), c.typeLength(), c.logicalType(), c.repetition(), c.key(),
                        prior == null ? 1 : prior.parts() + 1));
            }
        }
        return List.copyOf(columns.values());
    }

    /**
     * Returns the top-level columns of part {@code part}; see {@link #columns(int, Cancellation)}.
     *
     * @param part an index into {@code manifest().parts()}
     * @return the part's columns, in schema order
     * @throws IndexOutOfBoundsException if {@code part} is not an index of the manifest's parts
     * @throws RefusalException if the part breaks the specification
     * @throws UnsupportedFeatureException if the part uses a feature this reader does not implement
     * @throws LimitExceededException if the part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Column> columns(int part) {
        return columns(part, Cancellation.none());
    }

    /**
     * Returns the top-level columns of part {@code part}, an index into {@code manifest().parts()}, in
     * schema order, from the footer the dataset checked and holds for that part: if no read has checked
     * the part yet, this checks it as the first read to touch it would (CHECK-1), so the answer always
     * comes from a checked part, and every later read of the part reads that same file. Each column has
     * {@code parts} 1. COL-5 holds a column's physical type, repetition and nesting equal in every
     * checked part that has it; a part may lack a column (COL-2), and logical annotations are not
     * compared, so a caller that needs a column in every part asks for each part.
     *
     * @param part an index into {@code manifest().parts()}
     * @param cancellation the request's cancellation
     * @return the part's columns, in schema order
     * @throws IndexOutOfBoundsException if {@code part} is not an index of the manifest's parts
     * @throws RefusalException if the part breaks the specification
     * @throws UnsupportedFeatureException if the part uses a feature this reader does not implement
     * @throws LimitExceededException if the part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Column> columns(int part, Cancellation cancellation) {
        java.util.Objects.checkIndex(part, parts.size());
        try (Request request = begin(cancellation)) {
            PartFile p = parts.get(part, request.ctx);
            String key = manifest.key().column();
            return p.schema.fields.stream().map(f -> PartSchema.column(f, key)).toList();
        }
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
        return range(query, cancellation, stats -> {});
    }

    /**
     * Reads a range (spec §6.2), ending early when {@code cancellation} is cancelled, and hands the
     * request's read statistics to {@code stats} when it ends, whether it succeeds or fails. A
     * request that fails before it reads (a limit or projection error, a cancellation, a closed dataset)
     * delivers zero counts.
     *
     * @param query the query
     * @param cancellation the request's cancellation
     * @param stats receives the request's read statistics
     * @return the rows, in ascending key order, at most {@code query.limit()}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> range(RangeQuery query, Cancellation cancellation, Consumer<? super ReadStats> stats) {
        Request started;
        try {
            checkResultLimit(query.limit());
            checkProjection(query.columns());
            started = begin(cancellation);
        } catch (RuntimeException e) {
            throw notStarted(stats, e);
        }
        try (Request request = started) {
            try {
                return Queries.range(parts, query, request.ctx);
            } finally {
                stats.accept(request.ctx.stats.snapshot());
            }
        }
    }

    /**
     * Hands {@code action} the rows of a range read as borrowed batches; see {@link #rangeBatches(RangeQuery,
     * Cancellation, Consumer, Consumer)}.
     *
     * @param query the query
     * @param action receives each batch, borrowed until it returns
     * @throws RefusalException if the data breaks the specification
     * @throws UnsupportedFeatureException if a projected column is nested, repeated or {@code INT96}, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if the limit or a declared size exceeds the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if the dataset is closed meanwhile, or the thread interrupted
     * @throws IllegalStateException if the dataset is closed
     */
    public void rangeBatches(RangeQuery query, Consumer<? super Batch> action) {
        rangeBatches(query, Cancellation.none(), stats -> {}, action);
    }

    /**
     * Hands {@code action} the rows of a range read as borrowed batches, ending early when {@code
     * cancellation} is cancelled; see {@link #rangeBatches(RangeQuery, Cancellation, Consumer, Consumer)}.
     *
     * @param query the query
     * @param cancellation the request's cancellation
     * @param action receives each batch, borrowed until it returns
     * @throws RefusalException if the data breaks the specification
     * @throws UnsupportedFeatureException if a projected column is nested, repeated or {@code INT96}, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if the limit or a declared size exceeds the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public void rangeBatches(RangeQuery query, Cancellation cancellation, Consumer<? super Batch> action) {
        rangeBatches(query, cancellation, stats -> {}, action);
    }

    /**
     * Hands {@code action}, in serial calls on this thread, the rows {@link #range(RangeQuery, Cancellation,
     * Consumer)} returns for {@code query}, as borrowed {@link Batch}es instead of {@link Row}s: its bounds,
     * cursor, columns and limit mean what they mean to a range read, and its errors are a range read's. It
     * plans nothing: it opens each projected column chunk when it reaches its row group, reads a dictionary
     * only for a page that uses it, and reads no OffsetIndex beyond the rows it hands over. It stops at the
     * limit without moving past it; a range with no rows never calls {@code action}.
     *
     * <p>{@link Batch} sets out the boundaries (never crossing a row group, a key page or a projected page;
     * the last batch ends at the limit or the upper bound) and the lifetime: a batch is read-only and
     * borrowed until {@code action} returns, and {@link Batch#row}, {@link Batch#continuation} make the
     * owning rows and continuations a range read returns. {@code stats} receives the request's read
     * statistics when it ends, whether it succeeds or fails (zero counts when it fails to start); when every
     * call of {@code action} returns normally they are those of the same range read against the same caches.
     *
     * <p>An exception {@code action} throws ends the request and propagates unchanged; batches already handed
     * over stand, as they do before a read error or a refusal. Cancellation is checked as a range read checks
     * it, and also before every call of {@code action}. The request holds the dataset open while {@code
     * action} runs: {@link #close()} cancels it and waits for {@code action} to return. Closing the dataset
     * from inside {@code action} fails with {@link IllegalStateException}, and {@code action} must not wait
     * for another thread that is closing it. Concurrent and nested requests have storage of their own.
     *
     * @param query the query
     * @param cancellation the request's cancellation
     * @param stats receives the request's read statistics
     * @param action receives each batch, borrowed until it returns
     * @throws RefusalException if the data breaks the specification
     * @throws UnsupportedFeatureException if a projected column is nested, repeated or {@code INT96}, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if the limit or a declared size exceeds the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public void rangeBatches(RangeQuery query, Cancellation cancellation, Consumer<? super ReadStats> stats,
            Consumer<? super Batch> action) {
        java.util.Objects.requireNonNull(action, "action");
        Request started;
        try {
            checkResultLimit(query.limit());
            checkProjection(query.columns());
            started = begin(cancellation);
        } catch (RuntimeException e) {
            throw notStarted(stats, e);
        }
        try (Request request = started) {
            try {
                Queries.rangeBatches(parts, query, request.ctx, action);
            } finally {
                stats.accept(request.ctx.stats.snapshot());
            }
        }
    }

    /**
     * Plans reading the rows of {@code ranges}; see {@link #planRanges(List, List, Cancellation)}.
     *
     * @param ranges the ranges, read whole
     * @param columns the columns to project, by exact top-level name, in order
     * @return the plan
     * @throws IllegalArgumentException if a range projects columns of its own
     * @throws RefusalException if data planning decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or uses a codec this
     *     reader does not implement
     * @throws LimitExceededException if a page or index declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public RangePlan planRanges(List<RangeQuery> ranges, List<String> columns) {
        return planRanges(ranges, columns, Cancellation.none());
    }

    /**
     * Plans reading the rows of {@code ranges}, with the values of {@code columns} (as a range read's
     * columns). It reads each range's prefix, bounds and cursor; a planned range is read whole, so its
     * limit is not applied, and a range with columns of its own is rejected, the plan's columns applying
     * to every range. Rows that several ranges select are one row of the plan.
     *
     * <p>Planning decodes what a range read starting at each end would: the key pages that locate the
     * first row of the range and the first row after it, through the routing and the key page index,
     * and no part known from the manifest bounds to begin after the range. With columns, it also checks
     * every part holding a selected row and reads the OffsetIndex of each projected column chunk the rows
     * span, but no data page. Errors are those of a range read.
     *
     * @param ranges the ranges, read whole
     * @param columns the columns to project, by exact top-level name, in order
     * @param cancellation the request's cancellation
     * @return the plan, bound to this dataset
     * @throws IllegalArgumentException if a range projects columns of its own
     * @throws RefusalException if data planning decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or uses a codec this
     *     reader does not implement
     * @throws LimitExceededException if a page or index declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws ArithmeticException if the dataset holds more than {@link Long#MAX_VALUE} rows
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public RangePlan planRanges(List<RangeQuery> ranges, List<String> columns, Cancellation cancellation) {
        for (RangeQuery q : ranges) {
            if (!q.columns().isEmpty()) {
                throw new IllegalArgumentException("a planned range takes no columns: the plan's columns apply to every range");
            }
        }
        List<String> projected = List.copyOf(columns);
        checkProjection(projected);
        try (Request request = begin(cancellation)) {
            return RangePlan.plan(this, parts, List.copyOf(ranges), projected, request.ctx);
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
        return list(query, cancellation, stats -> {});
    }

    /**
     * Lists keys with delimiter rollups (spec §6.3), ending early when {@code cancellation} is cancelled,
     * and hands the request's read statistics to {@code stats} when it ends, whether it succeeds or
     * fails; a request that fails before it reads delivers zero counts.
     *
     * @param query the query
     * @param cancellation the request's cancellation
     * @param stats receives the request's read statistics
     * @return the entries, in ascending byte order, at most {@code query.limit()}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<ListEntry> list(ListQuery query, Cancellation cancellation, Consumer<? super ReadStats> stats) {
        Request started;
        try {
            checkResultLimit(query.limit());
            checkProjection(query.columns());
            started = begin(cancellation);
        } catch (RuntimeException e) {
            throw notStarted(stats, e);
        }
        try (Request request = started) {
            try {
                return Queries.list(parts, query, request.ctx);
            } finally {
                stats.accept(request.ctx.stats.snapshot());
            }
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
    final class Request implements AutoCloseable {
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

    /** {@return the dataset's parts} */
    Parts parts() {
        return parts;
    }

    /**
     * Begins a request that reports its read statistics to {@code stats}: a request that fails to start
     * (cancelled already, or on a closed dataset) reports zero counts, and throws as {@link #begin} does.
     */
    Request begin(Cancellation caller, Consumer<? super ReadStats> stats) {
        try {
            return begin(caller);
        } catch (RuntimeException e) {
            throw notStarted(stats, e);
        }
    }

    /** Reports zero counts for a request that failed to start with {@code e}, and {@return e}. */
    private static RuntimeException notStarted(Consumer<? super ReadStats> stats, RuntimeException e) {
        try {
            stats.accept(ReadStats.NONE);
        } catch (RuntimeException suppressed) {
            e.addSuppressed(suppressed);
        }
        return e;
    }

    Request begin(Cancellation caller) {
        caller.throwIfCancelled();
        if (closing.isCancelled()) {
            throw new IllegalStateException("the dataset is closed");
        }
        // A close waiting for the requests in progress does not hold back this one: tryLock takes the read
        // lock past a waiting writer. A request started inside another (from a scan's action, on any
        // thread) while the dataset closes therefore starts, and its cancellation, linked to the close,
        // ends it at once; waiting behind the close, it would wait for the request it runs inside.
        if (!lifecycle.readLock().tryLock()) {
            lifecycle.readLock().lock(); // the close holds the lock: it has drained, and is closing
        }
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
     * <p>A request cannot wait for itself: closing the dataset from inside one of its own requests on the
     * same thread (from a {@link RangePlan#scan scan}'s action or statistics consumer) would wait forever,
     * and fails instead, changing nothing. End the scan by throwing from the action, and close the dataset
     * once the scan has returned.
     *
     * @throws StorageException if closing a part source fails (every source is still closed)
     * @throws IllegalStateException if called from inside a request of this dataset on the same thread
     */
    @Override
    public void close() {
        if (lifecycle.getReadHoldCount() > 0) {
            throw new IllegalStateException("the dataset cannot be closed from inside one of its own requests: "
                    + "end the request first, then close it");
        }
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
