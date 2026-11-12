// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * An open SeekParquet dataset: a manifest and its parts, ready for range reads and delimited
 * listings. The Go reader's {@code Dataset}.
 *
 * <pre>{@code
 * try (Dataset dataset = Dataset.open(Path.of("/data/listing"))) {
 *     for (Row row : dataset.rows(Query.builder(Span.prefix(Bytes.utf8("photos/"))).limit(100).build())) {
 *         System.out.println(row.key().toStringUtf8());
 *     }
 * }
 * }</pre>
 *
 * <h2>Operations</h2>
 *
 * {@link #rows} collects a bounded page of a range; {@link #scan} streams a range as borrowed {@link
 * Batch}es, bounded or to the end of its span; {@link #get} reads one whole group of equal keys; {@link
 * #plan} plans several selections for counting, splitting and scanning ({@link Plan}); {@link #list} lists
 * keys with delimiter rollups. Each takes {@link ReadOptions}: a cancellation, and a consumer that receives
 * the operation's {@link ReadStats} once, on every exit. A malformed request fails with {@link
 * IllegalArgumentException} before anything is read.
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
 * IllegalStateException}. A scan's visitor must not close the dataset itself: see {@link #close()}.
 *
 * <p>Each part a request touches stays open, with its checked footer in memory, until the dataset
 * closes; no limit bounds how many. {@link Limits#parts()} bounds the manifest and the index memory
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
     * Opens the dataset in the local directory {@code root}, with default options. The Go reader's {@code
     * OpenDir}.
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
     * Opens the dataset in the local directory {@code root}; see {@link #open(DatasetStorage, OpenOptions,
     * Cancellation)}.
     *
     * @param root the dataset root
     * @param options the options
     * @param cancellation ends the opening early when cancelled
     * @return the open dataset
     * @throws RefusalException if the dataset breaks the specification
     * @throws UnsupportedFeatureException if it uses a feature this reader does not implement
     * @throws LimitExceededException if it declares a size above the configured limits
     * @throws StorageException if reading the directory fails
     * @throws CancellationException if cancelled or interrupted
     */
    public static Dataset open(Path root, OpenOptions options, Cancellation cancellation) {
        return open(LocalStorage.of(root), options, cancellation);
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
     * Opens a dataset, checking its manifest (CHECK-1) within the limits ({@link Manifest#parse(byte[],
     * Limits)}). No part is opened until a request touches it. The reader counts rows in signed 64-bit
     * dataset ordinals: a manifest whose row total exceeds 2^63-1 fails with {@link LimitExceededException}
     * ({@link LimitName#DATASET_ROWS}), as in the Go reader, even when each part's count fits. The Go
     * reader's {@code OpenDataset}.
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
            raw = storage.readManifest(limits.manifestBytes(), cancellation);
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
        Manifest manifest = Manifest.parse(raw, limits);
        if (manifest.rowsExact().compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw new LimitExceededException(LimitName.DATASET_ROWS, -1, Long.MAX_VALUE,
                    "manifest.json: the parts hold more rows than a signed 64-bit ordinal counts");
        }
        return new Dataset(manifest, new Parts(storage, manifest, limits), options);
    }

    /**
     * Checks every part now, with default options; see {@link #verify(ReadOptions)}.
     *
     * @throws RefusalException if a part breaks the specification: the first such part in manifest
     *     order among those checked
     * @throws UnsupportedFeatureException if a part uses a feature this reader does not implement
     * @throws LimitExceededException if a part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public void verify() {
        verify(ReadOptions.DEFAULT);
    }

    /**
     * Checks every part now, as the first request to touch each part would (CHECK-1), then decodes
     * every row group's first key page and each part's last, and compares each part's first and last
     * key with the manifest's bounds (CHECK-3). Parts already checked by a request are not checked
     * again; {@link OpenOptions#verifyConcurrency()} parts are checked at once (one by default, as the
     * Go reader does). It reads one footer per unchecked part, plus the key page index and one or two key
     * pages per row group, never every row. It does not check digests (PART-5) and is not validation.
     *
     * @param options the request's cancellation and statistics
     * @throws RefusalException if a part breaks the specification: the first such part in manifest
     *     order among those checked
     * @throws UnsupportedFeatureException if a part uses a feature this reader does not implement
     * @throws LimitExceededException if a part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public void verify(ReadOptions options) {
        run(options, () -> {}, ctx -> {
            parts.checkAll(verifyConcurrency, ctx);
            for (int i = 0; i < parts.size(); i++) {
                verifyKeyPages(parts.get(i, ctx), ctx);
            }
            return null;
        });
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
     * @throws IllegalStateException if the dataset is closed
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
     * Its own statistics are its result, so it takes a cancellation rather than {@link ReadOptions}: unlike
     * the read operations, it delivers no {@link ReadStats}, and a failed or cancelled build reports nothing.
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

    /** {@return the dataset's manifest, as parsed and validated on open} */
    public Manifest manifest() {
        return manifest;
    }

    /**
     * {@return the number of rows the manifest records} It fits a {@code long}: opening refuses a dataset
     * whose total does not. The Go reader sums {@code PartInfo.Rows}.
     */
    public long rowCount() {
        return manifest.rows();
    }

    /**
     * Returns the top-level columns of part {@code part}, with default options; see {@link #columns(int,
     * ReadOptions)}.
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
        return columns(part, ReadOptions.DEFAULT);
    }

    /**
     * Returns the top-level columns of part {@code part}, an index into {@code manifest().parts()}, in
     * schema order, from the footer the dataset checked and holds for that part: if no read has checked
     * the part yet, this checks it as the first read to touch it would (CHECK-1), so the answer always
     * comes from a checked part, and every later read of the part reads that same file. COL-5 holds a
     * column's physical type, repetition and nesting equal in every checked part that has it; a part may
     * lack a column (COL-2), and logical annotations are not compared, so a caller that needs a column in
     * every part asks for each part. There is no dataset-wide schema operation: the parts' schemas are
     * their own.
     *
     * @param part an index into {@code manifest().parts()}
     * @param options the request's cancellation and statistics
     * @return the part's columns, in schema order
     * @throws IndexOutOfBoundsException if {@code part} is not an index of the manifest's parts
     * @throws RefusalException if the part breaks the specification
     * @throws UnsupportedFeatureException if the part uses a feature this reader does not implement
     * @throws LimitExceededException if the part declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Column> columns(int part, ReadOptions options) {
        return run(options, () -> Objects.checkIndex(part, parts.size()), ctx -> {
            PartFile p = parts.get(part, ctx);
            String key = manifest.key().column();
            return p.schema.fields.stream().map(f -> PartSchema.column(f, key)).toList();
        });
    }

    /**
     * Returns the rows of {@code query}, with default options; see {@link #rows(Query, ReadOptions)}.
     *
     * @param query the query, with a positive limit
     * @return the rows, in ascending key order, at most {@code query.limit()}
     * @throws IllegalArgumentException if the limit is not positive
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> rows(Query query) {
        return rows(query, ReadOptions.DEFAULT);
    }

    /**
     * Returns the rows of {@code query} (spec §6.2), owning their keys and values: at most {@code
     * query.limit()}, which must be positive (QUERY-1) and at most {@link Integer#MAX_VALUE}, a {@link List}'s
     * capacity (a representation limit, as Go checks its slice length; a {@link #scan} takes any limit). It decodes key pages only from the page that can
     * hold its start, or the cursor's row, and value pages only for the rows it returns. On an error it
     * returns no rows. The Go reader's {@code Rows}.
     *
     * @param query the query, with a positive limit
     * @param options the request's cancellation and statistics
     * @return the rows, in ascending key order and equal keys in file order, immutable
     * @throws IllegalArgumentException if the limit is not between 1 and {@link Integer#MAX_VALUE}
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> rows(Query query, ReadOptions options) {
        return run(options, () -> {
            Objects.requireNonNull(query, "query");
            if (query.limit() <= 0 || query.limit() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("rows limit must be between 1 and Integer.MAX_VALUE, not " + query.limit());
            }
            checkProjection(query.columns());
        }, ctx -> Queries.range(parts, query, ctx));
    }

    /**
     * Returns every row of {@code key}'s group of equal keys, with default options; see {@link #get(Bytes,
     * List, ReadOptions)}.
     *
     * @param key the key
     * @param columns the columns to project, in order
     * @return the group's rows, in file order
     * @throws LimitExceededException if the group has more than {@link Limits#getRows()} rows
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> get(Bytes key, List<String> columns) {
        return get(key, columns, ReadOptions.DEFAULT);
    }

    /**
     * Returns every row of {@code key}'s group of equal keys, owning their keys and the values of {@code
     * columns}: none when no row has the key, at most one when keys are unique, the whole group otherwise,
     * in file order. It finds the group's keys before it reads any value page, and a group of more than
     * {@link Limits#getRows()} rows fails, with no rows and before any value page is read, with {@link
     * LimitExceededException} naming {@link LimitName#GET_ROWS}: read such a group with {@link #rows} over
     * {@link Span#key}, page by page, continuing after each page's last row. The limit bounds rows, not
     * bytes. Errors are otherwise those of {@link #rows}. The Go reader's {@code Get}.
     *
     * @param key the key
     * @param columns the columns to project, by exact top-level name, in order
     * @param options the request's cancellation and statistics
     * @return the group's rows, in file order, immutable
     * @throws LimitExceededException if the group has more than {@link Limits#getRows()} rows, or a page
     *     declares a size above the configured limits
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<Row> get(Bytes key, List<String> columns, ReadOptions options) {
        return run(options, () -> {
            Objects.requireNonNull(key, "key");
            List<String> projected = List.copyOf(columns);
            checkProjection(projected);
            return projected;
        }, (ctx, projected) -> Queries.get(parts, key, projected, limits.getRows(), ctx));
    }

    /**
     * Visits the rows of {@code query} as borrowed batches, with default options; see {@link #scan(Query,
     * ReadOptions, BatchVisitor)}.
     *
     * @param <E> the checked exception the visitor may throw
     * @param query the query; limit 0 for every row of its span
     * @param visitor receives each batch, borrowed until it returns
     * @throws E when the visitor throws it
     * @throws IllegalStateException if the dataset is closed
     */
    public <E extends Exception> void scan(Query query, BatchVisitor<E> visitor) throws E {
        scan(query, ReadOptions.DEFAULT, visitor);
    }

    /**
     * Hands {@code visitor}, in serial calls on this thread, the rows of {@code query} as borrowed {@link
     * Batch}es: the rows {@link #rows} returns. Bounds, cursor and columns mean what they mean to {@code
     * rows}, and its errors are {@code rows}'. A positive limit stops it after that many rows without
     * moving to the next row; a limit of 0 reads to the end of the span, a streaming extension of this
     * library (the specification's range has a positive limit, QUERY-1). No result limit applies: a scan
     * holds one batch at a time. It plans nothing: it opens each projected column chunk when it reaches its
     * row group, reads a dictionary only for a page that uses it, and reads no OffsetIndex beyond the rows
     * it hands over. A span with no rows never calls the visitor. The Go reader's {@code Scan}.
     *
     * <p>{@link Batch} sets out the boundaries (never crossing a row group, a key page or a projected page;
     * the last batch ends at the limit or the upper bound) and the lifetime: a batch is read-only and
     * borrowed until the visit returns, and {@link Batch#row}, {@link Batch#cursor} make the owning rows
     * and cursors {@code rows} returns.
     *
     * <p>The visitor returning {@code false} stops the scan successfully. An exception it throws ends the
     * request and propagates unchanged; batches already handed over stand, as they do before a read error
     * or a refusal. Cancellation is checked as {@code rows} checks it, and also before every visit. The
     * request holds the dataset open while the visitor runs: {@link #close()} cancels it and waits for the
     * visitor to return. Closing the dataset from inside the visitor fails with {@link
     * IllegalStateException}, and the visitor must not wait for another thread that is closing it.
     * Concurrent and nested requests have storage of their own.
     *
     * @param <E> the checked exception the visitor may throw
     * @param query the query; limit 0 for every row of its span
     * @param options the request's cancellation and statistics
     * @param visitor receives each batch, borrowed until it returns
     * @throws E when the visitor throws it
     * @throws RefusalException if the data breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a declared size exceeds the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public <E extends Exception> void scan(Query query, ReadOptions options, BatchVisitor<E> visitor) throws E {
        run(options, () -> {
            Objects.requireNonNull(query, "query");
            Objects.requireNonNull(visitor, "visitor");
            checkProjection(query.columns());
        }, ctx -> {
            Queries.scan(parts, query, ctx, visitor);
            return null;
        });
    }

    /**
     * Plans reading the rows of {@code selections}, with default options; see {@link #plan(List, List,
     * ReadOptions)}.
     *
     * @param selections the selections
     * @param columns the columns to project, by exact top-level name, in order
     * @return the plan
     * @throws RefusalException if data planning decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or uses a codec this
     *     reader does not implement
     * @throws LimitExceededException if a page or index declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws IllegalStateException if the dataset is closed
     */
    public Plan plan(List<Selection> selections, List<String> columns) {
        return plan(selections, columns, ReadOptions.DEFAULT);
    }

    /**
     * Plans reading the rows of {@code selections}, with the values of {@code columns} (as a {@link Query}'s
     * columns). Each selection is read whole, from its start or after its cursor, inside a group of equal
     * keys too; rows that several selections select are one row of the plan. The Go reader's {@code Plan}.
     *
     * <p>Planning decodes what {@link #rows} starting at each end would: the key pages that locate the
     * first row of the selection and the first row after it, through the routing and the key page index,
     * and no part known from the manifest bounds to begin after the selection. With columns, it also checks
     * every part holding a selected row and reads the OffsetIndex of each projected column chunk the rows
     * span, unless an earlier request kept its page locations ({@link Limits#offsetIndexMemory()}), but no
     * data page. Errors are those of {@code rows}. The statistics of planning go to {@code options}, on
     * success and failure alike.
     *
     * @param selections the selections
     * @param columns the columns to project, by exact top-level name, in order
     * @param options the request's cancellation and statistics
     * @return the plan, bound to this dataset
     * @throws RefusalException if data planning decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or uses a codec this
     *     reader does not implement
     * @throws LimitExceededException if a page or index declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public Plan plan(List<Selection> selections, List<String> columns, ReadOptions options) {
        record Planned(List<Selection> selections, List<String> columns) {}
        return run(options, () -> {
            Planned p = new Planned(List.copyOf(selections), List.copyOf(columns));
            checkProjection(p.columns());
            return p;
        }, (ctx, p) -> Plan.plan(this, parts, p.selections(), p.columns(), ctx));
    }

    /**
     * Lists keys with delimiter rollups, with default options; see {@link #list(ListQuery, ReadOptions)}.
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
        return list(query, ReadOptions.DEFAULT);
    }

    /**
     * Lists keys with delimiter rollups (spec §6.3). After a common prefix it continues at the prefix's
     * successor through the routing index and the page index instead of decoding the keys under it. Value
     * pages are read only for row entries and exact rows. {@link #buildListIndex} optionally replaces
     * slash-prefix resumes with exact directory-run anchors. Errors are those of {@link #rows}. The Go
     * reader's {@code List}.
     *
     * @param query the query
     * @param options the request's cancellation and statistics
     * @return the entries, in ascending byte order, at most {@code query.limit()}, immutable
     * @throws RefusalException if data the request decodes, or a part it touches, breaks the specification
     * @throws UnsupportedFeatureException if a projected column cannot be projected, or a page uses a
     *     codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<ListEntry> list(ListQuery query, ReadOptions options) {
        return run(options, () -> checkProjection(Objects.requireNonNull(query, "query").columns()), ctx -> Queries.list(parts, query, ctx));
    }

    /**
     * COL-3: a nested or repeated column of a checked part is rejected, never answered
     * as absent. A part checked later is held to the same rule when a row of it is projected.
     */
    private void checkProjection(List<String> names) {
        for (int i = 0; i < parts.size(); i++) {
            PartFile part = parts.checked(i);
            if (part != null) {
                part.checkProjection(names);
            }
        }
    }

    /** The work of an operation inside its request. */
    @FunctionalInterface
    interface Body<T, E extends Exception> {
        T apply(ReadContext ctx) throws E;
    }

    /** The work of an operation inside its request, with the arguments its preparation checked and copied. */
    @FunctionalInterface
    interface Prepared<A, T, E extends Exception> {
        T apply(ReadContext ctx, A args) throws E;
    }

    /** {@link #run(ReadOptions, java.util.function.Supplier, Prepared)} with nothing prepared but checks. */
    <T, E extends Exception> T run(ReadOptions options, Runnable checks, Body<T, E> body) throws E {
        return run(options, () -> {
            checks.run();
            return null;
        }, (ctx, none) -> body.apply(ctx));
    }

    /**
     * Runs one operation: {@code prepare} (the request's own validity, and defensive copies of its
     * arguments), then {@code body} inside a request, and delivers the operation's statistics to {@code
     * options} exactly once on every exit, an invalid argument included, after the request is released. A
     * statistics consumer that throws while the operation fails is added to the operation's exception as
     * suppressed; while it succeeds, its exception propagates.
     */
    <A, T, E extends Exception> T run(ReadOptions options, java.util.function.Supplier<? extends A> prepare, Prepared<A, T, E> body) throws E {
        Objects.requireNonNull(options, "options");
        ReadStats.Counter counter = null;
        Throwable failure = null;
        try {
            A args = prepare.get();
            try (Request request = begin(options.cancellation())) {
                counter = request.ctx.stats;
                return body.apply(request.ctx, args);
            }
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            report(options.statistics(), counter == null ? ReadStats.NONE : counter.snapshot(), failure);
        }
    }

    private static void report(Consumer<? super ReadStats> statistics, ReadStats stats, Throwable failure) {
        try {
            statistics.accept(stats);
        } catch (RuntimeException | Error e) {
            if (failure == null) {
                throw e;
            }
            if (e != failure) {
                failure.addSuppressed(e);
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
     * same thread (from a {@link Plan#scan scan}'s visitor, or from the statistics consumer of an operation
     * started inside one) would wait forever, and fails instead, changing nothing. End the scan by returning
     * {@code false} from the visitor, and close the dataset once the scan has returned. A statistics consumer
     * of a top-level operation runs after the operation released the dataset, and may close it.
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
