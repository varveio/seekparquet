// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Immutable sparse directory runs. A negative suffix length marks a dense page. */
final class ListBoundaryIndex {
    final Group[] groups;
    final ListIndexStats stats;

    ListBoundaryIndex(Group[] groups, ListIndexStats stats) {
        this.groups = groups;
        this.stats = stats;
    }

    Group group(int part, int slot) {
        int lo = 0, hi = groups.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            Group g = groups[mid];
            if (g.part < part || (g.part == part && g.slot < slot)) lo = mid + 1;
            else hi = mid;
        }
        return lo < groups.length && groups[lo].part == part && groups[lo].slot == slot ? groups[lo] : null;
    }

    static final class Group {
        final int part, slot;
        final boolean dense;
        final byte[] base, data;
        final long[] rows;
        final int[] offsets, sizes, shared, denseThrough;

        Group(int part, int slot, byte[] base, byte[] data, List<Boundary> runs) {
            this.part = part; this.slot = slot; this.base = base; this.data = data;
            rows = new long[runs.size()]; offsets = new int[runs.size()];
            sizes = new int[runs.size()]; shared = new int[runs.size()]; denseThrough = new int[runs.size()];
            boolean hasDense = false;
            for (int i = 0; i < runs.size(); i++) {
                Boundary b = runs.get(i);
                rows[i] = b.row(); offsets[i] = b.offset(); sizes[i] = b.size(); shared[i] = b.shared();
                hasDense |= b.size() < 0;
                denseThrough[i] = (i == 0 ? 0 : denseThrough[i - 1]) + (b.size() < 0 ? 1 : 0);
            }
            dense = hasDense;
        }

        long memoryBytes() { return charge(base.length, data.length, rows.length); }

        int runAt(long row) {
            int lo = 0, hi = rows.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (rows[mid] <= row) lo = mid + 1; else hi = mid;
            }
            return lo - 1;
        }

        boolean hasPrefix(int i, byte[] prefix) {
            if (sizes[i] < 0 || shared[i] + sizes[i] < prefix.length) return false;
            for (int j = 0; j < prefix.length; j++) {
                byte b = j < shared[i] ? base[j] : data[offsets[i] + j - shared[i]];
                if (b != prefix[j]) return false;
            }
            return true;
        }

        // Binary-search the successor and reject any dense marker in the interval.
        // The callback also permits deterministic cancellation/step-bound tests.
        int firstOutside(int start, byte[] prefix, Runnable checkCancelled) {
            byte[] target = Bytes.wrap(prefix).prefixEnd().map(Bytes::unsafeArray).orElse(null);
            int lo = start, hi = rows.length;
            while (lo < hi) {
                checkCancelled.run();
                int mid = (lo + hi) >>> 1;
                if (target == null || compare(mid, target) < 0) lo = mid + 1;
                else hi = mid;
            }
            checkCancelled.run();
            int end = Math.min(lo + 1, rows.length);
            int before = start == 0 ? 0 : denseThrough[start - 1];
            return end > start && denseThrough[end - 1] != before ? -1 : lo;
        }

        int compare(int i, byte[] key) {
            int n = shared[i] + (sizes[i] < 0 ? ~sizes[i] : sizes[i]);
            for (int j = 0; j < Math.min(n, key.length); j++) {
                byte b = j < shared[i] ? base[j] : data[offsets[i] + j - shared[i]];
                int c = Integer.compare(Byte.toUnsignedInt(b), Byte.toUnsignedInt(key[j]));
                if (c != 0) return c;
            }
            return Integer.compare(n, key.length);
        }

        byte[] key(int i) {
            byte[] key = Arrays.copyOf(base, shared[i] + sizes[i]);
            System.arraycopy(data, offsets[i], key, shared[i], sizes[i]);
            return key;
        }
    }

    record Boundary(long row, int offset, int size, int shared) {}
    private record Built(Group group, long pages, RuntimeException failure, byte[] first, byte[] last) {}

    private static int directoryLength(byte[] key) {
        for (int i = key.length - 1; i >= 0; i--) if (key[i] == '/') return i + 1;
        return 0;
    }

    private static boolean sameDirectory(byte[] a, byte[] b) {
        if (a == null) return false;
        int n = directoryLength(a);
        return n == directoryLength(b) && Arrays.equals(a, 0, n, b, 0, n);
    }

    static long allocation(long n) { return n <= 1 ? n : Long.highestOneBit(n - 1) << 1; }
    static long charge(int base, int data, int runs) { return 256 + allocation(base) + allocation(data) + allocation(40L * runs); }

    private static void add(List<Boundary> runs, ByteArrayOutputStream data, byte[] base, long row, byte[] key) {
        int shared = 0;
        if (key != null) {
            while (shared < base.length && shared < key.length && base[shared] == key[shared]) shared++;
        }
        runs.add(new Boundary(row, data.size(), key == null ? -1 : key.length - shared, shared));
        if (key != null) data.write(key, shared, key.length - shared);
    }

    private static Built buildGroup(PartFile part, int slot, ListIndexOptions options, ReadContext ctx) {
        long pages = 0;
        byte[] previous = null, base = null;
        try {
            int physical = part.groups[slot];
            KeyIndex index = part.keyIndex(physical, ctx, false);
            List<Boundary> runs = new ArrayList<>();
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            boolean denseBefore = false;
            for (int pg = 0; pg < index.pages(); pg++) {
                ctx.checkCancelled();
                byte[][] keys = part.keyPage(physical, index, pg, ctx);
                ctx.checkKeyPage(part, physical, pg, index.firstRow[pg], keys);
                ctx.retainLastKeyPage();
                pages++;
                if (pg == 0) { base = keys[0].clone(); }
                int changes = 0;
                byte[] prev = previous;
                for (byte[] key : keys) {
                    if (prev != null && !sameDirectory(prev, key)) changes++;
                    prev = key;
                }
                if (changes > options.maxChangesPerPage()) {
                    add(runs, data, base, index.firstRow[pg], keys[0]);
                    Boundary b = runs.getLast();
                    runs.set(runs.size() - 1, new Boundary(b.row(), b.offset(), ~b.size(), b.shared()));
                    denseBefore = true;
                } else {
                    for (int j = 0; j < keys.length; j++) {
                        if ((pg == 0 && j == 0) || (denseBefore && j == 0) || !sameDirectory(previous, keys[j])) {
                            add(runs, data, base, index.firstRow[pg] + j, keys[j]);
                        }
                        previous = keys[j];
                    }
                    denseBefore = false;
                }
                previous = keys[keys.length - 1];
                // Finish proving flatness before dropping the temporary anchor;
                // a later page may contain a directory and require admission.
                boolean flatSoFar = runs.size() == 1 && runs.getFirst().size() >= 0 && directoryLength(base) == 0;
                if (!flatSoFar && charge(base.length, data.size(), runs.size()) > options.maxBytes()) break;
            }
            if (runs.size() == 1 && runs.getFirst().size() >= 0 && directoryLength(base) == 0) {
                return new Built(null, pages, null, base, previous);
            }
            return new Built(new Group(part.ordinal, slot, base, data.toByteArray(), runs), pages, null, base, previous);
        } catch (CancellationException e) {
            throw e;
        } catch (RefusalException | UnsupportedFeatureException e) {
            return new Built(null, ctx.stats.keyPagesDecoded, e, base != null ? base : part.knownFirst(part.groups[slot]), previous);
        }
    }

    static ListBoundaryIndex build(Parts parts, ListIndexOptions options, ReadContext ctx) {
        long started = System.nanoTime();
        long skipped = 0, failed = 0, partsFailed = 0, pages = 0, entries = 0, bytes = 0;
        boolean exhausted = false;
        List<Group> groups = new ArrayList<>();
        byte[] previousLast = null;
        int previousPart = -1, previousSlot = -1;
        boolean previousFailed = false, previousAdmitted = false;
        try (var workers = Executors.newFixedThreadPool(options.workers())) {
            outer: for (int pi = 0; pi < parts.size(); pi++) {
                ctx.checkCancelled();
                PartFile part;
                try { part = parts.get(pi, ctx); }
                catch (RefusalException | UnsupportedFeatureException e) { ctx.checkCancelled(); partsFailed++; continue; }
                for (int start = 0; start < part.groups.length;) {
                    ctx.checkCancelled();
                    int n = Math.min(options.workers(), part.groups.length - start);
                    List<Future<Built>> futures = new ArrayList<>(n);
                    for (int j = 0; j < n; j++) {
                        ctx.checkCancelled();
                        int slot = start + j;
                        futures.add(workers.submit(() -> buildGroup(part, slot, options,
                                new ReadContext(ctx.cancellation, ctx.limits))));
                    }
                    List<Built> results = new ArrayList<>(n);
                    for (Future<Built> future : futures) {
                        Built built;
                        try { built = future.get(); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new CancellationException("interrupted"); }
                        catch (ExecutionException e) {
                            if (e.getCause() instanceof RuntimeException r) throw r;
                            if (e.getCause() instanceof Error error) throw error;
                            throw new IllegalStateException(e.getCause());
                        }
                        ctx.checkCancelled();
                        pages += built.pages();
                        results.add(built);
                    }
                    for (int bi = 0; bi < results.size(); bi++) {
                        Built built = results.get(bi);
                        Group group = built.group();
                        int slot = start + bi;
                        boolean badBoundary = previousLast != null && built.first() != null
                                && previousPart == part.ordinal && previousSlot + 1 == slot
                                && (Arrays.compareUnsigned(previousLast, built.first()) > 0
                                    || (part.unique && Arrays.equals(previousLast, built.first())));
                        if (badBoundary && !previousFailed) {
                            failed++;
                            if (previousAdmitted) {
                                Group removed = groups.removeLast();
                                bytes -= removed.memoryBytes(); entries -= removed.rows.length; skipped++;
                                if (groups.isEmpty()) bytes -= 256;
                            }
                        }
                        // Rejected groups still contribute their decoded boundary:
                        // chained violations exclude every participant exactly once.
                        previousLast = built.last() != null ? built.last() : built.first();
                        previousPart = part.ordinal; previousSlot = slot;
                        previousFailed = built.failure() != null || badBoundary;
                        previousAdmitted = false;
                        if (previousFailed) { skipped++; failed++; continue; }
                        if (group == null) { skipped++; continue; }
                        long cost = group.memoryBytes() + (groups.isEmpty() ? 256 : 0);
                        if (cost > options.maxBytes() - bytes) {
                            exhausted = true; skipped += results.size() - bi;
                            for (int j = bi + 1; j < results.size(); j++) if (results.get(j).failure() != null) failed++;
                            break outer;
                        }
                        groups.add(group); bytes += cost; entries += group.rows.length;
                        previousAdmitted = true;
                    }
                    start += n;
                }
            }
        }
        ctx.checkCancelled();
        return new ListBoundaryIndex(groups.toArray(Group[]::new), new ListIndexStats(groups.size(), skipped,
                failed, partsFailed, pages, entries, bytes, exhausted, Duration.ofNanos(System.nanoTime() - started), options));
    }
}
