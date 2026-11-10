// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.parquet.column.Dictionary;

/**
 * The state of one request (or of opening a dataset): its cancellation, the limits, and what it has
 * decoded so far. Confined to the thread running the request.
 */
final class ReadContext {

    final Cancellation cancellation;
    final Limits limits;
    /** What the request has read so far (shared by the contexts of one request). */
    final ReadStats.Counter stats;
    /**
     * Whether value pages are read for a {@link RangePlan} scan: a chunk's dictionary is read before its
     * first page, in the one read {@link RangePlan#projectedBytes()} counted.
     */
    final boolean planned;
    private final Map<ChunkKey, Dictionary> dictionaries = new HashMap<>();
    private final Map<ChunkKey, DecodedPage> valuePages = new HashMap<>();
    private final Map<ChunkKey, long[]> sequential = new HashMap<>();

    private int activePart = -1;
    private int activeGroup = -1;

    /** Only the current row group's value pages and dictionaries are retained. */
    void enterGroup(int part, int group) {
        if (part != activePart || group != activeGroup) {
            dictionaries.clear();
            valuePages.clear();
            sequential.clear();
            activePart = part;
            activeGroup = group;
        }
    }

    /** Boundaries of all pages decoded by this request, including out-of-order seek probes. */
    private final TreeMap<PagePosition, PageBounds> keyPages = new TreeMap<>();

    private record PagePosition(int part, int group, int page) implements Comparable<PagePosition> {
        @Override
        public int compareTo(PagePosition other) {
            int c = Integer.compare(part, other.part);
            if (c == 0) c = Integer.compare(group, other.group);
            return c == 0 ? Integer.compare(page, other.page) : c;
        }
    }

    private record PageBounds(byte[] first, byte[] last) {}

    void checkKeyPage(PartFile part, int group, int page, byte[][] keys) {
        PagePosition position = new PagePosition(part.ordinal, group, page);
        var before = keyPages.lowerEntry(position);
        var after = keyPages.higherEntry(position);
        byte[] first = keys[0];
        byte[] last = keys[keys.length - 1];
        if ((before != null && outOfOrder(before.getValue().last(), first, part.unique))
                || (after != null && outOfOrder(last, after.getValue().first(), part.unique))) {
            throw new RefusalException(RefusalCode.KEY_ORDER, part.name,
                    "row group " + group + " key page " + page + " is out of order with another decoded page");
        }
        keyPages.put(position, new PageBounds(first, last));
    }

    // A sequential builder needs only its preceding page, not a log of the group.
    void retainLastKeyPage() {
        while (keyPages.size() > 1) keyPages.pollFirstEntry();
    }

    private static boolean outOfOrder(byte[] a, byte[] b, boolean unique) {
        int c = Arrays.compareUnsigned(a, b);
        return c > 0 || (unique && c == 0);
    }

    ReadContext(Cancellation cancellation, Limits limits) {
        this(cancellation, limits, new ReadStats.Counter(), false);
    }

    private ReadContext(Cancellation cancellation, Limits limits, ReadStats.Counter stats, boolean planned) {
        this.cancellation = cancellation;
        this.limits = limits;
        this.stats = stats;
        this.planned = planned;
    }

    /**
     * {@return a context of the same request for a plan's value columns} Its row-group caches are its
     * own, so that the key cursor's routing probes do not drop a chunk's dictionary or current page.
     */
    ReadContext forPlannedValues() {
        return new ReadContext(cancellation, limits, stats, true);
    }

    void checkCancelled() {
        cancellation.throwIfCancelled();
    }

    /** A column chunk of one part. */
    record ChunkKey(int part, int group, int leaf) {}

    /** A decoded value page and the row of its group where it starts. */
    record DecodedPage(long firstRow, ColumnPage page) {}

    Dictionary dictionary(ChunkKey key) {
        return dictionaries.get(key);
    }

    void putDictionary(ChunkKey key, Dictionary d) {
        dictionaries.put(key, d);
    }

    DecodedPage valuePage(ChunkKey key) {
        return valuePages.get(key);
    }

    void putValuePage(ChunkKey key, DecodedPage page) {
        valuePages.put(key, page);
    }

    /** The resume point {offset, firstRow} of a sequential walk through a chunk without an OffsetIndex. */
    long[] sequential(ChunkKey key) {
        return sequential.get(key);
    }

    void putSequential(ChunkKey key, long[] state) {
        sequential.put(key, state);
    }
}
