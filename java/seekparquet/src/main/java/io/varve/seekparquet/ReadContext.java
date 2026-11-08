// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.column.Dictionary;

/**
 * The state of one request (or of opening a dataset): its cancellation, the limits, and what it has
 * decoded so far. Confined to the thread running the request.
 */
final class ReadContext {

    final Cancellation cancellation;
    final Limits limits;
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

    /**
     * The last key of the last key page decoded, in dataset order. A request decodes key pages in
     * ascending dataset position only, so checking each new page's first key against it checks the
     * page against every page decoded before it (CHECK-2).
     */
    byte[] lastKey;

    /** Where {@link #lastKey} came from, for diagnostics. */
    String lastKeyWhere;

    ReadContext(Cancellation cancellation, Limits limits) {
        this.cancellation = cancellation;
        this.limits = limits;
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
