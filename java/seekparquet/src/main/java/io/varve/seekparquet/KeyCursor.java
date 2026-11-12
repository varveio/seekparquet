// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;

/**
 * Walks the dataset's keys forward, in dataset order, for one request.
 *
 * <p>Parts are routed by exact manifest bounds. Large parts are routed by checked exact
 * row-group first keys, keeping the predecessor so repeated groups are never skipped. Shortened
 * page bounds are examined in order, never binary-searched. The request checks all decoded pages
 * against their neighbors in dataset order, including the out-of-order routing probes.
 *
 * <p>A part is checked the first time the cursor enters it (CHECK-1). A part whose manifest minimum
 * key is at or above the request's upper bound is never entered: the cursor ends there instead.
 */
final class KeyCursor {

    private final Parts parts;
    private final ListBoundaryIndex listIndex;
    private byte[] syntheticKey;
    private long syntheticRow;
    /** The request's exclusive upper bound, or null. */
    private final byte[] end;
    private final ReadContext ctx;
    private final Runnable checkCancelled;

    private int part = -1;
    /** The current part, once entered. */
    private PartFile pf;
    /** Index into {@code pf.groups}. */
    private int slot;
    private int physical;
    private KeyIndex index;
    private int page;
    private byte[][] keys;
    private int pos;
    private long[] offsets;
    private int decodedPart = -1;
    private int decodedSlot = -1;
    private boolean decodedLastPage;
    private int decodedPage = -1;
    private long tailOffset;
    private byte[] tailKey;

    private boolean exhausted;
    /** Once exhausted, the dataset ordinal of the first row the cursor did not reach. */
    private long bound;

    long offset() {
        return syntheticKey != null ? 0 : offsets[pos];
    }

    KeyCursor(Parts parts, byte[] end, ReadContext ctx) {
        this.parts = parts;
        this.listIndex = parts.listIndex;
        this.end = end;
        this.ctx = ctx;
        this.checkCancelled = ctx::checkCancelled;
    }

    /** The current key. Valid after a seek or next that returned true. */
    byte[] key() {
        return syntheticKey != null ? syntheticKey : keys[pos];
    }

    /**
     * {@return the keys of the current key page} The current key is at {@link #pagePosition()}. Valid after
     * a seek or next that returned true, from a cursor that is not on a list-index anchor (only listings
     * land on those). Key pages are decoded into arrays of their own, never reused.
     */
    byte[][] pageKeys() {
        if (syntheticKey != null) {
            throw new IllegalStateException("the cursor is on a list-index anchor");
        }
        return keys;
    }

    /** {@return the group offsets of the current key page's rows}, as {@link #pageKeys()} */
    long[] pageOffsets() {
        return offsets;
    }

    /** {@return the index of the current key in {@link #pageKeys()}} */
    int pagePosition() {
        return pos;
    }

    /** The part of the current key. */
    PartFile partFile() {
        return pf;
    }

    /** The physical row group of the current key. */
    int physical() {
        return physical;
    }

    /** The row of the current key within its row group. */
    long row() {
        return syntheticKey != null ? syntheticRow : index.firstRow[page] + pos;
    }

    /** {@return the dataset ordinal of the current key: its row counted from the dataset's first} */
    long ordinal() {
        return parts.start(part) + pf.groupStart[physical] + row();
    }

    /**
     * {@return once a seek or next returned false, the dataset ordinal of the first row the cursor did
     * not reach}: the first row of the first part known from its manifest bounds to begin at or after the
     * upper bound, or the dataset's row count.
     */
    long bound() {
        return bound;
    }

    private boolean exhaust(int p) {
        exhausted = true;
        bound = parts.start(p);
        return false;
    }

    private static boolean satisfies(byte[] key, byte[] target, boolean exclusive) {
        int c = Arrays.compareUnsigned(key, target);
        return exclusive ? c > 0 : c >= 0;
    }

    /**
     * Moves to the first key at or after {@code target} (strictly after it when {@code exclusive}),
     * from the current position on; a null target means the first key. Returns false when there is
     * none.
     */
    boolean seek(byte[] target, boolean exclusive) {
        materialize();
        if (exhausted) {
            return false;
        }
        if (target == null) {
            if (part < 0) {
                return enter(0, 0, 0);
            }
            return true;
        }
        if (keys != null) {
            int i = firstSatisfying(keys, pos, target, exclusive);
            if (i < keys.length) {
                pos = i;
                return true;
            }
        }
        // The first part that can hold such a key: its exact last key reaches the target.
        int p = part < 0 ? 0 : part;
        if (part < 0 || !satisfies(parts.entry(part).maxKey().unsafeArray(), target, exclusive)) {
            int lo = part < 0 ? 0 : part + 1;
            int hi = parts.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (satisfies(parts.entry(mid).maxKey().unsafeArray(), target, exclusive)) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            p = lo;
        }
        if (p >= parts.size() || (p != part && beyondEnd(p))) {
            return exhaust(p);
        }
        int firstSlot = 0;
        if (p == part && keys != null) {
            // Later pages of the current row group first.
            int pg = index.pageFor(page + 1, target, exclusive);
            if (pg < index.pages()) {
                return decodeForward(p, slot, pg, target, exclusive);
            }
            firstSlot = slot + 1;
        } else if (Arrays.compareUnsigned(parts.entry(p).minKey().unsafeArray(), target) >= 0) {
            // The whole part is at or after the target: start at its first key.
            return decodeForward(p, 0, 0, target, exclusive);
        }
        PartFile candidate = parts.get(p, ctx);
        // Exact first-key probes are monotone in a valid sorted file. Keep the predecessor:
        // the target can start at its tail even when the following group starts with it.
        int lo = firstSlot;
        if (candidate.groups.length - firstSlot > 8) {
            int hi = candidate.groups.length;
            while (lo < hi) {
                ctx.checkCancelled();
                int mid = (lo + hi) >>> 1;
                if (satisfies(firstKey(candidate, mid), target, exclusive)) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            lo = Math.max(firstSlot, lo - 1);
        }
        // Bounds may overlap and be non-monotone; visit the earliest remaining candidate.
        KeyIndex probe = null;
        while (lo < candidate.groups.length
                && !satisfies((probe = candidate.keyIndex(candidate.groups[lo], ctx)).upper(), target, exclusive)) {
            lo++;
        }
        if (lo >= candidate.groups.length) {
            // No row group of the part reaches it: continue at the next part's first key.
            if (p + 1 >= parts.size()) {
                return exhaust(p + 1);
            }
            return decodeForward(p + 1, 0, 0, target, exclusive);
        }
        int pg = probe.pageFor(0, target, exclusive);
        return decodeForward(p, lo, Math.min(pg, probe.pages() - 1), target, exclusive);
    }

    boolean seekPrefixCursor(byte[] prefix, byte[] target) {
        if (listIndex == null || listIndex.groups.length == 0 || !Arrays.equals(Bytes.wrap(prefix).successor().orElseThrow().unsafeArray(), target)) {
            return seek(target, false);
        }
        if (prefix.length == 0 || prefix[prefix.length - 1] != '/') {
            ctx.stats.listIndexUnsafeRun++; return seek(target, false);
        }
        int lo = 0, hi = parts.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Arrays.compareUnsigned(parts.entry(mid).maxKey().unsafeArray(), target) < 0) lo = mid + 1;
            else hi = mid;
        }
        if (lo == parts.size() || beyondEnd(lo)) return exhaust(lo);
        int p = lo;
        PartFile candidate = parts.checked(p);
        if (candidate == null) { ctx.stats.listIndexMissingGroup++; return seek(target, false); }
        lo = 0; hi = candidate.groups.length;
        while (lo < hi) {
            ctx.checkCancelled();
            int mid = (lo + hi) >>> 1;
            byte[] firstKey = firstKey(candidate, mid);
            if (firstKey == null) { ctx.stats.listIndexMissingGroup++; return seek(target, false); }
            if (Arrays.compareUnsigned(firstKey, target) < 0) lo = mid + 1;
            else hi = mid;
        }
        int s = Math.max(0, lo - 1);
        var group = listIndex.group(p, s);
        if (group == null) { ctx.stats.listIndexMissingGroup++; return seek(target, false); }
        if (group.dense) { ctx.stats.listIndexDensePage++; return seek(target, false); }
        lo = 0; hi = group.rows.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (group.compare(mid, target) < 0) lo = mid + 1; else hi = mid;
        }
        int i = lo - 1;
        if (i < 0 || !group.hasPrefix(i, prefix)) { ctx.stats.listIndexUnsafeRun++; return seek(target, false); }
        moveGroup(p, candidate, s, null);
        syntheticKey = group.key(i); syntheticRow = group.rows[i];
        return seekAfterPrefix(prefix, target, (byte) '/');
    }

    private void materialize() {
        if (syntheticKey != null) {
            long ordinal = ordinal();
            syntheticKey = null;
            keys = null;
            seekOrdinal(ordinal);
        }
    }

    boolean seekAfterPrefix(byte[] prefix, byte[] target, byte delimiter) {
        int answer = skipPrefix(prefix, delimiter);
        return answer >= 0 ? answer != 0 : seek(target, false);
    }

    private byte[] firstKey(PartFile candidate, int s) {
        var group = listIndex == null ? null : listIndex.group(candidate.ordinal, s);
        if (group != null) return group.base;
        return candidate.firstKey(candidate.groups[s], ctx);
    }

    // -1 declines, 0 exhausts, 1 lands on an exact run-start anchor.
    private int skipPrefix(byte[] prefix, byte delimiter) {
        if (listIndex == null || listIndex.groups.length == 0) return -1;
        if (delimiter != '/') { ctx.stats.listIndexDelimiter++; return -1; }
        ctx.checkCancelled();
        var current = listIndex.group(part, slot);
        if (current == null) { ctx.stats.listIndexMissingGroup++; return -1; }
        int start = current.runAt(row());
        if (start < 0 || current.sizes[start] < 0) { ctx.stats.listIndexDensePage++; return -1; }
        if (!current.hasPrefix(start, prefix)) { ctx.stats.listIndexUnsafeRun++; return -1; }
        byte[] target = Bytes.wrap(prefix).successor().map(Bytes::unsafeArray).orElse(null);
        int lo = part, hi = parts.size();
        while (lo < hi) {
            ctx.checkCancelled();
            int mid = (lo + hi) >>> 1;
            if (target == null || Arrays.compareUnsigned(parts.entry(mid).maxKey().unsafeArray(), target) < 0) lo = mid + 1;
            else hi = mid;
        }
        int p = lo;
        if (p == parts.size() || beyondEnd(p)) return exhaustIndexed(p);
        PartFile candidate = parts.get(p, ctx);
        lo = 0; hi = candidate.groups.length;
        while (lo < hi) {
            ctx.checkCancelled();
            int mid = (lo + hi) >>> 1;
            if (Arrays.compareUnsigned(firstKey(candidate, mid), target) < 0) lo = mid + 1;
            else hi = mid;
        }
        int s = Math.max(0, lo - 1);
        if (p == part && s < slot) s = slot;
        var group = listIndex.group(p, s);
        if (group == null) { ctx.stats.listIndexMissingGroup++; return -1; }
        if (p != part || s != slot) start = 0;
        int i = group.firstOutside(start, prefix, checkCancelled);
        if (i < 0) { ctx.stats.listIndexDensePage++; return -1; }
        if (i > start && !group.hasPrefix(i - 1, prefix)) { ctx.stats.listIndexUnsafeRun++; return -1; }
        if (i == group.rows.length) {
            if (++s == candidate.groups.length) { p++; s = 0; }
            if (p == parts.size() || beyondEnd(p)) return exhaustIndexed(p);
            group = listIndex.group(p, s); i = 0;
            if (group == null) { ctx.stats.listIndexMissingGroup++; return -1; }
            if (group.sizes[0] < 0) { ctx.stats.listIndexDensePage++; return -1; }
            candidate = parts.get(p, ctx);
        }
        ctx.checkCancelled();
        byte[] key = group.key(i);
        if (Arrays.compareUnsigned(key, target) < 0) { ctx.stats.listIndexUnsafeRun++; return -1; }
        // A synthetic position needs no page index until materialization.
        moveGroup(p, candidate, s, null);
        syntheticKey = key; syntheticRow = group.rows[i]; ctx.stats.listIndexSkips++;
        if (end != null && Arrays.compareUnsigned(key, end) >= 0) {
            bound = ordinal(); exhausted = true; syntheticKey = null; return 0;
        }
        return 1;
    }

    private int exhaustIndexed(int p) {
        ctx.checkCancelled();
        syntheticKey = null; exhaust(p); ctx.stats.listIndexSkips++;
        return 0;
    }

    /**
     * Moves to the row at dataset ordinal {@code ordinal}, below the dataset's row count, through the
     * OffsetIndex row positions: it decodes the one key page holding the row, unless it is the page
     * decoded last. The row must begin its group of equal keys (or keys be unique): its offset is
     * counted from it. {@return false when the ordinal is past the dataset's rows}
     */
    boolean seekOrdinal(long ordinal) {
        int p = parts.partAt(ordinal);
        if (p >= parts.size()) {
            return exhaust(p);
        }
        PartFile candidate = p == part ? pf : parts.get(p, ctx);
        long r = ordinal - parts.start(p);
        int lo = 0;
        int hi = candidate.groups.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (candidate.groupStart[candidate.groups[mid]] <= r) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        KeyIndex destination = index != null && p == part && lo == slot
                ? index : candidate.keyIndex(candidate.groups[lo], ctx);
        moveGroup(p, candidate, lo, destination);
        long row = r - pf.groupStart[physical];
        int pg = index.pageAt(row);
        if (keys == null || decodedPart != part || decodedSlot != slot || decodedPage != pg) {
            load(pg);
        }
        page = pg;
        pos = (int) (row - index.firstRow[pg]);
        offsets[pos] = 0;
        for (int i = pos + 1; i < keys.length; i++) {
            offsets[i] = Arrays.equals(keys[i - 1], keys[i]) ? offsets[i - 1] + 1 : 0;
        }
        tailOffset = offsets[offsets.length - 1];
        exhausted = false;
        return true;
    }

    /**
     * Skips a row continuation after seek has found the group's first row. Its logical offset
     * becomes a physical row displacement. A stale/oversized offset may land after the group;
     * in that case re-seek strictly after the key rather than skipping unrelated rows.
     */
    boolean resume(Cursor.RowPosition after) {
        byte[] target = after.key().unsafeArray();
        if (!Arrays.equals(key(), target) || offset() > after.offset()) {
            return true;
        }
        if (after.offset() == Long.MAX_VALUE) {
            return seek(target, true);
        }
        long displacement = after.offset() + 1 - offset();
        int s = slot;
        long r = row();
        while (displacement >= pf.groupRows[pf.groups[s]] - r) {
            ctx.checkCancelled();
            displacement -= pf.groupRows[pf.groups[s]] - r;
            r = 0;
            if (++s == pf.groups.length) {
                // Equal keys never cross parts (PART-4): the group ends with this part.
                return seek(target, true);
            }
        }
        r += displacement;
        KeyIndex destination = pf.keyIndex(pf.groups[s], ctx);
        int pg = destination.pageAt(r);
        // Save the original position so an oversized continuation can seek the real group end.
        if (s == slot && pg == page) {
            int next = (int) (r - index.firstRow[page]);
            if (!Arrays.equals(keys[next], target)) {
                return seek(target, true);
            }
            pos = next;
            return true;
        }
        countSeek(s, pg);
        ctx.enterGroup(part, pf.groups[s]);
        byte[][] decoded = pf.keyPage(pf.groups[s], destination, pg, ctx);
        ctx.checkKeyPage(pf, pf.groups[s], pg, destination.firstRow[pg], decoded);
        int next = (int) (r - destination.firstRow[pg]);
        if (!Arrays.equals(decoded[next], target)) {
            return seek(target, true);
        }
        moveGroup(part, pf, s, destination);
        page = pg;
        keys = decoded;
        pos = next;
        offsets = new long[decoded.length];
        // All rows through next belong to target (sorted order, and the original row did too).
        offsets[next] = after.offset() + 1;
        for (int i = next + 1; i < decoded.length; i++) {
            offsets[i] = Arrays.equals(decoded[i - 1], decoded[i]) ? offsets[i - 1] + 1 : 0;
        }
        tailOffset = offsets[offsets.length - 1];
        tailKey = decoded[decoded.length - 1];
        decodedPart = part;
        decodedSlot = slot;
        decodedPage = pg;
        decodedLastPage = pg + 1 == index.pages();
        return true;
    }

    /** {@return whether a '/' listing may land on list-index anchors: a list index is built} */
    boolean listIndexed() {
        return listIndex != null && listIndex.groups.length != 0;
    }

    /**
     * Predicts the key pages a listing will decode next in the row group the cursor is on, and reads ahead
     * those not yet pending, as far as {@code ahead} has room, at most one per entry the listing can still
     * return ({@code remaining}). The cursor is on the key whose entry the listing has just emitted: a common
     * prefix c, after which it seeks to successor(c) and lands on the first page from the current one whose
     * maximum reaches it ({@link KeyIndex#pageFor}), or a row, after which it decodes the next page once the
     * current one is done. A predicted page's maximum then stands for the entry after it: its common prefix,
     * or a row when it has none. Bounds may be shortened or overlap (RG-5, RG-6), so a prediction may be
     * wrong, which costs a read, never a result. It stops at the row group's end and at a page beginning at
     * or after the cursor's upper bound.
     */
    void readAhead(ReadAhead ahead, byte[] prefix, byte delimiter, int remaining) {
        if (ahead == null || remaining <= 0 || exhausted || syntheticKey != null || keys == null || index == null) {
            return;
        }
        byte[] cur = commonPrefix(keys[pos], prefix, delimiter);
        if (!ahead.predictFrom(pf, physical, page, cur == null)) {
            return;
        }
        int at = page;
        for (int n = 0; n < remaining;) {
            int j = at + 1;
            if (cur != null) {
                var next = Bytes.wrap(cur).successor();
                if (next.isEmpty()) {
                    return;
                }
                j = index.pageFor(at, next.get().unsafeArray(), false);
                if (j == at) {
                    // The next entry may be on this page: predict past its maximum's common prefix when that is later.
                    byte[] c = commonPrefix(index.hi[at], prefix, delimiter);
                    if (c != null && Arrays.compareUnsigned(c, cur) > 0) {
                        cur = c;
                        continue;
                    }
                    j = at + 1;
                }
            }
            if (j >= index.pages() || end != null && Arrays.compareUnsigned(index.lo[j], end) >= 0) {
                return;
            }
            if (!ahead.fetch(pf, physical, j, index.offset[j], index.size[j])) {
                return;
            }
            n++;
            at = j;
            cur = commonPrefix(index.hi[j], prefix, delimiter);
        }
    }

    /** {@return the common prefix of {@code key} after {@code prefix} through the first {@code delimiter}, or null} */
    private static byte[] commonPrefix(byte[] key, byte[] prefix, byte delimiter) {
        if (key.length < prefix.length || !Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length)) {
            return null;
        }
        for (int i = prefix.length; i < key.length; i++) {
            if (key[i] == delimiter) {
                return Arrays.copyOf(key, i + 1);
            }
        }
        return null;
    }

    /** Decodes from (part, slot, page) forward until a key satisfies the target. */
    private boolean decodeForward(int p, int s, int pg, byte[] target, boolean exclusive) {
        if (!enter(p, s, pg)) {
            return false;
        }
        while (true) {
            int i = firstSatisfying(keys, 0, target, exclusive);
            if (i < keys.length) {
                pos = i;
                return true;
            }
            int next = index.pageFor(page + 1, target, exclusive);
            if (next < index.pages()) {
                load(next);
                continue;
            }
            if (!advanceGroup()) {
                return false;
            }
        }
    }

    /** Moves to the next key. Returns false at the end of the dataset. */
    boolean next() {
        materialize();
        if (exhausted) {
            return false;
        }
        pos++;
        if (pos < keys.length) {
            return true;
        }
        if (page + 1 < index.pages()) {
            load(page + 1);
            return true;
        }
        return advanceGroup();
    }

    /** Moves to the first page of the next non-empty row group, in this part or the next. */
    private boolean advanceGroup() {
        if (slot + 1 < pf.groups.length) {
            return enter(part, slot + 1, 0);
        }
        if (part + 1 < parts.size()) {
            return enter(part + 1, 0, 0);
        }
        return exhaust(part + 1);
    }

    private boolean enter(int p, int s, int pg) {
        if (p >= parts.size() || (p != part && beyondEnd(p))) {
            return exhaust(p);
        }
        PartFile candidate = p == part ? pf : parts.get(p, ctx);
        KeyIndex destination = index != null && p == part && s == slot
                ? index : candidate.keyIndex(candidate.groups[s], ctx);
        moveGroup(p, candidate, s, destination);
        load(pg);
        return true;
    }

    /** Commits every ordinary or synthetic group transition before projection. */
    private void moveGroup(int p, PartFile candidate, int s, KeyIndex destination) {
        ctx.enterGroup(p, candidate.groups[s]);
        part = p; pf = candidate; slot = s; physical = candidate.groups[s]; index = destination;
    }

    /** Decodes a key page of the current row group and checks its order against the last page decoded. */
    private void load(int pg) {
        countSeek(slot, pg);
        ctx.enterGroup(part, physical);
        byte[][] decoded = pf.keyPage(physical, index, pg, ctx);
        ctx.checkKeyPage(pf, physical, pg, index.firstRow[pg], decoded);
        boolean adjacent = decodedPart == part
                && ((decodedSlot == slot && decodedPage + 1 == pg)
                    || (decodedSlot + 1 == slot && pg == 0 && decodedLastPage));
        // A seek skips only pages bounded below its target (at or below for exclusive seeks). A returned group
        // therefore starts here unless the immediately preceding decoded page ends in it.
        offsets = new long[decoded.length];
        if (!pf.unique) {
            offsets[0] = adjacent && Arrays.equals(decoded[0], tailKey) ? tailOffset + 1 : 0;
            for (int i = 1; i < decoded.length; i++) {
                offsets[i] = Arrays.equals(decoded[i - 1], decoded[i]) ? offsets[i - 1] + 1 : 0;
            }
        }
        tailOffset = offsets[offsets.length - 1];
        decodedPart = part;
        decodedSlot = slot;
        decodedLastPage = pg + 1 == index.pages();
        decodedPage = pg;
        tailKey = decoded[decoded.length - 1];
        page = pg;
        keys = decoded;
        pos = 0;
    }

    /**
     * Counts a page index seek when decoding page {@code pg} of row group slot {@code s} of the current
     * part skips a page: one after the page decoded last in the same group, or a group's first.
     */
    private void countSeek(int s, int pg) {
        boolean next = decodedPart == part && decodedSlot == s ? pg == decodedPage + 1 : pg == 0;
        if (!next) {
            ctx.stats.pageIndexSeeks++;
        }
    }

    /** {@return whether part p holds no key below the upper bound, by its manifest minimum key} */
    private boolean beyondEnd(int p) {
        return end != null && Arrays.compareUnsigned(parts.entry(p).minKey().unsafeArray(), end) >= 0;
    }

    /** {@return the first index at or after {@code from} whose key satisfies the target, or keys.length} */
    private static int firstSatisfying(byte[][] keys, int from, byte[] target, boolean exclusive) {
        int lo = from;
        int hi = keys.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (satisfies(keys[mid], target, exclusive)) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }
}
