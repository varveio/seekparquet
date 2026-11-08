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
 */
final class KeyCursor {

    private final PartFile[] parts;
    private final ReadContext ctx;

    private int part = -1;
    /** Index into {@code parts[part].groups}. */
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

    long offset() {
        return offsets[pos];
    }

    KeyCursor(PartFile[] parts, ReadContext ctx) {
        this.parts = parts;
        this.ctx = ctx;
    }

    /** The current key. Valid after a seek or next that returned true. */
    byte[] key() {
        return keys[pos];
    }

    /** The part of the current key. */
    PartFile partFile() {
        return parts[part];
    }

    /** The physical row group of the current key. */
    int physical() {
        return physical;
    }

    /** The row of the current key within its row group. */
    long row() {
        return index.firstRow[page] + pos;
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
        if (part < 0 || !satisfies(parts[part].entry.maxKey().unsafeArray(), target, exclusive)) {
            int lo = part < 0 ? 0 : part + 1;
            int hi = parts.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (satisfies(parts[mid].entry.maxKey().unsafeArray(), target, exclusive)) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            p = lo;
        }
        if (p >= parts.length) {
            exhausted = true;
            return false;
        }
        PartFile pf = parts[p];
        int firstSlot = 0;
        if (p == part && keys != null) {
            // Later pages of the current row group first.
            int pg = index.pageFor(page + 1, target, exclusive);
            if (pg < index.pages()) {
                return decodeForward(p, slot, pg, target, exclusive);
            }
            firstSlot = slot + 1;
        } else if (Arrays.compareUnsigned(pf.entry.minKey().unsafeArray(), target) >= 0) {
            // The whole part is at or after the target: start at its first key.
            return decodeForward(p, 0, 0, target, exclusive);
        }
        // Exact first-key probes are monotone in a valid sorted file. Keep the predecessor:
        // the target can start at its tail even when the following group starts with it.
        int lo = firstSlot;
        if (pf.groups.length - firstSlot > 8) {
            int hi = pf.groups.length;
            while (lo < hi) {
                ctx.checkCancelled();
                int mid = (lo + hi) >>> 1;
                if (satisfies(pf.firstKey(pf.groups[mid], ctx), target, exclusive)) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            lo = Math.max(firstSlot, lo - 1);
        }
        // Bounds may overlap and be non-monotone; visit the earliest remaining candidate.
        while (lo < pf.groups.length
                && !satisfies(pf.keyIndex(pf.groups[lo], ctx).upper(), target, exclusive)) {
            lo++;
        }
        if (lo >= pf.groups.length) {
            // No row group of the part reaches it: continue at the next part's first key.
            if (p + 1 >= parts.length) {
                exhausted = true;
                return false;
            }
            return decodeForward(p + 1, 0, 0, target, exclusive);
        }
        int pg = pf.keyIndex(pf.groups[lo], ctx).pageFor(0, target, exclusive);
        return decodeForward(p, lo, Math.min(pg, pf.keyIndex(pf.groups[lo], ctx).pages() - 1), target, exclusive);
    }

    /**
     * Skips a row continuation after seek has found the group's first row. Its logical offset
     * becomes a physical row displacement. A stale/oversized offset may land after the group;
     * in that case re-seek strictly after the key rather than skipping unrelated rows.
     */
    boolean resume(Continuation.RowPosition after) {
        byte[] target = after.bytes().unsafeArray();
        if (!Arrays.equals(key(), target) || offset() > after.offset()) {
            return true;
        }
        if (after.offset() == Long.MAX_VALUE) {
            return seek(target, true);
        }
        long displacement = after.offset() + 1 - offset();
        int p = part;
        int s = slot;
        long r = row();
        while (displacement >= parts[p].groupRows[parts[p].groups[s]] - r) {
            ctx.checkCancelled();
            displacement -= parts[p].groupRows[parts[p].groups[s]] - r;
            r = 0;
            if (++s == parts[p].groups.length) {
                s = 0;
                if (++p == parts.length) {
                    return seek(target, true);
                }
            }
        }
        r += displacement;
        KeyIndex destination = parts[p].keyIndex(parts[p].groups[s], ctx);
        int pg = destination.pageAt(r);
        // Save the original position so an oversized continuation can seek the real group end.
        if (p == part && s == slot && pg == page) {
            int next = (int) (r - index.firstRow[page]);
            if (!Arrays.equals(keys[next], target)) {
                return seek(target, true);
            }
            pos = next;
            return true;
        }
        ctx.enterGroup(p, parts[p].groups[s]);
        byte[][] decoded = parts[p].keyPage(parts[p].groups[s], destination, pg, ctx);
        ctx.checkKeyPage(parts[p], parts[p].groups[s], pg, decoded);
        int next = (int) (r - destination.firstRow[pg]);
        if (!Arrays.equals(decoded[next], target)) {
            return seek(target, true);
        }
        part = p;
        slot = s;
        physical = parts[p].groups[s];
        index = destination;
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
        if (slot + 1 < parts[part].groups.length) {
            return enter(part, slot + 1, 0);
        }
        if (part + 1 < parts.length) {
            return enter(part + 1, 0, 0);
        }
        exhausted = true;
        return false;
    }

    private boolean enter(int p, int s, int pg) {
        if (p >= parts.length) {
            exhausted = true;
            return false;
        }
        part = p;
        slot = s;
        physical = parts[p].groups[s];
        index = parts[p].keyIndex(physical, ctx);
        load(pg);
        return true;
    }

    /** Decodes a key page of the current row group and checks its order against the last page decoded. */
    private void load(int pg) {
        PartFile pf = parts[part];
        ctx.enterGroup(part, physical);
        byte[][] decoded = pf.keyPage(physical, index, pg, ctx);
        ctx.checkKeyPage(pf, physical, pg, decoded);
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
