// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;

/**
 * Walks the dataset's keys forward, in dataset order, for one request.
 *
 * <p>Seeking routes in three steps. Parts are chosen by the manifest's exact bounds: the first part
 * whose last key reaches the target. Row groups within a part are chosen by binary search over the
 * maximum of each group's last key page, and pages within a row group by binary search over the
 * page maxima ({@link KeyIndex#pageFor}). Bounds may be shortened, so both searches may land early,
 * never late; decoding forward from there finds the first key at or after the target.
 *
 * <p>The cursor never moves backwards, so it decodes key pages in ascending dataset position, and
 * checking each page's first key against the last key decoded before it checks the page against
 * every page the request decoded (CHECK-2).
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
    private boolean exhausted;

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
            int pg = index.pageFor(page + 1, target);
            if (pg < index.pages()) {
                return decodeForward(p, slot, pg, target, exclusive);
            }
            firstSlot = slot + 1;
        } else if (Arrays.compareUnsigned(pf.entry.minKey().unsafeArray(), target) >= 0) {
            // The whole part is at or after the target: start at its first key.
            return decodeForward(p, 0, 0, target, exclusive);
        }
        // The first row group whose last page's maximum reaches the target.
        int lo = firstSlot;
        int hi = pf.groups.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Arrays.compareUnsigned(pf.keyIndex(pf.groups[mid], ctx).upper(), target) >= 0) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (lo >= pf.groups.length) {
            // No row group of the part reaches it: continue at the next part's first key.
            if (p + 1 >= parts.length) {
                exhausted = true;
                return false;
            }
            return decodeForward(p + 1, 0, 0, target, exclusive);
        }
        int pg = pf.keyIndex(pf.groups[lo], ctx).pageFor(0, target);
        return decodeForward(p, lo, Math.min(pg, pf.keyIndex(pf.groups[lo], ctx).pages() - 1), target, exclusive);
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
            int next = index.pageFor(page + 1, target);
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
        byte[][] decoded = pf.keyPage(physical, index, pg, ctx);
        if (ctx.lastKey != null && Arrays.compareUnsigned(decoded[0], ctx.lastKey) <= 0) {
            throw new RefusalException(RefusalCode.KEY_ORDER, pf.name, "the first key of row group " + physical + " key page " + pg
                    + " does not sort after the last key of " + ctx.lastKeyWhere);
        }
        ctx.lastKey = decoded[decoded.length - 1];
        ctx.lastKeyWhere = "part " + pf.name + " row group " + physical + " key page " + pg;
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
