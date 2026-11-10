// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
)

// keyPages walks the key column of one row group forward, holding the
// page it decoded last. Decoded pages are never reused, so a key it
// returns stays valid after it moves on.
type keyPages struct {
	// deferFirst leaves cross-group certification and first-key cache warming
	// to the list-index coordinator, after independent group decoding.
	deferFirst bool
	r          *reader
	part       *part
	g          groupRef // the row group's position in the dataset
	index      *groupIndex
	chunk      *chunk
	// values are the keys of page at, next the position in them.
	values   []parquet.Value
	at, next int
	// log holds the pages the request decoded, shared by its walkers.
	log *pageLog
}

// keyPages opens a walker on row group g, loading the group's key page
// index (RG-2 to RG-4) first.
func (r *reader) keyPages(ctx context.Context, g groupRef, log *pageLog, st *ReadStats) (*keyPages, error) {
	return r.loadKeyPages(ctx, g, log, st, true)
}

func (r *reader) loadKeyPages(ctx context.Context, g groupRef, log *pageLog, st *ReadStats, retain bool) (*keyPages, error) {
	p, err := r.part(ctx, g.part, st)
	if err != nil {
		return nil, err
	}
	grp := p.groups[g.index]
	index, err := p.loadKeyIndex(ctx, grp.physical, st, retain)
	if err != nil {
		return nil, err
	}
	st.RowGroupsOpened++
	c, err := p.openChunk(ctx, grp.physical, p.key, st, &st.KeyPagesDecoded)
	if err != nil {
		return nil, err
	}
	return &keyPages{r: r, part: p, g: g, index: index, chunk: c, at: -1, log: log}, nil
}

// seek returns the first key at or after from (after it when !inclusive)
// from the walker's position on, and its row, or ok=false when the row
// group holds none.
func (k *keyPages) seek(from []byte, inclusive bool) (key []byte, row int64, ok bool, err error) {
	for {
		for ; k.next < len(k.values); k.next++ {
			key := k.values[k.next].ByteArray()
			if order := bytes.Compare(key, from); from == nil || order > 0 || order == 0 && inclusive {
				return key, k.index.pages[k.at].first + int64(k.next), true, nil
			}
		}
		target := k.at + 1
		if from != nil {
			if page := k.index.pageFor(target, from, inclusive); page > target {
				if k.at >= 0 {
					k.chunk.stats.PageIndexSeeks++
				}
				target = page
			}
		}
		if target >= len(k.index.pages) {
			return nil, 0, false, nil
		}
		if err := k.decode(target); err != nil {
			return nil, 0, false, err
		}
	}
}

// advance moves past the current key and returns the next one, as seek.
func (k *keyPages) advance() (key []byte, row int64, ok bool, err error) {
	k.next++
	return k.seek(nil, true)
}

// seekRow moves to row of the row group, decoding only the page holding
// it, and returns its key.
func (k *keyPages) seekRow(row int64) ([]byte, error) {
	if i := k.index.pageAt(row); i != k.at {
		if i > k.at+1 {
			k.chunk.stats.PageIndexSeeks++
		}
		if err := k.decode(i); err != nil {
			return nil, err
		}
	}
	k.next = int(row - k.index.pages[k.at].first)
	return k.values[k.next].ByteArray(), nil
}

// rewind moves back to the row group's start without decoding anything.
func (k *keyPages) rewind() { k.values, k.at, k.next = nil, -1, 0 }

// decode reads key page i and checks it (CHECK-2): its header locates
// exactly one data page of the rows its OffsetIndex entry gives it (RG-3);
// it holds one non-empty key per row (KEY-2), valid UTF-8 for a STRING key
// (KEY-6), in the declared order (KEY-3), inside the page's ColumnIndex
// bounds (RG-5), inside its part's bounds and equal to them at the part's
// ends (bounds_mismatch), and in dataset order with every other page the
// request decoded (KEY-3).
func (k *keyPages) decode(i int) error {
	c := k.chunk
	if err := c.ctx.Err(); err != nil {
		return err
	}
	p := k.part
	meta := k.index.pages[i]
	values, err := c.indexed(i, meta.pageLoc)
	if err != nil {
		return err
	}
	k.values, k.at, k.next = values, i, 0
	c.stats.KeysDecoded += int64(len(values))
	var previous []byte
	for j, value := range values {
		key := value.ByteArray()
		row := meta.first + int64(j)
		if value.IsNull() || len(key) == 0 || p.utf8Key && !utf8.Valid(key) {
			e := refusePart(CodeInvalidKey, p.name, c.physical, "an empty key, or a STRING key that is not valid UTF-8")
			e.Row = row
			return e
		}
		if j > 0 && k.r.outOfOrder(previous, key) {
			return keyOrder(p.name, c.physical, row, key)
		}
		previous = key
	}
	first, last := values[0].ByteArray(), values[len(values)-1].ByteArray()
	if bytes.Compare(first, meta.lo) < 0 || bytes.Compare(last, meta.hi) > 0 {
		return refusePart(CodeInvalidPageIndex, p.name, c.physical, "key page %d holds a key outside its ColumnIndex bounds", i)
	}
	if err := k.checkPartBounds(i, first, last); err != nil {
		return err
	}
	if err := k.log.add(pageSpan{g: k.g, page: i, part: p.name, physical: c.physical, firstRow: meta.first, first: first, last: last}); err != nil {
		return err
	}
	if i == 0 && !k.deferFirst {
		return k.r.noteFirst(k.g, first)
	}
	return nil
}

// checkPartBounds requires a decoded page's keys to lie within its part's
// manifest bounds, and the part's first and last keys to equal them.
func (k *keyPages) checkPartBounds(i int, first, last []byte) error {
	p := k.part
	bad := func(what string) error {
		return refusePart(CodeBoundsMismatch, p.name, k.chunk.physical, "key page %d: %s", i, what)
	}
	if p.minKey != nil {
		if cmp := bytes.Compare(first, p.minKey); cmp < 0 || cmp != 0 && i == 0 && k.g.index == 0 {
			return bad("the part's first key differs from its minimum key")
		}
	}
	if p.maxKey != nil {
		if cmp := bytes.Compare(last, p.maxKey); cmp > 0 || cmp != 0 && i == len(k.index.pages)-1 && k.g.index == len(p.groups)-1 {
			return bad("the part's last key differs from its maximum key")
		}
	}
	return nil
}
