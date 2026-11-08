// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// Stats counts the decoding work of reads: pages and keys decoded and the
// routing decisions taken. A read adds to the Stats it is given; the caller
// owns it and its synchronization. It does not count work done on open,
// page index loads, or bytes read from the source, so it is not a measure
// of total I/O.
type Stats struct {
	// PagesDecoded counts data pages decoded, key and value columns.
	PagesDecoded int64
	// KeysDecoded counts key values decoded from key pages.
	KeysDecoded int64
	// GroupsOpened counts key cursors opened on a row group.
	GroupsOpened int64
	// Reseeks counts key cursor moves that skipped at least one page of
	// their row group through the page index instead of decoding it.
	Reseeks int64
	// GroupShortcuts counts row groups a listing skipped whole because
	// their first key and the next group's share one common prefix.
	GroupShortcuts int64
}

// keyCursor walks the key column of one row group forward, holding the
// page it decoded last.
type keyCursor struct {
	ctx      context.Context
	r        *Reader
	part     *part
	g        int // the row group's position in the dataset
	physical int
	index    *groupIndex
	pages    *parquet.FilePages
	page     parquet.Page
	values   []parquet.Value
	at       int // index of the decoded page, -1 before the first
	next     int // position within values
	// log holds the pages the request decoded, shared by its cursors.
	log   *pageLog
	stats *Stats
}

// cursor opens a key cursor on row group g, loading the group's key page
// index (RG-2 to RG-4) first.
func (r *Reader) cursor(ctx context.Context, g int, log *pageLog, stats *Stats) (*keyCursor, error) {
	grp := r.groups[g]
	p := r.parts[grp.part]
	index, err := p.keyIndex(ctx, grp.physical)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		stats = new(Stats)
	}
	stats.GroupsOpened++
	return &keyCursor{ctx: ctx, r: r, part: p, g: g, physical: grp.physical, index: index, at: -1, stats: stats, log: log}, nil
}

// seek returns the first key at or after from (after it when !inclusive)
// from the cursor's position on, and its row, or ok=false when the row
// group holds none. The key aliases the decoded page until the cursor moves
// to another page or closes.
func (c *keyCursor) seek(from []byte, inclusive bool) (key []byte, row int64, ok bool, err error) {
	for {
		for ; c.next < len(c.values); c.next++ {
			key := c.values[c.next].ByteArray()
			if from == nil {
				return key, c.index.pages[c.at].first + int64(c.next), true, nil
			}
			if order := bytes.Compare(key, from); order > 0 || order == 0 && inclusive {
				return key, c.index.pages[c.at].first + int64(c.next), true, nil
			}
		}
		target := c.at + 1
		if from != nil {
			if page := c.index.pageFor(target, from); page > target {
				if c.at >= 0 {
					c.stats.Reseeks++
				}
				target = page
			}
		}
		if target >= len(c.index.pages) {
			return nil, 0, false, nil
		}
		if err := c.decode(target); err != nil {
			return nil, 0, false, err
		}
	}
}

// advance moves past the current key and returns the next one, as seek.
func (c *keyCursor) advance() (key []byte, row int64, ok bool, err error) {
	c.next++
	return c.seek(nil, true)
}

// decode reads key page i and checks it (CHECK-2): its header locates
// exactly one data page of the rows its OffsetIndex entry gives it (RG-3);
// it holds one non-empty key per row (KEY-2), valid UTF-8 for a STRING key
// (KEY-6), ordered according to the declared uniqueness mode, inside the
// page's ColumnIndex bounds (RG-5),
// inside its part's bounds and equal to them at the part's ends
// (bounds_mismatch), and in dataset order with every other page the request
// decoded (KEY-3).
func (c *keyCursor) decode(i int) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	p := c.part
	meta := c.index.pages[i]
	if err := p.checkKeyPageHeader(c.ctx, c.physical, i, meta); err != nil {
		return err
	}
	if c.pages == nil {
		chunk, err := p.chunk(c.physical, p.key)
		if err != nil {
			return err
		}
		// SeekToRow uses the OffsetIndex once the chunk has loaded it,
		// which keyIndex did.
		c.pages = chunk.PagesFrom(p.at(c.ctx))
	}
	if c.page != nil {
		parquet.Release(c.page)
		c.page, c.values = nil, c.values[:0]
	}
	page, values, err := readPage(c.pages, meta.first, c.values)
	if err != nil {
		return p.dataError(c.ctx, c.physical, fmt.Errorf("key page %d: %w", i, err))
	}
	c.page, c.values, c.at, c.next = page, values, i, 0
	c.stats.PagesDecoded++
	c.stats.KeysDecoded += int64(len(values))
	if int64(len(values)) != meta.rows {
		return refusePart(CodeInvalidPageIndex, p.name, c.physical, "key page %d holds %d rows, its OffsetIndex entry %d", i, len(values), meta.rows)
	}
	var previous []byte
	for j, value := range values {
		key := value.ByteArray()
		row := meta.first + int64(j)
		if value.IsNull() || len(key) == 0 || p.utf8Key && !utf8.Valid(key) {
			e := refusePart(CodeInvalidKey, p.name, c.physical, "an empty key, or a STRING key that is not valid UTF-8")
			e.Row = row
			return e
		}
		if j > 0 && c.r.outOfOrder(previous, key) {
			return keyOrder(p.name, c.physical, row, key)
		}
		previous = key
	}
	first, last := values[0].ByteArray(), values[len(values)-1].ByteArray()
	if bytes.Compare(first, meta.lo) < 0 || bytes.Compare(last, meta.hi) > 0 {
		return refusePart(CodeInvalidPageIndex, p.name, c.physical, "key page %d holds a key outside its ColumnIndex bounds", i)
	}
	if err := c.checkPartBounds(i, first, last); err != nil {
		return err
	}
	if err := c.log.add(pageSpan{g: c.g, page: i, part: p.name, physical: c.physical, firstRow: meta.first, first: first, last: last, repeated: !c.r.key.Unique}); err != nil {
		return err
	}
	if i == 0 {
		return c.r.noteFirst(c.g, first)
	}
	return nil
}

// checkPartBounds requires a decoded page's keys to lie within its part's
// manifest bounds, and the part's first and last keys to equal them.
func (c *keyCursor) checkPartBounds(i int, first, last []byte) error {
	p := c.part
	bad := func(what string) error {
		return refusePart(CodeBoundsMismatch, p.name, c.physical, "key page %d: %s", i, what)
	}
	if p.minKey != nil {
		if cmp := bytes.Compare(first, p.minKey); cmp < 0 || cmp != 0 && i == 0 && c.g == p.firstGroup {
			return bad("the part's first key differs from its minimum key")
		}
	}
	if p.maxKey != nil {
		if cmp := bytes.Compare(last, p.maxKey); cmp > 0 || cmp != 0 && i == len(c.index.pages)-1 && c.g == p.endGroup-1 {
			return bad("the part's last key differs from its maximum key")
		}
	}
	return nil
}

// checkKeyPageHeader reads the page header an OffsetIndex entry locates and
// requires it to begin exactly one data page that fills the entry and
// holds the entry's rows (RG-3).
func (p *part) checkKeyPageHeader(ctx context.Context, physical, i int, meta keyPage) error {
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidPageIndex, p.name, physical, "key page %d: "+format, append([]any{i}, args...)...)
	}
	header, length, err := p.pageHeader(ctx, physical, p.key, meta.offset, meta.offset+meta.size)
	if err != nil {
		var refusal *RefusalError
		if errors.As(err, &refusal) && refusal.Code == CodeInvalidParquet {
			return bad("its OffsetIndex entry does not locate a page header")
		}
		return err
	}
	var rows int64
	switch header.Type {
	case format.DataPage:
		h, ok := header.DataPageHeader.Get()
		if !ok {
			return bad("a data page without its header")
		}
		rows = int64(h.NumValues)
	case format.DataPageV2:
		h, ok := header.DataPageHeaderV2.Get()
		if !ok {
			return bad("a data page without its header")
		}
		rows = int64(h.NumRows)
	default:
		return bad("its OffsetIndex entry locates a page of type %s, not a data page", header.Type)
	}
	if length+int64(header.CompressedPageSize) != meta.size {
		return bad("its OffsetIndex entry does not locate exactly one page")
	}
	if rows != meta.rows {
		return bad("the page holds %d rows, its OffsetIndex entry %d", rows, meta.rows)
	}
	return nil
}

func (c *keyCursor) close() {
	if c.page != nil {
		parquet.Release(c.page)
		c.page = nil
	}
	if c.pages != nil {
		_ = c.pages.Close()
		c.pages = nil
	}
	c.values = nil
}
