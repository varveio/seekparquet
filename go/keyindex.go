// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"unicode/utf8"
	"unsafe"

	"github.com/parquet-go/parquet-go/format"
)

// keyPage is one key page as its row group's page indexes describe it.
type keyPage struct {
	pageLoc
	// lo and hi are the ColumnIndex bounds. Parquet's rules make them
	// bounds whatever their length (RG-5): lo is at or below the page's
	// first key, hi at or above its last. A writer may shorten them, so
	// adjacent pages' bounds may overlap and need not be monotone (RG-6).
	lo, hi []byte
}

// groupIndex is a row group's checked key page index.
type groupIndex struct {
	pages []keyPage
	// size is the memory the index retains, counted against
	// Limits.IndexMemory.
	size int64
}

// pageFor returns a page at or after start, and no later than the first
// page holding a key at or after from (after it unless inclusive):
// decoding forward from it finds the first such key. Every page that holds
// one has a maximum at or after from (after it unless inclusive), and
// those pages are a suffix of the group, so the binary search never passes
// the first of them. Pages before them whose maxima were written above
// their keys may stop the search early. Broad bounds can require decoding
// many extra pages, up to the whole row group.
func (index *groupIndex) pageFor(start int, from []byte, inclusive bool) int {
	return start + sort.Search(len(index.pages)-start, func(i int) bool {
		cmp := bytes.Compare(index.pages[start+i].hi, from)
		return from == nil || cmp > 0 || cmp == 0 && inclusive
	})
}

// pageAt returns the page holding row, which must be below the row
// group's row count.
func (index *groupIndex) pageAt(row int64) int {
	return sort.Search(len(index.pages), func(i int) bool { return index.pages[i].first > row }) - 1
}

// keyIndex returns the checked key page index of a non-empty physical row
// group, loading it on first use (RG-2 to RG-4), within the limits on page
// indexes and on the memory the Dataset retains for them. Concurrent first
// uses may both load it; one result is kept.
func (p *part) keyIndex(ctx context.Context, physical int, st *ReadStats) (*groupIndex, error) {
	return p.loadKeyIndex(ctx, physical, st, true)
}

// Build-only loads reuse existing entries but never charge or publish new ones.
func (p *part) loadKeyIndex(ctx context.Context, physical int, st *ReadStats, retain bool) (*groupIndex, error) {
	if index := p.index[physical].Load(); index != nil {
		return index, nil
	}
	meta := &p.footer.Metadata().RowGroups[physical].Columns[p.key.index]
	if meta.ColumnIndexOffset == 0 || meta.ColumnIndexLength == 0 || meta.OffsetIndexOffset == 0 || meta.OffsetIndexLength == 0 {
		return nil, refusePart(CodeMissingPageIndex, p.name, physical, "the key column lacks a ColumnIndex or an OffsetIndex")
	}
	start, end, ok := chunkRange(&meta.MetaData)
	if !ok || end > p.size {
		return nil, refusePart(CodeInvalidFooter, p.name, physical, "column chunk %d lies outside the file", p.key.index)
	}
	raw, err := p.readIndexBytes(ctx, physical, meta.ColumnIndexOffset, meta.ColumnIndexLength, st)
	if err != nil {
		return nil, p.indexError(ctx, physical, "ColumnIndex", err)
	}
	var columns format.ColumnIndex
	if _, err := decodeBounded(raw, &columns, p.r.limits.MetadataMemory); err != nil {
		return nil, p.indexError(ctx, physical, "ColumnIndex", err)
	}
	offsets, err := p.loadOffsetIndex(ctx, physical, p.key.index, st)
	if err != nil {
		return nil, p.indexError(ctx, physical, "OffsetIndex", err)
	}
	index, err := checkKeyIndex(&columns, offsets, p.footer.Metadata().RowGroups[physical].NumRows, start, end, p.utf8Key)
	if err != nil {
		return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "the key page index: %v", err)
	}
	if !retain {
		return index, nil
	}
	// Recheck, charge and publish as one operation. Competing cold loads
	// must not charge the same retained entry, even temporarily.
	p.r.indexMu.Lock()
	defer p.r.indexMu.Unlock()
	if cached := p.index[physical].Load(); cached != nil {
		return cached, nil
	}
	if err := p.r.reserveIndex(index.size); err != nil {
		return nil, err
	}
	st.IndexLoads++
	p.index[physical].Store(index)
	return index, nil
}

// indexError classifies an error loading a key page index.
func (p *part) indexError(ctx context.Context, physical int, which string, err error) error {
	var refusal *RefusalError
	if isEnvironment(ctx, err) || errors.As(err, &refusal) {
		return err
	}
	return refusePart(CodeInvalidPageIndex, p.name, physical, "read the key %s: %v", which, err)
}

// checkKeyIndex builds a row group's key pages from its ColumnIndex and
// OffsetIndex after checking RG-3 as far as the indexes alone allow
// (checkLocations) and RG-4: the same number of entries in every parallel
// list and no null page. Bounds must not be inverted (an inverted pair
// bounds no key, so it contradicts RG-5 for any page) and, for a STRING
// key, must be valid UTF-8 (KEY-6). The order of bounds from page to page
// is not checked (RG-6), and boundary_order is never read. The page
// headers are checked as each page is decoded.
func checkKeyIndex(columns *format.ColumnIndex, offsets *format.OffsetIndex, rows, start, end int64, utf8Key bool) (*groupIndex, error) {
	locs, err := checkLocations(offsets, rows, start, end)
	if err != nil {
		return nil, err
	}
	n := len(locs)
	if len(columns.NullPages) != n || len(columns.MinValues) != n || len(columns.MaxValues) != n {
		return nil, errors.New("the ColumnIndex lists and the OffsetIndex disagree on the page count")
	}
	index := &groupIndex{pages: make([]keyPage, n), size: int64(n) * int64(unsafe.Sizeof(keyPage{}))}
	for i, loc := range locs {
		if columns.NullPages[i] {
			return nil, errors.New("a key page is marked null")
		}
		lo, hi := bytes.Clone(columns.MinValues[i]), bytes.Clone(columns.MaxValues[i])
		if bytes.Compare(lo, hi) > 0 {
			return nil, errors.New("a key page's minimum is above its maximum")
		}
		if utf8Key && (!utf8.Valid(lo) || !utf8.Valid(hi)) {
			return nil, errors.New("a key page bound is not valid UTF-8")
		}
		index.pages[i] = keyPage{pageLoc: loc, lo: lo, hi: hi}
		index.size += int64(len(lo) + len(hi))
	}
	return index, nil
}

// pageSpan is one key page a request decoded, at its dataset position.
type pageSpan struct {
	g           groupRef
	page        int
	part        string
	physical    int
	firstRow    int64
	first, last []byte
}

// pageLog records the key pages a request decoded, in dataset order, so
// that each page is checked against the pages before and after it in the
// dataset whatever order they were decoded in (CHECK-2), and a page decoded
// again is not checked twice.
type pageLog struct {
	unique bool
	spans  []pageSpan
}

// ordered reports whether page before may precede page after: after begins
// above before's last key, or at it when keys may repeat and both pages
// are in one part, since a group never crosses parts (PART-4, KEY-7).
func (l *pageLog) ordered(before, after *pageSpan) bool {
	cmp := bytes.Compare(before.last, after.first)
	return cmp < 0 || cmp == 0 && !l.unique && before.part == after.part
}

// add checks a decoded page, whose own keys are in the declared order,
// against its neighbours in dataset order and records it. It returns the
// key_order refusal of the later of two pages out of order.
func (l *pageLog) add(s pageSpan) error {
	i := sort.Search(len(l.spans), func(i int) bool {
		t := l.spans[i]
		return s.g.less(t.g) || t.g == s.g && t.page >= s.page
	})
	if i < len(l.spans) && l.spans[i].g == s.g && l.spans[i].page == s.page {
		return nil // read again
	}
	if i > 0 && !l.ordered(&l.spans[i-1], &s) {
		return keyOrder(s.part, s.physical, s.firstRow, s.first)
	}
	if i < len(l.spans) && !l.ordered(&s, &l.spans[i]) {
		t := l.spans[i]
		return keyOrder(t.part, t.physical, t.firstRow, t.first)
	}
	s.first, s.last = bytes.Clone(s.first), bytes.Clone(s.last)
	l.spans = append(l.spans, pageSpan{})
	copy(l.spans[i+1:], l.spans[i:])
	l.spans[i] = s
	return nil
}
