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

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// keyPage is one key page as its row group's page indexes describe it.
type keyPage struct {
	// lo and hi are the ColumnIndex bounds. Parquet's rules make them
	// bounds whatever their length (RG-5): lo is at or below the page's
	// first key, hi at or above its last. A writer may shorten them, so
	// adjacent pages' bounds may overlap and need not be monotone (RG-6).
	lo, hi []byte
	first  int64
	rows   int64
	// offset and size locate the page, header included (RG-3).
	offset, size int64
}

// mayHoldAtOrAfter reports whether the page can hold a key at or after
// from.
func (page *keyPage) mayHoldAtOrAfter(from []byte) bool {
	return from == nil || bytes.Compare(page.hi, from) >= 0
}

// groupIndex is a row group's checked key page index.
type groupIndex struct {
	pages []keyPage
	// size is the memory the index retains, counted against
	// Limits.IndexMemory.
	size int64
}

// pageFor returns a page at or after start, and no later than the first
// page whose keys reach from: decoding forward from it finds the first key
// at or after from. Every page that holds a key at or after from has a
// maximum at or after it, and those pages are a suffix of the group, so
// the binary search never passes the first of them. A page before them
// whose maximum was shortened above its keys may stop the search early,
// which costs only a page read.
func (index *groupIndex) pageFor(start int, from []byte) int {
	lo, hi := start, len(index.pages)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if index.pages[mid].mayHoldAtOrAfter(from) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// keyIndex returns the checked key page index of a non-empty physical row
// group, loading it on first use (RG-2 to RG-4), within the limits on page
// indexes and on the memory the Reader retains for them. Concurrent first
// uses may both load it; one result is kept.
func (p *part) keyIndex(ctx context.Context, physical int) (*groupIndex, error) {
	if index := p.index[physical].Load(); index != nil {
		return index, nil
	}
	meta := &p.footer.Metadata().RowGroups[physical].Columns[p.key]
	if meta.ColumnIndexOffset == 0 || meta.ColumnIndexLength == 0 || meta.OffsetIndexOffset == 0 || meta.OffsetIndexLength == 0 {
		return nil, refusePart(CodeMissingPageIndex, p.name, physical, "the key column lacks a ColumnIndex or an OffsetIndex")
	}
	start, end, err := p.openChunk(ctx, physical, p.key)
	if err != nil {
		return nil, err
	}
	raw, err := p.readIndexBytes(ctx, physical, meta.ColumnIndexOffset, meta.ColumnIndexLength)
	if err != nil {
		return nil, p.indexError(ctx, physical, "ColumnIndex", err)
	}
	var columns format.ColumnIndex
	if _, err := decodeBounded(raw, &columns, p.r.limits.MetadataMemory); err != nil {
		return nil, p.indexError(ctx, physical, "ColumnIndex", err)
	}
	offsets, err := p.loadOffsetIndex(ctx, physical, p.key)
	if err != nil {
		return nil, p.indexError(ctx, physical, "OffsetIndex", err)
	}
	index, err := checkKeyIndex(&columns, offsets, p.footer.Metadata().RowGroups[physical].NumRows, start, end, p.utf8Key)
	if err != nil {
		return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "%v", err)
	}
	if limit := p.r.limits.IndexMemory; p.r.indexMemory.Add(index.size) > limit {
		p.r.indexMemory.Add(-index.size)
		return nil, exceeded("part %s: key page indexes need more than %d bytes (Limits.IndexMemory)", p.name, limit)
	}
	if !p.index[physical].CompareAndSwap(nil, index) {
		p.r.indexMemory.Add(-index.size)
		return p.index[physical].Load(), nil
	}
	return index, nil
}

// indexError classifies an error loading a key page index.
func (p *part) indexError(ctx context.Context, physical int, which string, err error) error {
	var refusal *RefusalError
	switch {
	case isEnvironment(ctx, err), errors.As(err, &refusal):
		return err
	case errors.Is(err, parquet.ErrMissingColumnIndex), errors.Is(err, parquet.ErrMissingOffsetIndex):
		return refusePart(CodeMissingPageIndex, p.name, physical, "the key column has no %s", which)
	}
	return refusePart(CodeInvalidPageIndex, p.name, physical, "read the key %s: %v", which, err)
}

// checkKeyIndex builds a row group's key pages from its ColumnIndex and
// OffsetIndex after checking RG-3 and RG-4 as far as the indexes alone
// allow: a non-empty list, the same number of entries in every parallel
// list, no null page, first rows starting at zero and ascending strictly
// below rows, and every page inside the column chunk [start, end), in
// order, without overlap, computed without overflow. Bounds must not be
// inverted (an inverted pair bounds no key, so it contradicts RG-5 for any
// page) and, for a STRING key, must be valid UTF-8 (KEY-6). The order of
// bounds from page to page is not checked (RG-6), and boundary_order is
// never read. The page headers are checked as each page is decoded.
func checkKeyIndex(columns *format.ColumnIndex, offsets *parquet.FileOffsetIndex, rows, start, end int64, utf8Key bool) (*groupIndex, error) {
	if offsets == nil {
		return nil, errors.New("the key OffsetIndex is missing")
	}
	n := offsets.NumPages()
	if n == 0 {
		return nil, errors.New("the key OffsetIndex is empty")
	}
	if len(columns.NullPages) != n || len(columns.MinValues) != n || len(columns.MaxValues) != n {
		return nil, errors.New("the key ColumnIndex lists and the OffsetIndex disagree on the page count")
	}
	index := &groupIndex{pages: make([]keyPage, n), size: int64(n) * int64(unsafe.Sizeof(keyPage{}))}
	previousEnd := start
	for i := range index.pages {
		if columns.NullPages[i] {
			return nil, errors.New("a key page is marked null")
		}
		first := offsets.FirstRowIndex(i)
		last := rows
		if i+1 < n {
			last = offsets.FirstRowIndex(i + 1)
		}
		if i == 0 && first != 0 || first < 0 || last <= first || last > rows {
			return nil, errors.New("the key OffsetIndex row positions are invalid")
		}
		off, size := offsets.Offset(i), offsets.CompressedPageSize(i)
		if size <= 0 || off < previousEnd || off > end-size {
			return nil, errors.New("a key page location lies outside its column chunk, out of order or overlapping")
		}
		previousEnd = off + size
		lo := bytes.Clone(columns.MinValues[i])
		hi := bytes.Clone(columns.MaxValues[i])
		if bytes.Compare(lo, hi) > 0 {
			return nil, errors.New("a key page's minimum is above its maximum")
		}
		if utf8Key && (!utf8.Valid(lo) || !utf8.Valid(hi)) {
			return nil, errors.New("a key page bound is not valid UTF-8")
		}
		index.pages[i] = keyPage{lo: lo, hi: hi, first: first, rows: last - first, offset: off, size: size}
		index.size += int64(len(lo) + len(hi))
	}
	return index, nil
}

// pageSpan is one key page a request decoded, at its dataset position.
type pageSpan struct {
	repeated    bool
	g, page     int
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
	spans []pageSpan
}

// add checks a decoded page, whose own keys are known to ascend, against
// its neighbours in dataset order and records it. It returns the key_order
// refusal of the later of two pages out of order.
func (l *pageLog) add(s pageSpan) error {
	i := sort.Search(len(l.spans), func(i int) bool {
		t := l.spans[i]
		return t.g > s.g || t.g == s.g && t.page >= s.page
	})
	if i < len(l.spans) && l.spans[i].g == s.g && l.spans[i].page == s.page {
		return nil // read again
	}
	if i > 0 && (bytes.Compare(s.first, l.spans[i-1].last) < 0 || !s.repeated && bytes.Equal(s.first, l.spans[i-1].last)) {
		return keyOrder(s.part, s.physical, s.firstRow, s.first)
	}
	if i < len(l.spans) && (bytes.Compare(l.spans[i].first, s.last) < 0 || !s.repeated && bytes.Equal(l.spans[i].first, s.last)) {
		t := l.spans[i]
		return keyOrder(t.part, t.physical, t.firstRow, t.first)
	}
	s.first, s.last = bytes.Clone(s.first), bytes.Clone(s.last)
	l.spans = append(l.spans, pageSpan{})
	copy(l.spans[i+1:], l.spans[i:])
	l.spans[i] = s
	return nil
}
