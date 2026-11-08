// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"

	"github.com/parquet-go/parquet-go"
)

// keyPage is one key page as its row group's page indexes describe it.
type keyPage struct {
	// lo and hi are the ColumnIndex bounds. Parquet's rules make them
	// bounds whatever their length (RG-5): lo is at or below the page's
	// first key, hi at or above its last. A writer may shorten them, so
	// adjacent pages' bounds may overlap (RG-6), and hi need not ascend
	// from page to page.
	lo, hi []byte
	first  int64
	rows   int64
}

// mayHoldAtOrAfter reports whether the page can hold a key at or after
// from.
func (page *keyPage) mayHoldAtOrAfter(from []byte) bool {
	return from == nil || bytes.Compare(page.hi, from) >= 0
}

// groupIndex is a row group's checked key page index.
type groupIndex struct {
	pages []keyPage
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
// group, loading it on first use (RG-2 to RG-4). Concurrent first uses may
// both load it; one result is kept.
func (p *part) keyIndex(ctx context.Context, physical int) (*groupIndex, error) {
	if index := p.index[physical].Load(); index != nil {
		return index, nil
	}
	meta := p.footer.Metadata().RowGroups[physical].Columns[p.key]
	if meta.ColumnIndexOffset == 0 || meta.ColumnIndexLength == 0 || meta.OffsetIndexOffset == 0 || meta.OffsetIndexLength == 0 {
		return nil, refusePart(CodeMissingPageIndex, p.name, physical, "the key column lacks a ColumnIndex or an OffsetIndex")
	}
	for _, loc := range [][2]int64{{meta.ColumnIndexOffset, int64(meta.ColumnIndexLength)}, {meta.OffsetIndexOffset, int64(meta.OffsetIndexLength)}} {
		if loc[0] < 0 || loc[1] < 0 || loc[0] > p.size-loc[1] {
			return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "a key page index lies outside the file")
		}
	}
	chunk, err := p.chunk(physical, p.key)
	if err != nil {
		return nil, err
	}
	columnIndex, err := chunk.ColumnIndexFrom(p.at(ctx))
	if err != nil {
		return nil, p.indexError(ctx, physical, "ColumnIndex", err)
	}
	offsets, err := chunk.OffsetIndexFrom(p.at(ctx))
	if err != nil {
		return nil, p.indexError(ctx, physical, "OffsetIndex", err)
	}
	index, err := checkKeyIndex(columnIndex, offsets, p.footer.Metadata().RowGroups[physical].NumRows, p.size)
	if err != nil {
		return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "%v", err)
	}
	if !p.index[physical].CompareAndSwap(nil, index) {
		return p.index[physical].Load(), nil
	}
	return index, nil
}

// indexError classifies an error loading a key page index.
func (p *part) indexError(ctx context.Context, physical int, which string, err error) error {
	switch {
	case ctx.Err() != nil:
		return err
	case errors.Is(err, parquet.ErrMissingColumnIndex), errors.Is(err, parquet.ErrMissingOffsetIndex):
		return refusePart(CodeMissingPageIndex, p.name, physical, "the key column has no %s", which)
	case errors.Is(err, ErrClosed), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	}
	return refusePart(CodeInvalidPageIndex, p.name, physical, "read the key %s: %v", which, err)
}

// checkKeyIndex builds a row group's key pages from its ColumnIndex and
// OffsetIndex after checking RG-3 and RG-4: the same number of pages, at
// least one, none null, first rows starting at zero and ascending strictly
// below rows, each page inside the file, and bounds that are not inverted
// (an inverted pair bounds no key, so it contradicts RG-5 for any page).
// The order of bounds from page to page is not checked: they may overlap
// (RG-6), and boundary_order is never read.
func checkKeyIndex(columns parquet.ColumnIndex, offsets parquet.OffsetIndex, rows, size int64) (*groupIndex, error) {
	n := columns.NumPages()
	if n == 0 || offsets.NumPages() != n {
		return nil, errors.New("the key ColumnIndex and OffsetIndex disagree on the page count")
	}
	index := &groupIndex{pages: make([]keyPage, n)}
	for i := range index.pages {
		if columns.NullPage(i) {
			return nil, errors.New("a key page is marked null")
		}
		first := offsets.FirstRowIndex(i)
		end := rows
		if i+1 < n {
			end = offsets.FirstRowIndex(i + 1)
		}
		if i == 0 && first != 0 || first < 0 || end <= first || end > rows {
			return nil, errors.New("the key OffsetIndex row positions are invalid")
		}
		if off, length := offsets.Offset(i), offsets.CompressedPageSize(i); off < 0 || length <= 0 || off > size-length {
			return nil, errors.New("a key page lies outside the file")
		}
		lo := bytes.Clone(columns.MinValue(i).ByteArray())
		hi := bytes.Clone(columns.MaxValue(i).ByteArray())
		if bytes.Compare(lo, hi) > 0 {
			return nil, errors.New("a key page's minimum is above its maximum")
		}
		index.pages[i] = keyPage{lo: lo, hi: hi, first: first, rows: end - first}
	}
	return index, nil
}
