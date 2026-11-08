// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
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

// supportedCodec reports whether parquet-go decodes codec.
func supportedCodec(codec format.CompressionCodec) bool {
	switch codec {
	case format.Uncompressed, format.Snappy, format.Gzip, format.Brotli, format.Zstd, format.Lz4Raw:
		return true
	}
	return false
}

// checkCodec reports a column chunk compressed with a codec this reader
// does not decode as unsupported (ENC-1).
func (p *part) checkCodec(physical, column int) error {
	codec := p.footer.Metadata().RowGroups[physical].Columns[column].MetaData.Codec
	if !supportedCodec(codec) {
		return &UnsupportedError{Code: UnsupportedEncoding, Detail: "codec " + codec.String() + " in part " + p.name}
	}
	return nil
}

// chunkRange returns the byte range of a column chunk: from its dictionary
// page (when it has one) or first data page, for its compressed size.
func chunkRange(meta *format.ColumnMetaData) (start, end int64, ok bool) {
	start = meta.DataPageOffset
	if meta.DictionaryPageOffset > 0 && meta.DictionaryPageOffset < start {
		start = meta.DictionaryPageOffset
	}
	if start < 0 || meta.TotalCompressedSize < 0 || start > maxInt64-meta.TotalCompressedSize {
		return 0, 0, false
	}
	return start, start + meta.TotalCompressedSize, true
}

const maxInt64 = int64(^uint64(0) >> 1)

// keyIndex returns the checked key page index of a non-empty physical row
// group, loading it on first use (RG-2 to RG-4). Concurrent first uses may
// both load it; one result is kept.
func (p *part) keyIndex(ctx context.Context, physical int) (*groupIndex, error) {
	if index := p.index[physical].Load(); index != nil {
		return index, nil
	}
	chunkMeta := &p.footer.Metadata().RowGroups[physical].Columns[p.key]
	if chunkMeta.ColumnIndexOffset == 0 || chunkMeta.ColumnIndexLength == 0 || chunkMeta.OffsetIndexOffset == 0 || chunkMeta.OffsetIndexLength == 0 {
		return nil, refusePart(CodeMissingPageIndex, p.name, physical, "the key column lacks a ColumnIndex or an OffsetIndex")
	}
	for _, loc := range [][2]int64{{chunkMeta.ColumnIndexOffset, int64(chunkMeta.ColumnIndexLength)}, {chunkMeta.OffsetIndexOffset, int64(chunkMeta.OffsetIndexLength)}} {
		if loc[0] < 0 || loc[1] < 0 || loc[0] > p.size-loc[1] {
			return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "a key page index lies outside the file")
		}
	}
	if err := p.checkCodec(physical, p.key); err != nil {
		return nil, err
	}
	start, end, ok := chunkRange(&chunkMeta.MetaData)
	if !ok || end > p.size {
		return nil, refusePart(CodeInvalidFooter, p.name, physical, "the key column chunk lies outside the file")
	}
	chunk, err := p.chunk(physical, p.key)
	if err != nil {
		return nil, err
	}
	columnIndex, err := p.readColumnIndex(ctx, chunkMeta)
	if err != nil {
		return nil, p.indexError(ctx, physical, "ColumnIndex", err)
	}
	offsets, err := chunk.OffsetIndexFrom(p.at(ctx))
	if err != nil {
		return nil, p.indexError(ctx, physical, "OffsetIndex", err)
	}
	index, err := checkKeyIndex(columnIndex, offsets, p.footer.Metadata().RowGroups[physical].NumRows, start, end, p.utf8Key)
	if err != nil {
		return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "%v", err)
	}
	if !p.index[physical].CompareAndSwap(nil, index) {
		return p.index[physical].Load(), nil
	}
	return index, nil
}

// readColumnIndex reads and decodes the key chunk's ColumnIndex as
// written, so that its parallel lists can be checked (RG-4); its location
// has been checked to lie inside the file.
func (p *part) readColumnIndex(ctx context.Context, chunk *format.ColumnChunk) (*format.ColumnIndex, error) {
	raw := make([]byte, chunk.ColumnIndexLength)
	if n, err := p.at(ctx).ReadAt(raw, chunk.ColumnIndexOffset); n < len(raw) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	var index format.ColumnIndex
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), raw, &index); err != nil {
		return nil, err
	}
	return &index, nil
}

// indexError classifies an error loading a key page index.
func (p *part) indexError(ctx context.Context, physical int, which string, err error) error {
	switch {
	case isEnvironment(ctx, err):
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
func checkKeyIndex(columns *format.ColumnIndex, offsets parquet.OffsetIndex, rows, start, end int64, utf8Key bool) (*groupIndex, error) {
	n := offsets.NumPages()
	if n == 0 {
		return nil, errors.New("the key OffsetIndex is empty")
	}
	if len(columns.NullPages) != n || len(columns.MinValues) != n || len(columns.MaxValues) != n {
		return nil, errors.New("the key ColumnIndex lists and the OffsetIndex disagree on the page count")
	}
	index := &groupIndex{pages: make([]keyPage, n)}
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
	}
	return index, nil
}

// pageSpan is one key page a request decoded, at its dataset position.
type pageSpan struct {
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
	if i > 0 && bytes.Compare(s.first, l.spans[i-1].last) <= 0 {
		return keyOrder(s.part, s.physical, s.firstRow, s.first)
	}
	if i < len(l.spans) && bytes.Compare(l.spans[i].first, s.last) <= 0 {
		t := l.spans[i]
		return keyOrder(t.part, t.physical, t.firstRow, t.first)
	}
	s.first, s.last = bytes.Clone(s.first), bytes.Clone(s.last)
	l.spans = append(l.spans, pageSpan{})
	copy(l.spans[i+1:], l.spans[i:])
	l.spans[i] = s
	return nil
}
