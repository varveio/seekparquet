// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// supportedCodec reports whether parquet-go decodes codec.
func supportedCodec(codec format.CompressionCodec) bool {
	switch codec {
	case format.Uncompressed, format.Snappy, format.Gzip, format.Brotli, format.Zstd, format.Lz4Raw:
		return true
	}
	return false
}

const maxInt64 = int64(^uint64(0) >> 1)

// chunkRange returns the byte range of a column chunk: from its dictionary
// page (when it has one) or first data page, for its compressed size.
func chunkRange(meta *format.ColumnMetaData) (start, end int64, ok bool) {
	start = meta.DataPageOffset
	if meta.DictionaryPageOffset != 0 {
		if meta.DictionaryPageOffset > start {
			return 0, 0, false
		}
		start = meta.DictionaryPageOffset
	}
	if start < 0 || meta.TotalCompressedSize < 0 || start > maxInt64-meta.TotalCompressedSize {
		return 0, 0, false
	}
	return start, start + meta.TotalCompressedSize, true
}

// openChunk checks a column chunk before any of its pages is read: a codec
// this reader decodes (else unsupported_encoding), a byte range inside the
// file (else invalid_footer), and the header of its dictionary page, when
// it has one, against the limits. It returns the chunk's byte range.
func (p *part) openChunk(ctx context.Context, physical, column int) (start, end int64, err error) {
	meta := &p.footer.Metadata().RowGroups[physical].Columns[column].MetaData
	if !supportedCodec(meta.Codec) {
		return 0, 0, &UnsupportedError{Code: UnsupportedEncoding, Detail: "codec " + meta.Codec.String() + " in part " + p.name}
	}
	start, end, ok := chunkRange(meta)
	if !ok || end > p.size || meta.DataPageOffset < start || meta.DataPageOffset >= end {
		return 0, 0, refusePart(CodeInvalidFooter, p.name, physical, "column chunk %d lies outside the file", column)
	}
	if meta.DictionaryPageOffset != 0 {
		h, _, err := p.pageHeader(ctx, physical, column, meta.DictionaryPageOffset, end)
		if err != nil {
			return 0, 0, err
		}
		if h.Type != format.DictionaryPage {
			return 0, 0, refusePart(CodeInvalidParquet, p.name, physical, "column chunk %d: no dictionary page at its dictionary offset", column)
		}
	}
	return start, end, nil
}

type headerKey struct {
	physical, column int
	offset           int64
}

type checkedHeader struct {
	header *format.PageHeader
	length int64
}

// pageHeader reads, decodes and checks against the limits the page header
// at off in a column chunk ending at end. Its decoding is bounded like a
// footer's; its sizes must be at most Limits.PageBytes and its value count
// at most Limits.PageValues (else ErrLimitExceeded). A header that does not
// decode is invalid_parquet. Checked headers are remembered, so a page read
// again is not checked again.
func (p *part) pageHeader(ctx context.Context, physical, column int, off, end int64) (*format.PageHeader, int64, error) {
	key := headerKey{physical, column, off}
	if h, ok := p.checked.Load(key); ok {
		return h.(checkedHeader).header, h.(checkedHeader).length, nil
	}
	lim := &p.r.limits
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidParquet, p.name, physical, "column chunk %d page at %d: "+format, append([]any{column, off}, args...)...)
	}
	if off < 0 || off >= end {
		return nil, 0, bad("outside the column chunk")
	}
	avail := end - off
	var header format.PageHeader
	var length int
	// A header is read in growing pieces up to 16 MiB, far beyond any
	// real header, without reading the page it precedes.
	ceiling := min(avail, 16<<20, lim.MetadataMemory)
	for want := int64(4 << 10); ; want *= 16 {
		n := min(want, ceiling)
		raw := make([]byte, n)
		if err := readFull(p.at(ctx), raw, off); err != nil {
			if isEnvironment(ctx, err) {
				return nil, 0, err
			}
			return nil, 0, bad("%v", err)
		}
		header = format.PageHeader{}
		var err error
		length, err = decodeBounded(raw, &header, lim.MetadataMemory)
		if errors.Is(err, ErrLimitExceeded) {
			return nil, 0, fmt.Errorf("part %s: page header: %w", p.name, err)
		}
		if err == nil {
			break
		}
		if n == ceiling {
			return nil, 0, bad("the page header does not decode: %v", err)
		}
	}
	if header.CompressedPageSize < 0 || header.UncompressedPageSize < 0 || int64(length)+int64(header.CompressedPageSize) > avail {
		return nil, 0, bad("the page sizes are invalid")
	}
	if size := int64(max(header.CompressedPageSize, header.UncompressedPageSize)); size > lim.PageBytes {
		return nil, 0, exceeded("part %s: a page of %d bytes (Limits.PageBytes %d)", p.name, size, lim.PageBytes)
	}
	var values int64
	switch header.Type {
	case format.DataPage:
		h, _ := header.DataPageHeader.Get()
		values = int64(h.NumValues)
	case format.DataPageV2:
		h, _ := header.DataPageHeaderV2.Get()
		values = int64(max(h.NumValues, h.NumRows))
		if h.NumValues < 0 || h.NumRows < 0 || h.NumNulls < 0 || h.DefinitionLevelsByteLength < 0 || h.RepetitionLevelsByteLength < 0 ||
			int64(h.DefinitionLevelsByteLength)+int64(h.RepetitionLevelsByteLength) > int64(header.CompressedPageSize) {
			return nil, 0, bad("the data page header is invalid")
		}
	case format.DictionaryPage:
		h, _ := header.DictionaryPageHeader.Get()
		values = int64(h.NumValues)
	}
	if values < 0 {
		return nil, 0, bad("the value count is negative")
	}
	if values > lim.PageValues {
		return nil, 0, exceeded("part %s: a page of %d values (Limits.PageValues %d)", p.name, values, lim.PageValues)
	}
	p.checked.Store(key, checkedHeader{&header, int64(length)})
	return &header, int64(length), nil
}

// loadOffsetIndex returns the OffsetIndex of a column chunk, nil when it
// has none. Its encoded size is bounded by Limits.PageIndexBytes and its
// decoding like a footer's, before parquet-go decodes it.
func (p *part) loadOffsetIndex(ctx context.Context, physical, column int) (*parquet.FileOffsetIndex, error) {
	meta := &p.footer.Metadata().RowGroups[physical].Columns[column]
	if meta.OffsetIndexOffset == 0 {
		return nil, nil
	}
	raw, err := p.readIndexBytes(ctx, physical, meta.OffsetIndexOffset, meta.OffsetIndexLength)
	if err != nil {
		return nil, err
	}
	var index format.OffsetIndex
	if _, err := decodeBounded(raw, &index, p.r.limits.MetadataMemory); err != nil {
		return nil, err
	}
	chunk, err := p.chunk(physical, column)
	if err != nil {
		return nil, err
	}
	return chunk.OffsetIndexFrom(prefetched{p.at(ctx), meta.OffsetIndexOffset, raw})
}

// readIndexBytes reads a page index of length bytes at off, after checking
// that it lies in the file and within Limits.PageIndexBytes.
func (p *part) readIndexBytes(ctx context.Context, physical int, off int64, length int32) ([]byte, error) {
	if off < 0 || length <= 0 || off > p.size-int64(length) {
		return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "a page index lies outside the file")
	}
	if int64(length) > p.r.limits.PageIndexBytes {
		return nil, exceeded("part %s: a page index of %d bytes (Limits.PageIndexBytes %d)", p.name, length, p.r.limits.PageIndexBytes)
	}
	raw := make([]byte, length)
	if err := readFull(p.at(ctx), raw, off); err != nil {
		return nil, err
	}
	return raw, nil
}

// guardPages checks the headers of the data pages of a value column chunk
// that a read of n rows from row will decode, before parquet-go reads
// them: through the chunk's OffsetIndex when it has one, otherwise by
// walking the pages from the chunk's start, as parquet-go will. It returns
// the OffsetIndex, nil when the chunk has none.
func (p *part) guardPages(ctx context.Context, physical, column int, row int64, n int) (*parquet.FileOffsetIndex, error) {
	start, end, err := p.openChunk(ctx, physical, column)
	if err != nil {
		return nil, err
	}
	index, err := p.loadOffsetIndex(ctx, physical, column)
	if err != nil {
		if isEnvironment(ctx, err) {
			return nil, err
		}
		return nil, p.dataError(ctx, physical, fmt.Errorf("the OffsetIndex of column chunk %d: %w", column, err))
	}
	last := row + int64(n)
	if index != nil {
		for i := range index.NumPages() {
			first := index.FirstRowIndex(i)
			if i+1 < index.NumPages() && index.FirstRowIndex(i+1) <= row || first >= last {
				continue
			}
			if _, _, err := p.pageHeader(ctx, physical, column, index.Offset(i), end); err != nil {
				return nil, err
			}
		}
		return index, nil
	}
	var rows int64
	for off := start; rows < last && off < end; {
		h, length, err := p.pageHeader(ctx, physical, column, off, end)
		if err != nil {
			return nil, err
		}
		switch h.Type {
		case format.DataPage:
			v, _ := h.DataPageHeader.Get()
			rows += int64(v.NumValues)
		case format.DataPageV2:
			v, _ := h.DataPageHeaderV2.Get()
			rows += int64(v.NumRows)
		}
		off += length + int64(h.CompressedPageSize)
	}
	return nil, nil
}

// readPage seeks pages to row and decodes the page holding it, from row to
// the page's end, into values (which then alias the page's buffers). A
// panic inside parquet-go while it decodes the page is reported as an
// error, which the caller classifies as corrupt data: a last line of
// defence, not a substitute for the checks made before.
func readPage(pages *parquet.FilePages, row int64, values []parquet.Value) (_ parquet.Page, _ []parquet.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parquet-go failed on the page at row %d: %v", row, r)
		}
	}()
	if err := pages.SeekToRow(row); err != nil {
		return nil, nil, fmt.Errorf("seek to row %d: %w", row, err)
	}
	page, err := pages.ReadPage()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, fmt.Errorf("read page at row %d: %w", row, err)
	}
	values, err = pageValues(page, values)
	if err != nil {
		parquet.Release(page)
		return nil, nil, err
	}
	if len(values) == 0 {
		parquet.Release(page)
		return nil, nil, fmt.Errorf("page at row %d holds no values", row)
	}
	return page, values, nil
}

// pageValues decodes every value of a page of a non-repeated column.
func pageValues(page parquet.Page, values []parquet.Value) ([]parquet.Value, error) {
	count := int(page.NumValues())
	if page.NumRows() != int64(count) || count < 0 {
		return nil, errors.New("page values do not match its rows")
	}
	values = slices.Grow(values[:0], count)[:count]
	reader := page.Values()
	for read := 0; read < count; {
		n, err := reader.ReadValues(values[read:])
		read += n
		if read == count {
			break
		}
		if errors.Is(err, io.EOF) || err == nil && n == 0 {
			return nil, errors.New("page holds fewer values than it declares")
		}
		if err != nil {
			return nil, fmt.Errorf("read page values: %w", err)
		}
	}
	return values, nil
}

// dataError classifies an error decoding a page: the environment's own
// error, or an *UnsupportedError or *RefusalError, as is; anything else
// corrupt Parquet data (CHECK-5).
func (p *part) dataError(ctx context.Context, physical int, err error) error {
	var unsupported *UnsupportedError
	var refusal *RefusalError
	switch {
	case ctx.Err() != nil, errors.As(err, &unsupported), errors.As(err, &refusal):
		return err
	case isEnvironment(ctx, err):
		return fmt.Errorf("seekparquet: part %s row group %d: %w", p.name, physical, err)
	}
	return refusePart(CodeInvalidParquet, p.name, physical, "%v", err)
}

// readValues reads n rows of the named columns of a physical row group
// from row on, cloned out of the page buffers; only those columns' pages
// are decoded. A column the part lacks is nil (absent).
func (p *part) readValues(ctx context.Context, physical int, row int64, n int, names []string, stats *Stats) ([][]parquet.Value, error) {
	out := make([][]parquet.Value, len(names))
	for i, name := range names {
		column, ok := p.leaves[name]
		if !ok {
			continue
		}
		values := make([]parquet.Value, 0, n)
		err := p.readColumnInto(ctx, physical, column, row, n, stats, func(page []parquet.Value) error {
			for _, value := range page {
				values = append(values, value.Clone())
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		out[i] = values
	}
	return out, nil
}

// errSink marks an error a ColumnSink returned, so that it is returned to
// the caller unwrapped.
type errSink struct{ err error }

func (e errSink) Error() string { return e.err.Error() }

// readColumnInto hands n rows of a value column of a physical row group
// from row on to emit, a page at a time; the values alias the page's
// buffers and are valid only until emit returns. Without an OffsetIndex
// the column is decoded from its chunk's start, which is correct, only
// slower (COL-4).
func (p *part) readColumnInto(ctx context.Context, physical, column int, row int64, n int, stats *Stats, emit func([]parquet.Value) error) error {
	if _, err := p.guardPages(ctx, physical, column, row, n); err != nil {
		return err
	}
	chunk, err := p.chunk(physical, column)
	if err != nil {
		return err
	}
	pages := chunk.PagesFrom(p.at(ctx))
	defer func() { _ = pages.Close() }()
	var scratch []parquet.Value
	for next, done := row, 0; done < n; {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, values, err := readPage(pages, next, scratch)
		if err != nil {
			return p.dataError(ctx, physical, fmt.Errorf("column chunk %d: %w", column, err))
		}
		stats.PagesDecoded++
		take := min(len(values), n-done)
		err = emit(values[:take])
		parquet.Release(page)
		if err != nil {
			return errSink{err}
		}
		scratch = values[:0]
		next += int64(take)
		done += take
	}
	return nil
}
