// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// Stats counts the decoding work of reads: pages and keys decoded and the
// routing decisions taken. A read adds to the Stats it is given; the caller
// owns it and its synchronization. It does not count opening work, page
// index loads, or bytes read from the source, so it is not a measure of
// total I/O.
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
	part     *part
	physical int
	index    *groupIndex
	pages    *parquet.FilePages
	page     parquet.Page
	values   []parquet.Value
	at       int // index of the decoded page, -1 before the first
	next     int // position within values
	// g is the row group's global index, its position in the dataset.
	g int
	// log holds the pages the request decoded, shared by its cursors.
	log   *pageLog
	stats *Stats
}

func (p *part) keyCursor(ctx context.Context, g, physical int, index *groupIndex, stats *Stats, log *pageLog) *keyCursor {
	if stats == nil {
		stats = new(Stats)
	}
	return &keyCursor{ctx: ctx, part: p, g: g, physical: physical, index: index, at: -1, stats: stats, log: log}
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
// (KEY-6), strictly ascending, inside the page's ColumnIndex bounds (RG-5),
// and in dataset order with every other page the request decoded (KEY-3).
func (c *keyCursor) decode(i int) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	p := c.part
	meta := c.index.pages[i]
	if err := p.checkPageHeader(c.ctx, c.physical, i, meta); err != nil {
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
		if j > 0 && bytes.Compare(key, previous) <= 0 {
			return keyOrder(p.name, c.physical, row, key)
		}
		previous = key
	}
	first, last := values[0].ByteArray(), values[len(values)-1].ByteArray()
	if bytes.Compare(first, meta.lo) < 0 || bytes.Compare(last, meta.hi) > 0 {
		return refusePart(CodeInvalidPageIndex, p.name, c.physical, "key page %d holds a key outside its ColumnIndex bounds", i)
	}
	return c.log.add(pageSpan{g: c.g, page: i, part: p.name, physical: c.physical, firstRow: meta.first, first: first, last: last})
}

// checkPageHeader reads the page header an OffsetIndex entry locates and
// requires it to begin exactly one data page that fills the entry and
// holds the entry's rows (RG-3).
func (p *part) checkPageHeader(ctx context.Context, physical, i int, meta keyPage) error {
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidPageIndex, p.name, physical, "key page %d: "+format, append([]any{i}, args...)...)
	}
	var header format.PageHeader
	var headerLen int64
	for _, want := range []int64{4 << 10, 1 << 20} {
		n := min(meta.size, want)
		buf := make([]byte, n)
		if got, err := p.at(ctx).ReadAt(buf, meta.offset); int64(got) < n {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			if isEnvironment(ctx, err) {
				return err
			}
			return bad("read its header: %v", err)
		}
		r := new(thrift.CompactProtocol).NewReaderFromBytes(buf)
		header = format.PageHeader{}
		if err := thrift.NewDecoder(r).Decode(&header); err != nil {
			if n < meta.size && n == want {
				continue // the header may be longer than the bytes read
			}
			return bad("its OffsetIndex entry does not locate a page header")
		}
		headerLen = int64(r.BytesRead())
		break
	}
	if headerLen == 0 {
		return bad("its OffsetIndex entry does not locate a page header")
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
	if header.CompressedPageSize < 0 || headerLen+int64(header.CompressedPageSize) != meta.size {
		return bad("its OffsetIndex entry does not locate exactly one page")
	}
	if rows != meta.rows {
		return bad("the page holds %d rows, its OffsetIndex entry %d", rows, meta.rows)
	}
	return nil
}

// dataError classifies an error decoding a page: the environment's own
// error as is, anything else corrupt Parquet data (CHECK-5).
func (p *part) dataError(ctx context.Context, physical int, err error) error {
	if isEnvironment(ctx, err) {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("seekparquet: part %s row group %d: %w", p.name, physical, err)
	}
	var unsupported *UnsupportedError
	if errors.As(err, &unsupported) {
		return err
	}
	return refusePart(CodeInvalidParquet, p.name, physical, "%v", err)
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

// readPage seeks pages to row and decodes the page holding it, from row to
// the page's end, into values (which then alias the page's buffers).
func readPage(pages *parquet.FilePages, row int64, values []parquet.Value) (parquet.Page, []parquet.Value, error) {
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
// buffers and are valid only until emit returns.
//
// Errors in the data are invalid_parquet refusals (CHECK-5); a column
// chunk whose codec this reader does not decode is unsupported.
func (p *part) readColumnInto(ctx context.Context, physical, column int, row int64, n int, stats *Stats, emit func([]parquet.Value) error) error {
	if err := p.checkCodec(physical, column); err != nil {
		return err
	}
	chunk, err := p.chunk(physical, column)
	if err != nil {
		return err
	}
	// Without an OffsetIndex SeekToRow decodes from the chunk's start,
	// which is correct, only slower (COL-4).
	if _, err := chunk.OffsetIndexFrom(p.at(ctx)); err != nil && !errors.Is(err, parquet.ErrMissingOffsetIndex) {
		return p.dataError(ctx, physical, fmt.Errorf("read the OffsetIndex of column %d: %w", column, err))
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
			return p.dataError(ctx, physical, fmt.Errorf("column %d: %w", column, err))
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
