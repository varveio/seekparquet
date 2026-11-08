// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/bits"
	"strings"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/rle"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/manifest"
)

// This file holds the validation levels. They reuse the reader's checked
// footer, page index and page decoding, but not its seek path: every column
// chunk is walked page header by page header from its start, and the
// indexes are compared with the pages found, so that an index cannot hide a
// page from the validator (§8).

// validator holds one validation's dataset, opened by the structural level
// and read by the levels after it.
type validator struct {
	ctx    context.Context
	src    Source
	limits Limits
	d      *Dataset
	// st and pages take the reader's read and decoding counters, which
	// validation does not report.
	st    ReadStats
	pages int64
}

func (v *validator) close() {
	if v.d != nil {
		_ = v.d.Close()
	}
}

// structural opens the dataset (CHECK-1, which includes COL-5 for every
// column) and checks RG-2 to RG-4 for every non-empty row group: the key
// column's indexes, and the page header at every OffsetIndex entry, which
// must begin one data page of exactly the entry's size and rows (RG-3).
// Page data is not read.
func (v *validator) structural(lr *LevelReport) error {
	d, err := OpenDataset(v.ctx, v.src, Options{Limits: v.limits})
	if err != nil {
		return err
	}
	v.d = d
	for _, p := range d.r.parts {
		lr.At = Location{Part: p.name, RowGroup: -1, Page: -1}
		for physical := range p.footer.Metadata().RowGroups {
			if p.footer.Metadata().RowGroups[physical].NumRows == 0 {
				continue
			}
			lr.At = Location{Part: p.name, RowGroup: physical, Column: v.d.r.key.Column, Page: -1}
			index, end, err := v.keyIndex(p, physical, false)
			if err != nil {
				return err
			}
			for i, page := range index.pages {
				lr.At.Page = i
				if err := v.ctx.Err(); err != nil {
					return err
				}
				if err := checkIndexedHeader(v.ctx, p, physical, i, page.pageLoc, end); err != nil {
					return err
				}
				lr.Checked.Pages++
			}
			lr.Checked.RowGroups++
		}
		lr.Checked.Parts++
	}
	return nil
}

// keyIndex loads and checks the key page index of a non-empty row group
// (RG-2 to RG-4, as part.keyIndex does, without retaining it) and returns
// it with the end of the key column chunk. With utf8Bounds, a STRING key's
// bounds must also be valid UTF-8 (KEY-6, a profile check).
func (v *validator) keyIndex(p *part, physical int, utf8Bounds bool) (*groupIndex, int64, error) {
	meta := &p.footer.Metadata().RowGroups[physical].Columns[p.key.index]
	if meta.ColumnIndexOffset == 0 || meta.ColumnIndexLength == 0 || meta.OffsetIndexOffset == 0 || meta.OffsetIndexLength == 0 {
		return nil, 0, refusePart(CodeMissingPageIndex, p.name, physical, "the key column lacks a ColumnIndex or an OffsetIndex")
	}
	start, end, ok := chunkRange(&meta.MetaData)
	if !ok || end > p.size {
		return nil, 0, refusePart(CodeInvalidFooter, p.name, physical, "column chunk %d lies outside the file", p.key.index)
	}
	raw, err := p.readIndexBytes(v.ctx, physical, meta.ColumnIndexOffset, meta.ColumnIndexLength, &v.st)
	if err != nil {
		return nil, 0, p.indexError(v.ctx, physical, "ColumnIndex", err)
	}
	var columns format.ColumnIndex
	if _, err := decodeBounded(raw, &columns, v.limits.MetadataMemory); err != nil {
		return nil, 0, p.indexError(v.ctx, physical, "ColumnIndex", err)
	}
	offsets, err := p.loadOffsetIndex(v.ctx, physical, p.key.index, &v.st)
	if err != nil {
		return nil, 0, p.indexError(v.ctx, physical, "OffsetIndex", err)
	}
	index, err := checkKeyIndex(&columns, offsets, p.footer.Metadata().RowGroups[physical].NumRows, start, end, utf8Bounds && p.utf8Key)
	if err != nil {
		return nil, 0, refusePart(CodeInvalidPageIndex, p.name, physical, "the key page index: %v", err)
	}
	return index, end, nil
}

// checkIndexedHeader requires the page header that key OffsetIndex entry i
// locates to begin one data page that fills the entry and holds its rows
// (RG-3), refusing with invalid_page_index otherwise.
func checkIndexedHeader(ctx context.Context, p *part, physical, i int, loc pageLoc, end int64) error {
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidPageIndex, p.name, physical, "key page %d: "+format, append([]any{i}, args...)...)
	}
	h, n, _, err := p.pageHeader(ctx, physical, p.key.index, loc.offset, end, nil)
	if err != nil {
		if CodeOf(err) == CodeInvalidParquet {
			return bad("its OffsetIndex entry does not locate a page header")
		}
		return err
	}
	rows, ok := dataRows(h)
	switch {
	case !ok:
		return bad("its OffsetIndex entry locates a page of type %s, not a data page", h.Type)
	case n+int64(h.CompressedPageSize) != loc.size:
		return bad("its OffsetIndex entry does not locate exactly one page")
	case rows != loc.rows:
		return bad("the page holds %d rows, its OffsetIndex entry %d", rows, loc.rows)
	}
	return nil
}

// walk enumerates the pages of chunk c from its start without consulting
// any index, and calls visit for each data page with its number, its
// location (first row, the rows its header declares, offset and size,
// header included), its header and the bytes read with it; visit returns
// the rows the page holds. A dictionary page may only begin the chunk, and
// it is checked and decoded when found, whether or not a data page uses
// it. Other page types are stepped over. The pages must fill the chunk and
// hold exactly rows rows (CHECK-5).
func (v *validator) walk(c *chunk, rows int64, visit func(i int, loc pageLoc, h *format.PageHeader, n int64, raw []byte) (int64, error)) error {
	first, i := int64(0), 0
	for off := c.start; off < c.end; {
		if err := v.ctx.Err(); err != nil {
			return err
		}
		h, n, raw, err := c.header(off)
		if err != nil {
			return err
		}
		size := n + int64(h.CompressedPageSize)
		switch h.Type {
		case format.DictionaryPage:
			if off != c.start {
				return c.corrupt(fmt.Errorf("a dictionary page at %d, after the chunk's first page", off))
			}
			if _, err := c.dictionary(); err != nil { // its checksum, size and values
				return err
			}
		case format.DataPage, format.DataPageV2:
			declared, ok := dataRows(h)
			if !ok {
				return c.corrupt(fmt.Errorf("the data page at %d has no data page header", off))
			}
			pageRows, err := visit(i, pageLoc{first: first, rows: declared, offset: off, size: size}, h, n, raw)
			if err != nil {
				return err
			}
			if pageRows > rows-first {
				return c.corrupt(fmt.Errorf("the chunk holds more than its row group's %d rows", rows))
			}
			first += pageRows
			i++
		}
		off += size
	}
	if first != rows {
		return c.corrupt(fmt.Errorf("the chunk holds %d of its row group's %d rows", first, rows))
	}
	return nil
}

// page decodes the data page that walk found at loc, after its checksum
// when it has one, and requires it to hold one value per row.
func (c *chunk) page(loc pageLoc, h *format.PageHeader, n int64, raw []byte) ([]parquet.Value, error) {
	data, err := c.payload(h, loc.offset, n, raw)
	if err != nil {
		return nil, err
	}
	values, err := c.decode(h, data)
	if err != nil {
		return nil, err
	}
	if int64(len(values)) != loc.rows {
		return nil, c.corrupt(fmt.Errorf("a page declares %d rows and holds %d values", loc.rows, len(values)))
	}
	return values, nil
}

// profile walks every key column chunk from its start and requires its
// OffsetIndex to list exactly the data pages found (RG-3); decodes every
// key page and checks every key: non-empty and, for a STRING key, valid
// UTF-8 (KEY-2, KEY-6), in the declared order across the whole dataset
// (KEY-3, strictly ascending when keys are unique, and a group never
// across parts, PART-4), inside its page's ColumnIndex bounds (RG-5); and
// requires each part's first and last keys to equal its manifest bounds
// (PART-3).
func (v *validator) profile(lr *LevelReport) error {
	r := v.d.r
	var previous []byte
	previousPart := -1
	for pi, p := range r.parts {
		lr.At = Location{Part: p.name, RowGroup: -1, Page: -1}
		var first, last []byte
		for physical := range p.footer.Metadata().RowGroups {
			rows := p.footer.Metadata().RowGroups[physical].NumRows
			if rows == 0 {
				continue
			}
			lr.At = Location{Part: p.name, RowGroup: physical, Column: r.key.Column, Page: -1}
			index, _, err := v.keyIndex(p, physical, true)
			if err != nil {
				return err
			}
			c, err := p.openChunk(v.ctx, physical, p.key, &v.st, &v.pages)
			if err != nil {
				return err
			}
			bad := func(code Code, row int64, key []byte, format string, args ...any) error {
				e := refusePart(code, p.name, physical, format, args...)
				e.Row, e.Key = row, bytes.Clone(key)
				return e
			}
			found := 0
			err = v.walk(c, rows, func(i int, loc pageLoc, h *format.PageHeader, n int64, raw []byte) (int64, error) {
				lr.At.Page = i
				if i >= len(index.pages) || index.pages[i].pageLoc != loc {
					return 0, bad(CodeInvalidPageIndex, -1, nil, "key page %d at offset %d (%d bytes, rows %d to %d) is not the page its OffsetIndex lists there",
						i, loc.offset, loc.size, loc.first, loc.first+loc.rows)
				}
				values, err := c.page(loc, h, n, raw)
				if err != nil {
					return 0, err
				}
				for j, value := range values {
					key, row := value.ByteArray(), loc.first+int64(j)
					if value.IsNull() || len(key) == 0 || p.utf8Key && !utf8.Valid(key) {
						return 0, bad(CodeInvalidKey, row, key, "an empty key, or a STRING key that is not valid UTF-8")
					}
					if previous != nil {
						cmp := bytes.Compare(previous, key)
						if cmp > 0 || cmp == 0 && (r.key.Unique || previousPart != pi) {
							return 0, bad(CodeKeyOrder, row, key, "the key breaks the declared key order with the key before it")
						}
					}
					if first == nil {
						first = bytes.Clone(key)
						if !bytes.Equal(first, p.minKey) {
							return 0, bad(CodeBoundsMismatch, row, key, "the part's first key differs from its manifest min_key")
						}
					}
					previous, previousPart = key, pi
				}
				lo, hi := values[0].ByteArray(), values[len(values)-1].ByteArray()
				if bytes.Compare(lo, index.pages[i].lo) < 0 || bytes.Compare(hi, index.pages[i].hi) > 0 {
					return 0, bad(CodeInvalidPageIndex, -1, nil, "key page %d holds a key outside its ColumnIndex bounds", i)
				}
				previous = bytes.Clone(hi)
				found++
				lr.Checked.Pages++
				lr.Checked.Rows += loc.rows
				return loc.rows, nil
			})
			if err != nil {
				return err
			}
			lr.At.Page = -1
			if found != len(index.pages) {
				return refusePart(CodeInvalidPageIndex, p.name, physical, "the OffsetIndex lists %d pages, the chunk holds %d", len(index.pages), found)
			}
			if declared := p.footer.Metadata().RowGroups[physical].Columns[p.key.index].MetaData.NumValues; declared != rows {
				return refusePart(CodeInvalidParquet, p.name, physical, "the key column chunk holds %d values, its metadata declares %d", rows, declared)
			}
			last = previous
			lr.Checked.Chunks++
			lr.Checked.RowGroups++
		}
		if !bytes.Equal(last, p.maxKey) {
			return refusePart(CodeBoundsMismatch, p.name, -1, "the part's last key differs from its manifest max_key")
		}
		lr.Checked.Parts++
	}
	return nil
}

// full walks and decodes every column chunk other than the key column's
// (decoded by profile) in every non-empty row group: every page passes its
// checksum, decompresses and decodes, its levels and values agree with its
// header, and the chunk holds exactly the row group's rows and the value
// count its metadata declares (CHECK-5). A column's OffsetIndex, when it
// has one, must list exactly the data pages found, and its ColumnIndex,
// when it has one, one entry per page; for a column that is neither nested
// nor repeated, a page it marks null must hold only nulls, and its null
// counts must be exact. Bounds of value columns, whose order depends on
// their type, are not compared with their values.
//
// A codec or encoding the reader does not decode leaves the level
// incomplete; the other columns are still decoded, so that a fault in them
// fails the level.
func (v *validator) full(lr *LevelReport) error {
	var unsupported error
	var unsupportedAt Location
	for _, p := range v.d.r.parts {
		meta := p.footer.Metadata()
		leaves, paths := map[int]parquet.LeafColumn{}, map[int]string{}
		for _, path := range p.footer.Schema().Columns() {
			if l, ok := p.footer.Schema().Lookup(path...); ok {
				leaves[l.ColumnIndex], paths[l.ColumnIndex] = l, strings.Join(path, ".")
			}
		}
		for physical := range meta.RowGroups {
			rows := meta.RowGroups[physical].NumRows
			if rows == 0 {
				continue
			}
			for column := range meta.RowGroups[physical].Columns {
				if column == p.key.index {
					continue
				}
				l, ok := leaves[column]
				if !ok {
					return refusePart(CodeInvalidFooter, p.name, physical, "column chunk %d has no leaf column", column)
				}
				lr.At = Location{Part: p.name, RowGroup: physical, Column: paths[column], Page: -1}
				err := v.column(lr, p, physical, l, rows)
				var u *UnsupportedError
				switch {
				case err == nil:
					lr.Checked.Chunks++
				case errors.As(err, &u):
					if unsupported == nil {
						unsupported, unsupportedAt = err, lr.At
					}
				default:
					return err
				}
			}
			lr.Checked.RowGroups++
			lr.Checked.Rows += rows
		}
		lr.Checked.Parts++
	}
	if unsupported != nil {
		lr.At = unsupportedAt
		return unsupported
	}
	return nil
}

// column walks and decodes one value column chunk, and compares its page
// indexes, when present, with the pages found.
func (v *validator) column(lr *LevelReport, p *part, physical int, l parquet.LeafColumn, rows int64) error {
	chunkMeta := &p.footer.Metadata().RowGroups[physical].Columns[l.ColumnIndex]
	col := leaf{index: l.ColumnIndex, typ: l.Node.Type(), optional: l.MaxDefinitionLevel > 0}
	flat := l.MaxRepetitionLevel == 0 && l.MaxDefinitionLevel <= 1 // the reader decodes it
	c, err := p.openChunk(v.ctx, physical, col, &v.st, &v.pages)
	if err != nil {
		return err
	}
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidParquet, p.name, physical, "column chunk %d: "+format, append([]any{col.index}, args...)...)
	}
	var locs []pageLoc
	if chunkMeta.OffsetIndexOffset != 0 {
		offsets, err := p.loadOffsetIndex(v.ctx, physical, col.index, &v.st)
		if err != nil {
			return valueIndexError(v.ctx, p, physical, col.index, "OffsetIndex", err)
		}
		if locs, err = checkLocations(offsets, rows, c.start, c.end); err != nil {
			return bad("the OffsetIndex: %v", err)
		}
	}
	var columns *format.ColumnIndex
	if chunkMeta.ColumnIndexOffset != 0 {
		raw, err := p.readIndexBytes(v.ctx, physical, chunkMeta.ColumnIndexOffset, chunkMeta.ColumnIndexLength, &v.st)
		if err != nil {
			return valueIndexError(v.ctx, p, physical, col.index, "ColumnIndex", err)
		}
		columns = new(format.ColumnIndex)
		if _, err := decodeBounded(raw, columns, v.limits.MetadataMemory); err != nil {
			return valueIndexError(v.ctx, p, physical, col.index, "ColumnIndex", err)
		}
	}
	pages, values := 0, int64(0)
	err = v.walk(c, rows, func(i int, loc pageLoc, h *format.PageHeader, n int64, raw []byte) (int64, error) {
		lr.At.Page = i
		var entries int64
		if flat {
			if h.Type == format.DataPage && col.optional {
				if err := c.levelEncoding(h.DataPageHeader.V.DefinitionLevelEncoding); err != nil {
					return 0, err
				}
			}
			decoded, err := c.page(loc, h, n, raw)
			if err != nil {
				return 0, err
			}
			if h.Type == format.DataPageV2 {
				if nulls := countNulls(decoded); nulls != int64(h.DataPageHeaderV2.V.NumNulls) {
					return 0, bad("page %d holds %d nulls, its header declares %d", i, nulls, h.DataPageHeaderV2.V.NumNulls)
				}
			}
			if columns != nil {
				if err := checkValueIndexPage(columns, i, decoded); err != nil {
					return 0, bad("page %d: the ColumnIndex: %v", i, err)
				}
			}
			entries = loc.rows
		} else {
			data, err := c.payload(h, loc.offset, n, raw)
			if err != nil {
				return 0, err
			}
			if entries, loc.rows, err = c.levelled(h, data, l.MaxRepetitionLevel, l.MaxDefinitionLevel, i == 0); err != nil {
				return 0, err
			}
		}
		if locs != nil && (i >= len(locs) || locs[i] != loc) {
			return 0, bad("page %d at offset %d (%d bytes, rows %d to %d) is not the page its OffsetIndex lists there",
				i, loc.offset, loc.size, loc.first, loc.first+loc.rows)
		}
		pages++
		values += entries
		lr.Checked.Pages++
		return loc.rows, nil
	})
	if err != nil {
		return err
	}
	lr.At.Page = -1
	if locs != nil && pages != len(locs) {
		return bad("the OffsetIndex lists %d pages, the chunk holds %d", len(locs), pages)
	}
	if columns != nil && (len(columns.NullPages) != pages || len(columns.MinValues) != pages || len(columns.MaxValues) != pages ||
		len(columns.NullCounts) != 0 && len(columns.NullCounts) != pages) {
		return bad("the ColumnIndex lists do not have one entry per page (%d pages)", pages)
	}
	if values != chunkMeta.MetaData.NumValues {
		return bad("the chunk holds %d values, its metadata declares %d", values, chunkMeta.MetaData.NumValues)
	}
	return nil
}

// levelled decodes a data page of a nested or repeated column, which the
// reader does not project: its repetition and definition levels, each at
// most its column's maximum, and its present values (those at the maximum
// definition level). The first page of a chunk, and every V2 page, must
// begin a row (repetition level 0), and a V2 page's null count must be
// its entries below the maximum definition level. It returns the page's
// level entries and its rows (the entries at repetition level 0).
func (c *chunk) levelled(h *format.PageHeader, payload []byte, maxRep, maxDef int, first bool) (entries, rows int64, err error) {
	defer func() {
		if r := recover(); r != nil { // a last line of defence, as in decode
			err = c.corrupt(fmt.Errorf("parquet-go failed on a page: %v", r))
		}
	}()
	var n int
	var enc format.Encoding
	var repLevels, defLevels, data []byte
	switch h.Type {
	case format.DataPage:
		d := &h.DataPageHeader.V
		n, enc = int(d.NumValues), d.Encoding
		if data, err = decompress(c.codec, payload, int(h.UncompressedPageSize)); err != nil {
			return 0, 0, c.corrupt(err)
		}
		take := func(encoding format.Encoding, max int) ([]byte, error) {
			if max == 0 {
				return nil, nil
			}
			if err := c.levelEncoding(encoding); err != nil {
				return nil, err
			}
			if len(data) < 4 || int64(binary.LittleEndian.Uint32(data)) > int64(len(data)-4) {
				return nil, c.corrupt(errors.New("the levels exceed the page"))
			}
			m := 4 + int(binary.LittleEndian.Uint32(data))
			out := data[4:m]
			data = data[m:]
			return out, nil
		}
		if repLevels, err = take(d.RepetitionLevelEncoding, maxRep); err != nil {
			return 0, 0, err
		}
		if defLevels, err = take(d.DefinitionLevelEncoding, maxDef); err != nil {
			return 0, 0, err
		}
	case format.DataPageV2:
		d := &h.DataPageHeaderV2.V
		n, enc = int(d.NumValues), d.Encoding
		rep, def := int(d.RepetitionLevelsByteLength), int(d.DefinitionLevelsByteLength)
		repLevels, defLevels, data = payload[:rep], payload[rep:rep+def], payload[rep+def:]
		codec := c.codec
		if d.IsCompressed.Valid && !d.IsCompressed.V {
			codec = format.Uncompressed
		}
		if data, err = decompress(codec, data, int(h.UncompressedPageSize)-rep-def); err != nil {
			return 0, 0, c.corrupt(err)
		}
	}
	reps, err := levels(repLevels, n, maxRep)
	if err != nil {
		return 0, 0, c.corrupt(fmt.Errorf("repetition levels: %w", err))
	}
	defs, err := levels(defLevels, n, maxDef)
	if err != nil {
		return 0, 0, c.corrupt(fmt.Errorf("definition levels: %w", err))
	}
	present, rows := n, int64(n)
	if maxDef > 0 {
		present = 0
		for _, level := range defs {
			if int(level) == maxDef {
				present++
			}
		}
	}
	if maxRep > 0 {
		rows = 0
		for _, level := range reps {
			if level == 0 {
				rows++
			}
		}
	}
	if maxRep > 0 && n > 0 && (first || h.Type == format.DataPageV2) && reps[0] != 0 {
		return 0, 0, c.corrupt(fmt.Errorf("the page begins at repetition level %d, not at a row", reps[0]))
	}
	if h.Type == format.DataPageV2 {
		d := &h.DataPageHeaderV2.V
		if rows != int64(d.NumRows) {
			return 0, 0, c.corrupt(fmt.Errorf("the page holds %d rows, its header declares %d", rows, d.NumRows))
		}
		if nulls := n - present; nulls != int(d.NumNulls) {
			return 0, 0, c.corrupt(fmt.Errorf("the page holds %d nulls, its header declares %d", nulls, d.NumNulls))
		}
	}
	if _, err := c.values(enc, data, present); err != nil {
		return 0, 0, err
	}
	return int64(n), rows, nil
}

// levelEncoding accepts RLE for levels and reports the legacy BIT_PACKED
// as unsupported; Parquet allows no other encoding for levels, so anything
// else is corrupt data.
func (c *chunk) levelEncoding(e format.Encoding) error {
	switch e {
	case format.RLE:
		return nil
	case format.BitPacked:
		return &UnsupportedError{Code: UnsupportedEncoding, Detail: "levels encoded BIT_PACKED in part " + c.p.name}
	}
	return c.corrupt(fmt.Errorf("levels encoded %s, which Parquet does not allow for levels", e))
}

// countNulls returns the number of null values.
func countNulls(values []parquet.Value) int64 {
	nulls := int64(0)
	for _, value := range values {
		if value.IsNull() {
			nulls++
		}
	}
	return nulls
}

// levels decodes n levels of at most max from an RLE/bit-packed hybrid
// stream, after checking that the stream declares no more than n.
func levels(src []byte, n, max int) ([]byte, error) {
	if max == 0 {
		return nil, nil
	}
	width := bits.Len(uint(max))
	if err := checkRuns(src, n, width); err != nil {
		return nil, err
	}
	out, err := (&rle.Encoding{BitWidth: width}).DecodeLevels(make([]byte, 0, n+8), src)
	if err != nil || len(out) < n {
		return nil, errors.New("the page holds fewer levels than values")
	}
	out = out[:n]
	for _, level := range out {
		if int(level) > max {
			return nil, fmt.Errorf("a level of %d is above the column's maximum %d", level, max)
		}
	}
	return out, nil
}

// checkValueIndexPage checks ColumnIndex entry i against the decoded values
// of page i of a column that is neither nested nor repeated: a page marked
// null holds only nulls, and a null count, when the index has them, is
// exact.
func checkValueIndexPage(columns *format.ColumnIndex, i int, values []parquet.Value) error {
	if i >= len(columns.NullPages) {
		return errors.New("it lists fewer pages than the chunk holds")
	}
	nulls := countNulls(values)
	if columns.NullPages[i] && nulls != int64(len(values)) {
		return errors.New("a page marked null holds values")
	}
	if len(columns.NullCounts) != 0 && (i >= len(columns.NullCounts) || columns.NullCounts[i] != nulls) {
		return fmt.Errorf("the page holds %d nulls, its entry does not say so", nulls)
	}
	return nil
}

// valueIndexError classifies an error loading a value column's page index:
// a fault is invalid_parquet, never a key column's invalid_page_index.
func valueIndexError(ctx context.Context, p *part, physical, column int, which string, err error) error {
	var refusal *RefusalError
	if isEnvironment(ctx, err) {
		return err
	}
	if errors.As(err, &refusal) {
		err = errors.New(refusal.Detail)
	}
	return refusePart(CodeInvalidParquet, p.name, physical, "column chunk %d: the %s: %v", column, which, err)
}

// digest reads the manifest and streams every part that declares a
// sha256 through SHA-256, comparing the digests (PART-5). It does not open
// the parts as Parquet. A manifest that cannot be read or parsed fails the
// level with its refusal, as opening would.
func (v *validator) digest(lr *LevelReport) error {
	raw, err := readManifest(v.ctx, v.src, v.limits.ManifestBytes)
	if err != nil {
		return err
	}
	m, err := manifest.Parse(raw, v.limits.Parts)
	if err != nil {
		var e *manifest.Error
		switch {
		case errors.As(err, &e):
			return refuse(Code(e.Code), "%s: %s", ManifestName, e.Detail)
		case errors.Is(err, manifest.ErrTooManyParts):
			return exceeded("%s lists more than %d parts (Limits.Parts)", ManifestName, v.limits.Parts)
		}
		return err
	}
	buf := make([]byte, 1<<20)
	for _, entry := range m.Parts {
		if entry.SHA256 == "" {
			continue
		}
		lr.At = Location{Part: entry.Path, RowGroup: -1, Page: -1}
		sum, err := v.hashPart(entry.Path, buf)
		if err != nil {
			return err
		}
		if sum != entry.SHA256 {
			return refusePart(CodeDigestMismatch, entry.Path, -1, "the part's SHA-256 is %s, its manifest entry %s", sum, entry.SHA256)
		}
		lr.Checked.Digests++
		lr.Checked.Parts++
	}
	if lr.Checked.Digests == 0 {
		lr.Reason = "no part declares a sha256: nothing checked (VAL-2)"
	}
	return nil
}

// hashPart returns the lowercase hexadecimal SHA-256 of the part at path.
func (v *validator) hashPart(path string, buf []byte) (string, error) {
	f, err := v.src.Open(v.ctx, path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", refusePart(CodeMissingPart, path, -1, "%v", err)
		}
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	at := storageReader{r: bind(v.ctx, f)}
	for off, size := int64(0), f.Size(); off < size; {
		if err := v.ctx.Err(); err != nil {
			return "", err
		}
		n := int(min(int64(len(buf)), size-off))
		if err := readFull(at, buf[:n], off); err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				return "", err
			}
			return "", fmt.Errorf("read part %s: the file is shorter than its size %d: %w", path, size, err)
		}
		h.Write(buf[:n])
		off += int64(n)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
