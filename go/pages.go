// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/encoding/rle"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// This file reads pages. The reader locates every page it decodes itself,
// through a checked page index or by walking the page headers from the
// chunk's start, and decodes it with parquet-go's encodings only after
// checking what the page declares: its header against the limits, its
// decompressed size against the header (decompress), and the value counts
// and decoded size its encodings declare against the header and the limits
// (checkRuns, deltaStream). Only top-level, non-repeated columns are read,
// so a page has no repetition levels and at most one definition level.

// leaf is a top-level, non-repeated leaf column of a part.
type leaf struct {
	index    int // leaf column index in the part's schema
	typ      parquet.Type
	optional bool // the column has definition levels: its values may be null
}

// pageLoc locates one data page of a column chunk, as its OffsetIndex
// gives it: the page's first row, its row count, and its offset and size,
// header included.
type pageLoc struct {
	first, rows  int64
	offset, size int64
}

// pageAt returns the page of locs holding row, which must be below the
// row group's row count.
func pageAt(locs []pageLoc, row int64) int {
	return sort.Search(len(locs), func(i int) bool { return locs[i].first > row }) - 1
}

// checkLocations builds a chunk's page locations from its OffsetIndex after
// checking them (RG-3 as far as the index alone allows): a non-empty list,
// first rows starting at zero and ascending strictly below rows, and every
// page inside the column chunk [start, end), in order, without overlap,
// computed without overflow. Each page header is checked as the page is
// read.
func checkLocations(index *format.OffsetIndex, rows, start, end int64) ([]pageLoc, error) {
	n := len(index.PageLocations)
	if n == 0 {
		return nil, errors.New("the OffsetIndex is empty")
	}
	locs := make([]pageLoc, n)
	previousEnd := start
	for i, page := range index.PageLocations {
		first, last := page.FirstRowIndex, rows
		if i+1 < n {
			last = index.PageLocations[i+1].FirstRowIndex
		}
		if i == 0 && first != 0 || first < 0 || last <= first || last > rows {
			return nil, errors.New("the OffsetIndex row positions are invalid")
		}
		off, size := page.Offset, int64(page.CompressedPageSize)
		if size <= 0 || off < previousEnd || off > end-size {
			return nil, errors.New("a page location lies outside its column chunk, out of order or overlapping")
		}
		previousEnd = off + size
		locs[i] = pageLoc{first: first, rows: last - first, offset: off, size: size}
	}
	return locs, nil
}

// supportedCodec reports whether decompress decodes codec.
func supportedCodec(codec format.CompressionCodec) bool {
	switch codec {
	case format.Uncompressed, format.Snappy, format.Gzip, format.Brotli, format.Zstd, format.Lz4Raw:
		return true
	}
	return false
}

// supportedEncoding reports whether decodeValues decodes enc.
func supportedEncoding(enc format.Encoding) bool {
	switch enc {
	case format.Plain, format.RLE, format.DeltaBinaryPacked, format.DeltaLengthByteArray, format.DeltaByteArray, format.ByteStreamSplit:
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

// chunk reads the pages of one column chunk for one request, and decodes
// its dictionary on first use. ctx is the request's.
type chunk struct {
	scratch    *pageScratch // nil: allocating key/dictionary path (keys: see scan.keyScratch)
	ctx        context.Context
	p          *part
	physical   int
	col        leaf
	codec      format.CompressionCodec
	start, end int64
	// code refuses a page its index entry does not describe:
	// invalid_page_index for the key column (RG-3, RG-7), invalid_parquet
	// for a value column (CHECK-5).
	code RefusalCode
	// first is the offset of the chunk's first data page when an
	// OffsetIndex gives it, so that what precedes it is the dictionary's
	// extent; zero when not known.
	first    int64
	dict     parquet.Dictionary
	dictRead bool
	stats    *ReadStats
	pages    *int64 // the counter of the data pages decoded: key or value pages
	// ahead, when set, holds key pages a listing read ahead: located takes
	// a page's bytes from it instead of reading them (readAhead).
	ahead *readAhead
	// memo, when set, holds the key pages the request read (pageMemo).
	memo *pageMemo
}

// openChunk checks a column chunk before any of its pages is read: a codec
// this reader decodes (else unsupported_encoding) and a byte range inside
// the file (else invalid_footer).
func (p *part) openChunk(ctx context.Context, physical int, col leaf, st *ReadStats, pages *int64) (*chunk, error) {
	meta := &p.footer.Metadata().RowGroups[physical].Columns[col.index].MetaData
	if !supportedCodec(meta.Codec) {
		return nil, &UnsupportedError{Code: UnsupportedEncoding, Detail: "codec " + meta.Codec.String() + " in part " + p.name}
	}
	start, end, ok := chunkRange(meta)
	if !ok || end > p.size || meta.DataPageOffset < start || meta.DataPageOffset >= end {
		return nil, refusePart(CodeInvalidFooter, p.name, physical, "column chunk %d lies outside the file", col.index)
	}
	code := CodeInvalidParquet
	if col.index == p.key.index {
		code = CodeInvalidPageIndex
	}
	return &chunk{ctx: ctx, p: p, physical: physical, col: col, codec: meta.Codec, start: start, end: end, code: code, stats: st, pages: pages}, nil
}

// corrupt reports a page that does not decode (CHECK-5); an error of the
// environment, an *UnsupportedError or a *RefusalError is returned as is.
func (c *chunk) corrupt(err error) error {
	return c.p.dataError(c.ctx, c.physical, fmt.Errorf("column chunk %d: %w", c.col.index, err))
}

// header reads, decodes and checks the page header at off. It returns the
// header, its length, and the bytes read from off, which may hold the page
// that follows the header too.
func (c *chunk) header(off int64) (*format.PageHeader, int64, []byte, error) {
	return c.p.pageHeaderInto(c.ctx, c.physical, c.col.index, off, c.end, c.stats, c.scratch)
}

// located reads the page that a page index locates at off, size bytes
// with its header, in one read, and returns what header returns. A size
// beyond any page within the limits is read through header instead, so
// that it fails as any page beyond them does.
func (c *chunk) located(off, size, rows int64) (*format.PageHeader, int64, []byte, error) {
	if lim := &c.p.r.limits; size > lim.PageBytes+lim.headerBytes() {
		return c.header(off)
	}
	var pre []byte
	if c.ahead != nil {
		pre, _ = c.ahead.take(c.p, off, size)
	}
	if pre == nil {
		if kept := c.memo.get(c.p, off, size); kept != nil {
			c.stats.RequestMemoHitBytes += size
			pre = kept
			// A scan decodes into its scratch, which its next page
			// overwrites: it decodes a copy, as if it had read the page.
			if c.scratch != nil {
				pre = c.scratch.readBuffer(int(size))
				copy(pre, kept)
			}
		}
	}
	h, n, raw, err := c.p.pageExtent(c.ctx, c.physical, c.col.index, off, size, rows, c.end, c.stats, c.scratch, pre)
	if err == nil && pre == nil && c.memo != nil && c.scratch == nil {
		c.memo.put(c.p, off, raw)
	}
	return h, n, raw, err
}

// payload returns the page data after a header of length n at off, from
// raw when it holds them, after checking the CRC its header carries, when
// it carries one (zero included). The slice's capacity ends with the page,
// so that no decoder can read the bytes after it (raw may hold the next
// page's header).
func (c *chunk) payload(h *format.PageHeader, off, n int64, raw []byte) ([]byte, error) {
	// A separate payload read may overwrite raw in the same scratch allocation.
	// Determine optional CRC presence while the header bytes are still live.
	hasCRC := h.CRC != 0 || c.hasCRC(off, n, raw)
	size := int64(h.CompressedPageSize)
	var data []byte
	if int64(len(raw)) >= n+size {
		data = raw[n : n+size : n+size]
	} else {
		data = c.scratch.readBuffer(int(size))
		if err := c.p.at(c.ctx, c.stats).readFull(data, off+n); err != nil {
			return nil, c.corrupt(err)
		}
	}
	if hasCRC {
		if uint32(h.CRC) != crc32.ChecksumIEEE(data) {
			return nil, c.corrupt(errors.New("the page fails its checksum"))
		}
	}
	return data, nil
}

// crcField decodes only a page header's optional crc (field 4), keeping
// whether it is present: format.PageHeader holds it as a plain int32, so a
// CRC of zero reads as none.
type crcField struct {
	CRC *int32 `thrift:"4,optional"`
}

// hasCRC reports whether the page header of length n at off, held at the
// start of raw or read again, carries a crc field: called only when the
// decoded CRC is zero, to tell a stored zero from an absent one.
func (c *chunk) hasCRC(off, n int64, raw []byte) bool {
	if int64(len(raw)) < n {
		raw = make([]byte, n)
		if err := c.p.at(c.ctx, c.stats).readFull(raw, off); err != nil {
			return false // the header was read before; a failing reread is not the page's
		}
	}
	if present, ok := crcFieldPresent(raw[:n]); ok {
		return present
	}
	var f crcField
	return thrift.Unmarshal(new(thrift.CompactProtocol), raw[:n], &f) == nil && f.CRC != nil
}

// crcFieldPresent reads, without allocating, whether a page header in the
// compact protocol carries its crc (field 4), when the header begins as
// writers encode it: fields 1 to 3, each an i32 in a short-form header
// (0x15), then field 4 (0x15) or a later field in short form. ok is false
// for any other encoding, which hasCRC then decodes in full. Every page
// without a CRC (most pages) asks this, so it must not cost a decode.
func crcFieldPresent(header []byte) (present, ok bool) {
	p := 0
	for range 3 {
		if p >= len(header) || header[p] != 0x15 {
			return false, false
		}
		_, k := binary.Uvarint(header[p+1:])
		if k <= 0 {
			return false, false
		}
		p += 1 + k
	}
	if p >= len(header) {
		return false, false
	}
	switch b := header[p]; {
	case b == 0x15:
		return true, true
	case b>>4 > 1: // a short-form header of field 5 or later: no field 4 before it
		return false, true
	}
	return false, false // the end of the struct, or a long-form header
}

// dataRows returns the rows of a data page and true, or false for a page
// of another type.
func dataRows(h *format.PageHeader) (int64, bool) {
	switch h.Type {
	case format.DataPage:
		return int64(h.DataPageHeader.V.NumValues), h.DataPageHeader.Valid
	case format.DataPageV2:
		return int64(h.DataPageHeaderV2.V.NumRows), h.DataPageHeaderV2.Valid
	}
	return 0, false
}

// indexed decodes page i, which loc locates, after requiring its header to
// begin exactly one data page that fills loc and holds loc's rows (RG-3).
func (c *chunk) indexed(i int, loc pageLoc) ([]parquet.Value, error) {
	h, data, err := c.indexedPage(i, loc)
	if err != nil {
		return nil, err
	}
	return c.decode(h, data)
}

// indexedKeys decodes key page i, which loc locates, as indexed does, into
// pageKeys (decodeKeys).
func (c *chunk) indexedKeys(i int, loc pageLoc) (pageKeys, error) {
	h, data, err := c.indexedPage(i, loc)
	if err != nil {
		return pageKeys{}, err
	}
	return c.decodeKeys(h, data)
}

// indexedPage reads page i, which loc locates, after requiring its header
// to begin exactly one data page that fills loc and holds loc's rows
// (RG-3), and returns its header and payload.
func (c *chunk) indexedPage(i int, loc pageLoc) (*format.PageHeader, []byte, error) {
	bad := func(format string, args ...any) error {
		return refusePart(c.code, c.p.name, c.physical, "column chunk %d page %d: "+format, append([]any{c.col.index, i}, args...)...)
	}
	h, n, raw, err := c.located(loc.offset, loc.size, loc.rows)
	if err != nil {
		if RefusalCodeOf(err) == CodeInvalidParquet {
			return nil, nil, bad("its OffsetIndex entry does not locate a page header")
		}
		return nil, nil, err
	}
	rows, ok := dataRows(h)
	switch {
	case !ok:
		return nil, nil, bad("its OffsetIndex entry locates a page of type %s, not a data page", h.Type)
	case n+int64(h.CompressedPageSize) != loc.size:
		return nil, nil, bad("its OffsetIndex entry does not locate exactly one page")
	case rows != loc.rows:
		return nil, nil, bad("the page holds %d rows, its OffsetIndex entry %d", rows, loc.rows)
	}
	data, err := c.payload(h, loc.offset, n, raw)
	if err != nil {
		return nil, nil, err
	}
	return h, data, nil
}

// recoverPage is a last line of defence: a panic inside parquet-go's
// decoders is reported as corrupt data. The checks before decoding are the
// protection; this is not a substitute for them.
func (c *chunk) recoverPage(err *error) {
	if r := recover(); r != nil {
		*err = c.corrupt(fmt.Errorf("parquet-go failed on a page: %v", r))
	}
}

// pageData decompresses a data page's payload, after checking its header
// against Limits.PageMemory, and returns its value count n, its encoding,
// its definition levels (an optional column's), its values, and a V2
// page's declared null count (-1 for a V1 page).
func (c *chunk) pageData(h *format.PageHeader, payload []byte) (n int, enc format.Encoding, levels, data []byte, nulls int, err error) {
	if err := c.checkPageMemory(h); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	nulls = -1
	switch h.Type {
	case format.DataPage:
		d := &h.DataPageHeader.V
		n, enc = int(d.NumValues), d.Encoding
		if data, err = c.scratch.decompress(c.codec, payload, int(h.UncompressedPageSize)); err != nil {
			return 0, 0, nil, nil, 0, c.corrupt(err)
		}
		if c.col.optional {
			if d.DefinitionLevelEncoding != format.RLE {
				return 0, 0, nil, nil, 0, &UnsupportedError{Code: UnsupportedEncoding, Detail: "definition levels encoded " + d.DefinitionLevelEncoding.String() + " in part " + c.p.name}
			}
			if len(data) < 4 || int64(binary.LittleEndian.Uint32(data)) > int64(len(data)-4) {
				return 0, 0, nil, nil, 0, c.corrupt(errors.New("the definition levels exceed the page"))
			}
			m := 4 + int(binary.LittleEndian.Uint32(data))
			levels, data = data[4:m:m], data[m:]
		}
	case format.DataPageV2:
		d := &h.DataPageHeaderV2.V
		n, enc = int(d.NumValues), d.Encoding
		if d.NumRows != d.NumValues {
			return 0, 0, nil, nil, 0, c.corrupt(errors.New("a page of a non-repeated column holds a different number of rows and values"))
		}
		rep, def := int(d.RepetitionLevelsByteLength), int(d.DefinitionLevelsByteLength)
		// A top-level, non-repeated column has no repetition levels, and a
		// REQUIRED one no definition levels: bytes declared for them are not
		// the page's values (CHECK-5).
		if rep != 0 {
			return 0, 0, nil, nil, 0, c.corrupt(fmt.Errorf("a page of a non-repeated column declares %d bytes of repetition levels", rep))
		}
		if !c.col.optional && def != 0 {
			return 0, 0, nil, nil, 0, c.corrupt(fmt.Errorf("a page of a REQUIRED column declares %d bytes of definition levels", def))
		}
		nulls = int(d.NumNulls)
		levels, data = payload[:def:def], payload[def:]
		codec := c.codec
		if d.IsCompressed.Valid && !d.IsCompressed.V {
			codec = format.Uncompressed
		}
		if data, err = c.scratch.decompress(codec, data, int(h.UncompressedPageSize)-def); err != nil {
			return 0, 0, nil, nil, 0, c.corrupt(err)
		}
	}
	return n, enc, levels, data, nulls, nil
}

// decode decodes a data page: one value per row, null where the
// definition level is zero.
func (c *chunk) decode(h *format.PageHeader, payload []byte) (_ []parquet.Value, err error) {
	defer c.recoverPage(&err)
	n, enc, levels, data, nulls, err := c.pageData(h, payload)
	if err != nil {
		return nil, err
	}
	if !c.col.optional {
		if nulls > 0 {
			return nil, c.corrupt(fmt.Errorf("a page of a REQUIRED column declares %d nulls", nulls))
		}
		values, err := c.values(enc, data, n)
		if err == nil {
			*c.pages++
		}
		return values, err
	}
	// The runs are checked first (each repeated value below 2, no surplus),
	// then decoded into this request's level buffer; a level above the
	// column's maximum of 1 is refused, not counted.
	if err := checkRuns(levels, n, 1); err != nil {
		return nil, c.corrupt(fmt.Errorf("definition levels: %w", err))
	}
	s := c.scratch.local()
	s.levels = resize(s.levels, n+8)
	defined, err := (&rle.Encoding{BitWidth: 1}).DecodeLevels(s.levels[:0:n+8], levels)
	s.levels = decodedBuffer(s.levels, defined)
	if err != nil || len(defined) < n {
		return nil, c.corrupt(errors.New("the page holds fewer definition levels than values"))
	}
	defined = defined[:n]
	for _, level := range defined {
		if level > 1 {
			return nil, c.corrupt(fmt.Errorf("definition levels: a level of %d is above the column's maximum 1", level))
		}
	}
	present := 0
	for _, level := range defined {
		present += int(level)
	}
	if nulls >= 0 && nulls != n-present {
		return nil, c.corrupt(fmt.Errorf("the page declares %d nulls, its definition levels hold %d", nulls, n-present))
	}
	values, err := c.values(enc, data, present)
	if err != nil {
		return nil, err
	}
	*c.pages++
	s.out = resize(s.out, n)
	out := s.out
	for i, j := 0, 0; i < n; i++ {
		if defined[i] == 0 {
			out[i] = parquet.Value{}.Level(0, 0, c.col.index)
			continue
		}
		out[i] = values[j].Level(0, 1, c.col.index)
		j++
	}
	return out, nil
}

// values decodes the n non-null values of a page, encoded with enc.
func (c *chunk) values(enc format.Encoding, data []byte, n int) ([]parquet.Value, error) {
	// A reused decompression buffer can be longer than this page: no decoder
	// may reach past the page into a previous page's bytes.
	data = data[:len(data):len(data)]
	s := c.scratch.local()
	// Drop pointers into the previous page/dictionary, including a long page's
	// unused tail, before decoding a smaller page or another chunk.
	s.clearValues()
	s.values = resize(s.values, n)
	out := s.values
	if enc == format.PlainDictionary || enc == format.RLEDictionary {
		dict, indexes, err := c.dictionaryIndexes(data, n)
		if err != nil {
			return nil, err
		}
		dict.Lookup(indexes, out)
		return out, nil
	}
	decoded, err := c.decodeValues(enc, data, n)
	if err != nil {
		return nil, err
	}
	if read, _ := c.col.typ.NewPage(c.col.index, n, decoded).Values().ReadValues(out); read != n {
		return nil, c.corrupt(fmt.Errorf("the page holds %d of its %d values", read, n))
	}
	return out, nil
}

// dictionaryIndexes returns the chunk's dictionary and the n dictionary
// indexes of a dictionary-encoded page's values, data, each checked to
// lie inside the dictionary. A page of no values may hold no indexes, nor
// their bit width: one whose values are all null (Java and PyArrow read
// it so).
func (c *chunk) dictionaryIndexes(data []byte, n int) (parquet.Dictionary, []int32, error) {
	dict, err := c.dictionary()
	if err != nil {
		return nil, nil, err
	}
	if dict == nil {
		return nil, nil, c.corrupt(errors.New("a dictionary-encoded page in a chunk without a dictionary"))
	}
	if n == 0 && len(data) == 0 {
		return dict, nil, nil
	}
	if len(data) == 0 || data[0] > 32 {
		return nil, nil, c.corrupt(errors.New("the dictionary indexes have no valid bit width"))
	}
	width := int(data[0])
	if err := checkRuns(data[1:], n, width); err != nil {
		return nil, nil, c.corrupt(fmt.Errorf("dictionary indexes: %w", err))
	}
	s := c.scratch.local()
	s.indexes = resize(s.indexes, n+8)
	indexes, err := (&rle.Encoding{BitWidth: width}).DecodeInt32(s.indexes[:0:n+8], data[1:])
	s.indexes = decodedBuffer(s.indexes, indexes)
	if err != nil || len(indexes) < n {
		return nil, nil, c.corrupt(errors.New("the page holds fewer dictionary indexes than values"))
	}
	indexes = indexes[:n]
	for _, i := range indexes {
		if i < 0 || int(i) >= dict.Len() {
			return nil, nil, c.corrupt(fmt.Errorf("dictionary index %d is outside the dictionary of %d values", i, dict.Len()))
		}
	}
	return dict, indexes, nil
}

// decodeKeys decodes a key page (a REQUIRED BYTE_ARRAY column's, KEY-1)
// as decode would, into pageKeys instead of parquet.Values: the decoded
// bytes and offsets of a PLAIN, DELTA_LENGTH_BYTE_ARRAY or
// DELTA_BYTE_ARRAY page, or the dictionary's with the page's indexes.
// With the chunk's scratch, the page's storage is the scratch's and the
// next page decoded through it overwrites it; without, it is the page's own.
func (c *chunk) decodeKeys(h *format.PageHeader, payload []byte) (_ pageKeys, err error) {
	defer c.recoverPage(&err)
	if c.col.optional || c.col.typ.Kind() != parquet.ByteArray {
		return pageKeys{}, c.corrupt(errors.New("a key column that is not a REQUIRED BYTE_ARRAY column"))
	}
	n, enc, _, data, nulls, err := c.pageData(h, payload)
	if err != nil {
		return pageKeys{}, err
	}
	if nulls > 0 {
		return pageKeys{}, c.corrupt(fmt.Errorf("a page of a REQUIRED column declares %d nulls", nulls))
	}
	data = data[:len(data):len(data)] // as values: no decoder may read past the page
	var page pageKeys
	if enc == format.PlainDictionary || enc == format.RLEDictionary {
		dict, indexes, err := c.dictionaryIndexes(data, n)
		if err != nil {
			return pageKeys{}, err
		}
		values := dict.Page().Data()
		if values.Kind() != encoding.ByteArray {
			return pageKeys{}, c.corrupt(errors.New("a key dictionary that does not hold byte arrays"))
		}
		page.data, page.offsets = values.ByteArray()
		if len(page.offsets) != dict.Len()+1 {
			return pageKeys{}, c.corrupt(errors.New("a key dictionary whose offsets do not match its values"))
		}
		page.indexes = indexes
		if indexes == nil {
			page.indexes = []int32{}
		}
	} else {
		decoded, err := c.decodeValues(enc, data, n)
		if err != nil {
			return pageKeys{}, err
		}
		page.data, page.offsets = decoded.ByteArray()
		if len(page.offsets) != n+1 {
			return pageKeys{}, c.corrupt(fmt.Errorf("the page decodes to %d of its %d values", len(page.offsets)-1, n))
		}
	}
	*c.pages++
	return page, nil
}

// pageKeys is a decoded key page. Key i is data[offsets[j]:offsets[j+1]],
// where j is indexes[i] for a dictionary-encoded page (indexes not nil)
// and i otherwise.
type pageKeys struct {
	data    []byte
	offsets []uint32
	indexes []int32
}

// len returns the number of keys.
func (p *pageKeys) len() int {
	if p.indexes != nil {
		return len(p.indexes)
	}
	return max(len(p.offsets)-1, 0)
}

// key returns key i, its capacity ending with it.
func (p *pageKeys) key(i int) []byte {
	j := i
	if p.indexes != nil {
		j = int(p.indexes[i])
	}
	from, to := p.offsets[j], p.offsets[j+1]
	return p.data[from:to:to]
}

// slice returns keys [from, to) as a pageKeys.
func (p *pageKeys) slice(from, to int) pageKeys {
	if p.indexes != nil {
		return pageKeys{data: p.data, offsets: p.offsets, indexes: p.indexes[from:to:to]}
	}
	return pageKeys{data: p.data, offsets: p.offsets[from : to+1 : to+1]}
}

// decodeValues decodes n values encoded with enc, after checking the value
// counts the encoding declares and the decoded size, and requires the
// decoded values to hold exactly n of them.
func (c *chunk) decodeValues(enc format.Encoding, data []byte, n int) (encoding.Values, error) {
	var none encoding.Values
	s := c.scratch.local()
	if !supportedEncoding(enc) {
		return none, &UnsupportedError{Code: UnsupportedEncoding, Detail: "encoding " + enc.String() + " in part " + c.p.name}
	}
	kind := c.col.typ.Kind()
	if enc == format.RLE && kind != parquet.Boolean {
		// Parquet defines RLE values only for Booleans. parquet-go would
		// decode another type's payload as runs of bit width 0, where one
		// run header can declare 2^31 values that occupy no bytes: refused
		// before anything is decoded or allocated.
		return none, c.corrupt(fmt.Errorf("RLE values in a %s column", kind))
	}
	if err := checkCounts(enc, data, n, kind == parquet.Int64); err != nil {
		return none, c.corrupt(err)
	}
	if enc == format.Plain && kind == parquet.ByteArray {
		// parquet-go bounds a value's length by the whole buffer rather
		// than by the bytes after it: check every length first.
		if err := plainByteArrays(data, n); err != nil {
			return none, c.corrupt(err)
		}
	}
	// The dependency's Boolean hybrid decoder copies a repeated bit as a byte,
	// which turns a run of true into true,false,... . Expand into a bounded
	// bitmap ourselves, including runs that begin between byte boundaries.
	if c.col.typ.Kind() == parquet.Boolean && enc == format.RLE {
		size := (n + 7) / 8
		if int64(size) > c.p.r.limits.PageBytes {
			return none, exceeded(LimitPageBytes, int64(size), c.p.r.limits.PageBytes, "part %s: a Boolean page decoding", c.p.name)
		}
		s.data = resize(s.data, size)
		bits, err := decodeBooleanRLEInto(s.data, data, n)
		if err != nil {
			return none, c.corrupt(err)
		}
		return c.col.typ.NewValues(bits, nil), nil
	}
	typ, e := c.col.typ, parquet.LookupEncoding(enc)
	size := typ.EstimateDecodeSize(n, data, e)
	if size < 0 {
		return none, c.corrupt(errors.New("the encoded value lengths are invalid"))
	}
	if limit := c.p.r.limits.PageBytes; int64(size) > limit {
		return none, exceeded(LimitPageBytes, int64(size), limit, "part %s: a page decoding", c.p.name)
	}
	var offsets []uint32
	if typ.Kind() == parquet.ByteArray {
		s.offsets = resize(s.offsets, n+1)
		offsets = s.offsets[: 0 : n+1]
	}
	// parquet-go v0.32.0's amd64 DELTA_BYTE_ARRAY decoder requests
	// totalLength+padding in encoding/delta/byte_array_amd64.go; padding is
	// defined in encoding/delta/delta_amd64.go. Give it that capacity to avoid
	// regrowing every page. purego leaves it unused; exactly still checks length.
	const parquetGoDeltaPadding = 64
	s.data = resize(s.data, size+parquetGoDeltaPadding)
	decoded, err := typ.Decode(typ.NewValues(s.data[:0:size+parquetGoDeltaPadding], offsets), data, e)
	// Encodings may replace either destination; retain the actual returned buffers.
	dataBytes, dataOffsets := decoded.Data()
	s.data = decodedBuffer(s.data, dataBytes)
	s.offsets = decodedBuffer(s.offsets, dataOffsets)
	if err != nil {
		return none, c.corrupt(err)
	}
	if err := exactly(typ, &decoded, n); err != nil {
		return none, c.corrupt(err)
	}
	return decoded, nil
}

// exactly requires decoded to hold exactly n values. The dependency's page
// exposes only the first n of them, so a payload holding more would pass
// every later count unnoticed (CHECK-5). The only padding allowed is that
// of PLAIN Booleans, bit-packed into whole bytes: the final byte may hold
// up to seven bits after the last value. (A Boolean RLE stream, with the
// padding of its final bit-packed group, does not reach here.)
func exactly(typ parquet.Type, decoded *encoding.Values, n int) error {
	got := held(typ, decoded)
	data, _ := decoded.Data()
	switch typ.Kind() {
	case parquet.Boolean:
		if int64(len(data)) == (int64(n)+7)/8 {
			return nil
		}
	case parquet.FixedLenByteArray:
		// held rounds down: a DELTA_BYTE_ARRAY page can decode to a length
		// that is not a whole number of values.
		if int64(len(data)) == int64(n)*int64(max(typ.Length(), 1)) {
			return nil
		}
		if got == int64(n) {
			return fmt.Errorf("the page decodes to %d bytes, not %d values of %d", len(data), n, typ.Length())
		}
	default:
		if got == int64(n) {
			return nil
		}
	}
	if got < int64(n) {
		return fmt.Errorf("the page decodes to fewer than its %d values", n)
	}
	return fmt.Errorf("the page decodes to more than its %d values", n)
}

// held returns the number of values decoded holds.
func held(typ parquet.Type, decoded *encoding.Values) int64 {
	data, offsets := decoded.Data()
	switch typ.Kind() {
	case parquet.Boolean:
		return 8 * int64(len(data))
	case parquet.Int32, parquet.Float:
		return int64(len(data) / 4)
	case parquet.Int64, parquet.Double:
		return int64(len(data) / 8)
	case parquet.Int96:
		return int64(len(data) / 12)
	case parquet.FixedLenByteArray:
		return int64(len(data) / max(typ.Length(), 1))
	}
	return int64(len(offsets) - 1)
}

// dictionary returns the chunk's dictionary, nil when it has none: the
// dictionary page that begins the chunk, decoded on first use. When the
// chunk's first data page is known, the bytes before it are the
// dictionary's extent, read at once.
func (c *chunk) dictionary() (_ parquet.Dictionary, err error) {
	if c.dictRead {
		return c.dict, nil
	}
	c.dictRead = true
	// The last line of defence decode has: a planned scan and the validator
	// read the dictionary outside decode.
	defer func() {
		if r := recover(); r != nil {
			c.dict = nil
			err = c.corrupt(fmt.Errorf("parquet-go failed on the dictionary page: %v", r))
		}
	}()
	// A lazy read must not overwrite the active data page or its decode buffers.
	dictionaryChunk := *c
	dictionaryChunk.scratch = nil
	c.dict, err = dictionaryChunk.readDictionary()
	return c.dict, err
}

func (c *chunk) readDictionary() (parquet.Dictionary, error) {
	none := func() (parquet.Dictionary, error) {
		if c.p.footer.Metadata().RowGroups[c.physical].Columns[c.col.index].MetaData.DictionaryPageOffset != 0 {
			return nil, c.corrupt(errors.New("no dictionary page at its dictionary offset"))
		}
		return nil, nil
	}
	var h *format.PageHeader
	var n int64
	var raw []byte
	var err error
	switch {
	case c.first == c.start:
		return none()
	case c.first != 0:
		h, n, raw, err = c.located(c.start, c.first-c.start, -1)
	default:
		h, n, raw, err = c.header(c.start)
	}
	if err != nil {
		return nil, err
	}
	if h.Type != format.DictionaryPage || !h.DictionaryPageHeader.Valid {
		return none()
	}
	if c.first != 0 && n+int64(h.CompressedPageSize) > c.first-c.start {
		return nil, c.corrupt(errors.New("the dictionary page overlaps the first data page"))
	}
	d := &h.DictionaryPageHeader.V
	if d.Encoding != format.Plain && d.Encoding != format.PlainDictionary {
		return nil, &UnsupportedError{Code: UnsupportedEncoding, Detail: "a dictionary encoded " + d.Encoding.String() + " in part " + c.p.name}
	}
	if err := c.checkPageMemory(h); err != nil {
		return nil, err
	}
	payload, err := c.payload(h, c.start, n, raw)
	if err != nil {
		return nil, err
	}
	data, err := c.scratch.decompress(c.codec, payload, int(h.UncompressedPageSize))
	if err != nil {
		return nil, c.corrupt(fmt.Errorf("dictionary page: %w", err))
	}
	decoded, err := c.decodeValues(format.Plain, data, int(d.NumValues))
	if err != nil {
		return nil, err
	}
	c.dict = c.col.typ.NewDictionary(c.col.index, int(d.NumValues), decoded)
	return c.dict, nil
}

// pageMemory estimates, from a page header alone, the memory decoding the
// page holds (Limits.PageMemory): valueBytes for each value's parquet.Value,
// optionalValueSlices times that for an optional column, and the bytes its
// decoded values hold: their fixed width, or for byte arrays the page's
// uncompressed size (PLAIN and DELTA_LENGTH_BYTE_ARRAY values, and
// dictionary indexes, take no more), or PageBytes for DELTA_BYTE_ARRAY,
// whose shared prefixes can expand it. decodeValues refuses decoded bytes
// beyond PageBytes, so that term is capped there. A dictionary page counts
// as a page of one Value slice. The estimate is arithmetic only.
func (c *chunk) pageMemory(h *format.PageHeader, optional bool) int64 {
	var values int64
	var enc format.Encoding
	switch h.Type {
	case format.DataPage:
		values, enc = int64(h.DataPageHeader.V.NumValues), h.DataPageHeader.V.Encoding
	case format.DataPageV2:
		d := &h.DataPageHeaderV2.V
		values, enc = int64(max(d.NumValues, d.NumRows)), d.Encoding
	case format.DictionaryPage:
		values, enc = int64(h.DictionaryPageHeader.V.NumValues), h.DictionaryPageHeader.V.Encoding
	}
	values = max(values, 0)
	slices := int64(1)
	if optional {
		slices = optionalValueSlices
	}
	pageBytes := c.p.r.limits.PageBytes
	var decoded int64
	switch kind := c.col.typ.Kind(); kind {
	case parquet.ByteArray:
		decoded = int64(max(h.UncompressedPageSize, 0))
		if enc == format.DeltaByteArray {
			decoded = pageBytes
		}
	case parquet.Boolean:
		decoded = values // at most a byte per value
	case parquet.FixedLenByteArray:
		decoded = values * int64(max(c.col.typ.Length(), 1))
	case parquet.Int32, parquet.Float:
		decoded = values * 4
	case parquet.Int64, parquet.Double:
		decoded = values * 8
	default: // INT96
		decoded = values * 12
	}
	return values*valueBytes*slices + min(decoded, pageBytes)
}

// checkPageMemory fails with an error wrapping ErrLimitExceeded when the
// page whose header is h would hold more than Limits.PageMemory decoding,
// before anything is allocated for its values.
func (c *chunk) checkPageMemory(h *format.PageHeader) error {
	optional := c.col.optional && h.Type != format.DictionaryPage
	if need, limit := c.pageMemory(h, optional), c.p.r.limits.PageMemory; need > limit {
		return exceeded(LimitPageMemory, need, limit, "part %s: a page whose decoding holds about that many bytes", c.p.name)
	}
	return nil
}

// checkCounts checks the value counts an encoding declares inside a page
// against the n values the page header declares, before a decoder
// allocates for them. int64Values tells a DELTA_BINARY_PACKED stream of INT64
// values (bit widths up to 64) from one of INT32 values; the lengths of
// the byte-array delta encodings are INT32 (widths up to 32).
func checkCounts(enc format.Encoding, data []byte, n int, int64Values bool) error {
	switch enc {
	case format.RLE: // booleans: a 4-byte length, then runs of one bit
		if len(data) < 4 || int64(binary.LittleEndian.Uint32(data)) > int64(len(data)-4) {
			return errors.New("the RLE length exceeds the page")
		}
		m := 4 + int(binary.LittleEndian.Uint32(data))
		return checkRuns(data[4:m:m], n, 1)
	case format.DeltaBinaryPacked:
		width := 32
		if int64Values {
			width = 64
		}
		_, err := deltaStream(data, n, width)
		return err
	case format.DeltaLengthByteArray: // lengths, then the values' bytes
		return deltaLengths(data, n)
	case format.DeltaByteArray: // prefix lengths, then suffix lengths and bytes
		rest, err := deltaStream(data, n, 32)
		if err != nil {
			return err
		}
		return deltaLengths(rest, n)
	}
	return nil
}

// deltaLengths checks a DELTA_BINARY_PACKED stream of at most n lengths
// followed by the bytes they measure: every length is non-negative and
// together they cover exactly the bytes after the stream. Surplus bytes
// would be values no one reads, and parquet-go's amd64 decoder of
// DELTA_BYTE_ARRAY reads its last values from the end of the page, so
// with surplus bytes it would return the wrong ones (CHECK-5). The lengths
// are decoded to be summed; their count is bounded by n.
func deltaLengths(src []byte, n int) error {
	rest, err := deltaStream(src, n, 32)
	if err != nil {
		return err
	}
	total, err := deltaInt32Sum(src[:len(src)-len(rest)])
	if err != nil {
		return err
	}
	if total != int64(len(rest)) {
		return fmt.Errorf("the value lengths add up to %d bytes, the page holds %d after them", total, len(rest))
	}
	return nil
}

// deltaInt32Sum returns the sum of the INT32 values of a DELTA_BINARY_PACKED
// stream that deltaStream has checked (its geometry, its bit widths of at
// most 32, and its miniblocks inside it), refusing a negative value. It
// decodes as parquet-go does (each value the previous plus the block's min
// delta plus the miniblock's packed delta, in wrapping INT32 arithmetic)
// without allocating: every page of the byte-array delta encodings asks
// it, key pages included.
func deltaInt32Sum(src []byte) (int64, error) {
	var header [3]uint64 // block size, miniblocks per block, value count
	for i := range header {
		v, k := binary.Uvarint(src)
		header[i], src = v, src[k:]
	}
	first, k := binary.Varint(src)
	src = src[k:]
	block, miniblocks, count := header[0], header[1], header[2]
	if count == 0 {
		return 0, nil
	}
	if first != int64(int32(first)) {
		return 0, fmt.Errorf("the first value length %d is outside INT32", first)
	}
	value := int32(first)
	if value < 0 {
		return 0, fmt.Errorf("a value length of %d", value)
	}
	sum := int64(value)
	per := int(block / miniblocks)
	for left := int64(count) - 1; left > 0; {
		minDelta, k := binary.Varint(src)
		widths := src[k : k+int(miniblocks)]
		src = src[k+int(miniblocks):]
		for _, w := range widths {
			if left <= 0 {
				break
			}
			size := per * int(w) / 8
			mini, mask := src[:size], uint64(1)<<w-1
			for i := 0; i < per && left > 0; i, left = i+1, left-1 {
				bit := i * int(w)
				var x uint64
				for b := 0; b < 5 && bit/8+b < len(mini); b++ {
					x |= uint64(mini[bit/8+b]) << (8 * b)
				}
				value = int32(uint32(value) + uint32(int32(minDelta)) + uint32((x>>(bit%8))&mask))
				if value < 0 {
					return 0, fmt.Errorf("a value length of %d", value)
				}
				sum += int64(value)
			}
			src = src[size:]
		}
	}
	return sum, nil
}

// plainByteArrays requires src to hold exactly n PLAIN BYTE_ARRAY values,
// each a 4-byte length and that many bytes: every length inside the
// stream, and the values ending exactly at its end.
func plainByteArrays(src []byte, n int) error {
	at := 0
	for i := range n {
		if len(src)-at < 4 {
			return fmt.Errorf("PLAIN BYTE_ARRAY value %d has no length", i)
		}
		length := uint64(binary.LittleEndian.Uint32(src[at:]))
		at += 4
		if length > uint64(len(src)-at) {
			return fmt.Errorf("a PLAIN BYTE_ARRAY value of %d bytes overruns its page", length)
		}
		at += int(length)
	}
	if at != len(src) {
		return fmt.Errorf("the page holds %d bytes after its %d PLAIN BYTE_ARRAY values", len(src)-at, n)
	}
	return nil
}

// checkRuns requires an RLE/bit-packed hybrid stream of the given bit width
// to hold at most n values, without decoding it: a repeated run may not
// hold more values than remain, and a bit-packed run only the padding of
// its final group of eight; no run with values may follow the nth. A run
// of no values holds nothing and is stepped over (parquet-go writes one
// for a page of no indexes, and decodes one anywhere). A repeated run's
// value must fit in width bits (below 2^width; any value at width 32):
// a larger one is no value of the stream, and decoders differ on it (Go's
// Boolean decoder kept its low bit, parquet-java any non-zero value).
func checkRuns(src []byte, n, width int) error {
	total := uint64(0)
	for len(src) > 0 {
		h, k := binary.Uvarint(src)
		if k <= 0 {
			return errors.New("a run header is malformed")
		}
		src = src[k:]
		// A run of one value repeated, or groups of eight bit-packed values.
		count, size := h>>1, uint64(width+7)/8
		packed := h&1 == 1
		if packed {
			size = count * uint64(width)
		}
		switch remaining := uint64(n) - min(total, uint64(n)); {
		case count == 0:
		case remaining == 0:
			return fmt.Errorf("the runs hold more than the page's %d values", n)
		case packed && count > (remaining+7)/8, !packed && count > remaining:
			return fmt.Errorf("the runs hold more than the page's %d values", n)
		}
		if packed {
			count *= 8
		}
		if size > uint64(len(src)) {
			return errors.New("a run is truncated")
		}
		if !packed && width < 32 {
			var value uint64
			for i, b := range src[:size] {
				value |= uint64(b) << (8 * i)
			}
			if value>>width != 0 {
				return fmt.Errorf("a repeated run's value %d does not fit in %d bits", value, width)
			}
		}
		total += count
		src = src[size:]
	}
	return nil
}

// deltaStream checks the header of a DELTA_BINARY_PACKED stream, requires
// its value count to be at most n and every miniblock's bit width to be
// at most maxWidth (32 for a stream of INT32 values, 64 for INT64), and
// returns the bytes after its blocks. A wider miniblock is corrupt, and
// checking it here keeps garbage lengths from reaching a size estimate.
func deltaStream(src []byte, n, maxWidth int) ([]byte, error) {
	var header [3]uint64 // block size, miniblocks per block, value count
	for i := range header {
		v, k := binary.Uvarint(src)
		if k <= 0 {
			return nil, errors.New("the delta header is malformed")
		}
		header[i], src = v, src[k:]
	}
	_, k := binary.Varint(src) // the first value
	if k <= 0 {
		return nil, errors.New("the delta header is malformed")
	}
	src = src[k:]
	block, miniblocks, count := header[0], header[1], header[2]
	// Writers use blocks of 128 values or a few times that; a block of more
	// than 2^20 values is refused, so that a miniblock's size (and the
	// decoder's buffer for it) stays small.
	if block == 0 || block > 1<<20 || block%128 != 0 || miniblocks == 0 || block%miniblocks != 0 || (block/miniblocks)%32 != 0 {
		return nil, errors.New("the delta block geometry is invalid")
	}
	if count > uint64(n) {
		return nil, fmt.Errorf("the delta stream declares %d values, the page %d", count, n)
	}
	per := block / miniblocks
	for left := int64(count) - 1; left > 0; {
		_, k := binary.Varint(src) // the block's minimum delta
		if k <= 0 || uint64(len(src)-k) < miniblocks {
			return nil, errors.New("a delta block is truncated")
		}
		widths := src[k : k+int(miniblocks)]
		src = src[k+int(miniblocks):]
		for _, w := range widths {
			if left <= 0 {
				break
			}
			if int(w) > maxWidth {
				return nil, fmt.Errorf("a delta miniblock bit width of %d, above %d", w, maxWidth)
			}
			size := per * uint64(w) / 8
			if size > uint64(len(src)) {
				return nil, errors.New("a delta miniblock is truncated")
			}
			src = src[size:]
			left -= int64(per)
		}
	}
	return src, nil
}

// pageHeader reads, decodes and checks against the limits the page header
// at off in a column chunk ending at end. Its decoding is bounded like a
// footer's; its sizes must be at most Limits.PageBytes and its value count
// at most Limits.PageValues (else ErrLimitExceeded). A header that does not
// decode is invalid_parquet. It returns the header, its length and the
// bytes read from off.
func (p *part) pageHeader(ctx context.Context, physical, column int, off, end int64, st *ReadStats) (*format.PageHeader, int64, []byte, error) {
	return p.pageHeaderInto(ctx, physical, column, off, end, st, nil)
}

func (p *part) pageHeaderInto(ctx context.Context, physical, column int, off, end int64, st *ReadStats, scratch *pageScratch) (*format.PageHeader, int64, []byte, error) {
	bad := p.badPage(physical, column, off)
	if off < 0 || off >= end {
		return nil, 0, nil, bad("outside the column chunk")
	}
	// A header is read in growing pieces up to headerBytes; a small page
	// arrives with its header.
	ceiling := min(end-off, p.r.limits.headerBytes())
	for want := int64(4 << 10); ; want *= 16 {
		raw, err := p.readPage(ctx, off, min(want, ceiling), st, bad, scratch)
		if err != nil {
			return nil, 0, nil, err
		}
		h, n, err := p.decodeHeader(raw, end-off, int64(len(raw)) == ceiling, -1, bad)
		if h != nil || err != nil {
			return h, n, raw, err
		}
	}
}

// pageExtent reads the size bytes at off that a page index locates as one
// page, header included, in a single read, and decodes and checks its
// header as pageHeader does, in a column chunk ending at end; rows is the
// rows its OffsetIndex entry gives, or -1 (decodeHeader). The caller
// bounds size (chunk.located). pre, when not nil, holds those size bytes,
// read ahead (readAhead): they are checked exactly as bytes it reads.
func (p *part) pageExtent(ctx context.Context, physical, column int, off, size, rows, end int64, st *ReadStats, scratch *pageScratch, pre []byte) (*format.PageHeader, int64, []byte, error) {
	bad := p.badPage(physical, column, off)
	if off < 0 || off >= end || size > end-off {
		return nil, 0, nil, bad("outside the column chunk")
	}
	raw := pre
	if int64(len(raw)) != size {
		var err error
		if raw, err = p.readPage(ctx, off, size, st, bad, scratch); err != nil {
			return nil, 0, nil, err
		}
	}
	h, n, err := p.decodeHeader(raw, end-off, true, rows, bad)
	return h, n, raw, err
}

// badPage returns the invalid_parquet refusal of the page at off.
func (p *part) badPage(physical, column int, off int64) func(string, ...any) error {
	return func(format string, args ...any) error {
		return refusePart(CodeInvalidParquet, p.name, physical, "column chunk %d page at %d: "+format, append([]any{column, off}, args...)...)
	}
}

// readPage reads n bytes of a column chunk at off.
func (p *part) readPage(ctx context.Context, off, n int64, st *ReadStats, bad func(string, ...any) error, scratch *pageScratch) ([]byte, error) {
	raw := scratch.readBuffer(int(n))
	if err := p.at(ctx, st).readFull(raw, off); err != nil {
		var refusal *RefusalError
		if isEnvironment(ctx, err) || errors.As(err, &refusal) { // the source's own, or a part it found missing or resized
			return nil, err
		}
		return nil, bad("%v", err)
	}
	return raw, nil
}

// decodeHeader decodes the page header at the start of raw and checks it
// against the limits and against avail, the bytes from the header to the
// end of its column chunk. rows, when not -1, is the row count of the
// OffsetIndex entry that located the page: a data page declaring other
// rows is returned for its caller to refuse (RG-3), even beyond
// Limits.PageValues, which then does not apply. A header that does not decode from raw is
// refused when raw is all there is to read (final), and otherwise reported
// by a nil header and error, so that the caller reads more.
func (p *part) decodeHeader(raw []byte, avail int64, final bool, rows int64, bad func(string, ...any) error) (*format.PageHeader, int64, error) {
	lim := &p.r.limits
	var header format.PageHeader
	length, err := decodeBounded(raw, &header, lim.MetadataMemory)
	switch {
	case errors.Is(err, ErrLimitExceeded):
		return nil, 0, fmt.Errorf("part %s: page header: %w", p.name, err)
	case err != nil && final:
		return nil, 0, bad("the page header does not decode: %v", err)
	case err != nil:
		return nil, 0, nil
	}
	if header.CompressedPageSize < 0 || header.UncompressedPageSize < 0 || int64(length)+int64(header.CompressedPageSize) > avail {
		return nil, 0, bad("the page sizes are invalid")
	}
	if size := int64(max(header.CompressedPageSize, header.UncompressedPageSize)); size > lim.PageBytes {
		return nil, 0, exceeded(LimitPageBytes, size, lim.PageBytes, "part %s: a page", p.name)
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
			int64(h.DefinitionLevelsByteLength)+int64(h.RepetitionLevelsByteLength) > int64(min(header.CompressedPageSize, header.UncompressedPageSize)) {
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
		// A page its OffsetIndex locates with other rows breaks RG-3 whatever
		// the limits: its caller refuses it, rather than call it too large.
		if declared, ok := dataRows(&header); ok && rows >= 0 && declared != rows {
			return &header, int64(length), nil
		}
		return nil, 0, exceeded(LimitPageValues, values, lim.PageValues, "part %s: a page", p.name)
	}
	return &header, int64(length), nil
}

// loadOffsetIndex returns the OffsetIndex of a column chunk, nil when it
// has none. Its encoded size is bounded by Limits.PageIndexBytes and its
// decoding like a footer's.
func (p *part) loadOffsetIndex(ctx context.Context, physical, column int, st *ReadStats) (*format.OffsetIndex, error) {
	meta := &p.footer.Metadata().RowGroups[physical].Columns[column]
	if meta.OffsetIndexOffset == 0 {
		return nil, nil
	}
	raw, err := p.readIndexBytes(ctx, physical, meta.OffsetIndexOffset, meta.OffsetIndexLength, st)
	if err != nil {
		return nil, err
	}
	return p.decodeOffsetIndex(raw)
}

// decodeOffsetIndex decodes an OffsetIndex, bounded like a footer.
func (p *part) decodeOffsetIndex(raw []byte) (*format.OffsetIndex, error) {
	var index format.OffsetIndex
	if _, err := decodeBounded(raw, &index, p.r.limits.MetadataMemory); err != nil {
		return nil, err
	}
	return &index, nil
}

// checkIndexExtent checks that a page index of length bytes at off lies in
// the file and within Limits.PageIndexBytes.
func (p *part) checkIndexExtent(physical int, off int64, length int32) error {
	if off < 0 || length <= 0 || off > p.size-int64(length) {
		return refusePart(CodeInvalidPageIndex, p.name, physical, "a page index lies outside the file")
	}
	if int64(length) > p.r.limits.PageIndexBytes {
		return exceeded(LimitPageIndexBytes, int64(length), p.r.limits.PageIndexBytes, "part %s: a page index", p.name)
	}
	return nil
}

// readIndexBytes reads a page index of length bytes at off, after checking
// that it lies in the file and within Limits.PageIndexBytes.
func (p *part) readIndexBytes(ctx context.Context, physical int, off int64, length int32, st *ReadStats) ([]byte, error) {
	if err := p.checkIndexExtent(physical, off, length); err != nil {
		return nil, err
	}
	if raw := p.retainedBytes(off, int64(length)); raw != nil {
		if st != nil {
			st.MetadataHitBytes += int64(length)
		}
		return raw, nil
	}
	raw := make([]byte, length)
	// An index that begins before the retained bytes and ends in them:
	// only its missing beginning is read.
	if n, kept := p.retainedSuffix(off, int64(length)); n > 0 {
		missing := int64(length) - n
		if err := p.at(ctx, st).readFull(raw[:missing], off); err != nil {
			return nil, err
		}
		copy(raw[missing:], kept)
		if st != nil {
			st.MetadataHitBytes += n
		}
		return raw, nil
	}
	if err := p.at(ctx, st).readFull(raw, off); err != nil {
		return nil, err
	}
	return raw, nil
}

// dataError classifies an error decoding a page: the environment's own
// error, or an *UnsupportedError or *RefusalError, as is; anything else
// corrupt Parquet data (CHECK-5).
func (p *part) dataError(ctx context.Context, physical int, err error) error {
	var unsupported *UnsupportedError
	var refusal *RefusalError
	switch {
	case ctx.Err() != nil, errors.As(err, &unsupported), errors.As(err, &refusal), errors.Is(err, ErrLimitExceeded):
		return err
	case isEnvironment(ctx, err):
		return fmt.Errorf("seekparquet: part %s row group %d: %w", p.name, physical, err)
	}
	return refusePart(CodeInvalidParquet, p.name, physical, "%v", err)
}

// columnPages reads the pages of one value column chunk forward, for one
// request, holding the page it decoded last: rows selected in ascending
// order, over one call or several, decode each page at most once, and only
// that page and the chunk's dictionary are retained. Through the
// OffsetIndex it reads only the pages holding selected rows, each in one
// read of the extent the index gives it; without an OffsetIndex (COL-4) it
// walks the page headers from the chunk's start, decoding only pages
// holding selected rows. The dictionary is read when a page needs it, or,
// for a planned scan, before the first page, as ProjectedBytes counted it.
type columnPages struct {
	c *chunk
	// locs are the chunk's pages, nil when it has no OffsetIndex.
	locs []pageLoc
	rows int64 // the row group's row count
	// values are the page decoded last, holding rows from first on.
	values []parquet.Value
	first  int64
	// off and walked are the walk's position: the offset of the next page
	// header, and the first row of that page.
	off, walked int64
	// planned reads the dictionary before the first page.
	planned bool
}

// openColumn opens the chunk of the named column in a physical row group,
// with its OffsetIndex when it has one; nil when the part lacks the column
// (COL-2).
func (p *part) openColumn(ctx context.Context, physical int, name string, st *ReadStats, scratch *pageScratch) (*columnPages, error) {
	col, ok := p.leaves[name]
	if !ok {
		return nil, nil
	}
	c, err := p.openChunk(ctx, physical, col, st, &st.ValuePagesDecoded)
	if err != nil {
		return nil, err
	}
	// Even a projected key keeps the non-reusing key policy.
	if col.index != p.key.index {
		c.scratch = scratch
	}
	cp := &columnPages{c: c, rows: p.footer.Metadata().RowGroups[physical].NumRows, off: c.start}
	slot := &p.locs[physical][col.index]
	if locs := slot.Load(); locs != nil {
		cp.locs = *locs
	} else {
		index, err := p.loadOffsetIndex(ctx, physical, col.index, st)
		if err != nil {
			return nil, c.corrupt(fmt.Errorf("the OffsetIndex: %w", err))
		}
		if index != nil {
			if cp.locs, err = checkLocations(index, cp.rows, c.start, c.end); err != nil {
				return nil, c.corrupt(err)
			}
			p.r.keepLocations(slot, cp.locs)
		}
	}
	if cp.locs != nil {
		c.first = cp.locs[0].offset
	}
	return cp, nil
}

// keepLocations publishes a chunk's checked page locations to slot, for
// later requests to share read-only, when Limits.OffsetIndexMemory has
// room for them; otherwise they serve this request alone. Only checked
// locations reach it: an OffsetIndex that fails to load or check is never
// kept, so every request that touches it is refused. A request that finds
// the locations kept makes no index read, so its SourceReads and
// SourceBytes leave out the index bytes an earlier request read.
func (r *reader) keepLocations(slot *atomic.Pointer[[]pageLoc], locs []pageLoc) {
	size := int64(len(locs))*int64(unsafe.Sizeof(pageLoc{})) + 64
	for {
		used := r.locsMemory.Load()
		if size > r.limits.OffsetIndexMemory-used {
			return
		}
		if r.locsMemory.CompareAndSwap(used, used+size) {
			break
		}
	}
	if !slot.CompareAndSwap(nil, &locs) {
		r.locsMemory.Add(-size) // a concurrent first use published first
	}
}

// dictionaryBytes returns the extent of the chunk's dictionary page when
// the OffsetIndex locates the first data page after the chunk's start.
func (cp *columnPages) dictionaryBytes() int64 { return cp.c.first - cp.c.start }

// pages returns the pages holding rows [from, to) of the row group, which
// has an OffsetIndex.
func (cp *columnPages) pages(from, to int64) (first, last int) {
	return pageAt(cp.locs, from), pageAt(cp.locs, to-1)
}

// read hands the selected rows to emit in order. selected is non-empty,
// ascending, after every row of an earlier call, and confined to this
// physical row group.
func (cp *columnPages) read(selected []position, emit func(int, parquet.Value)) error {
	for i, pos := range selected {
		if err := cp.load(pos.row); err != nil {
			return err
		}
		emit(i, cp.values[pos.row-cp.first])
	}
	return nil
}

// load retains the page containing row until a later load moves to another
// page. Calls must move forward, as read does. Keeping the loading boundary
// separate lets a batch borrow several columns' pages simultaneously.
func (cp *columnPages) load(row int64) error {
	c := cp.c
	for row < cp.first || row >= cp.first+int64(len(cp.values)) {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if cp.locs != nil {
			if cp.planned && !c.dictRead && cp.dictionaryBytes() > 0 {
				if _, err := c.dictionary(); err != nil {
					return err
				}
			}
			i := pageAt(cp.locs, row)
			values, err := c.indexed(i, cp.locs[i])
			if err != nil {
				return err
			}
			cp.values, cp.first = values, cp.locs[i].first
			continue
		}
		if cp.off >= c.end {
			return c.corrupt(fmt.Errorf("the chunk holds %d of its row group's %d rows", cp.walked, cp.rows))
		}
		h, length, raw, err := c.header(cp.off)
		if err != nil {
			return err
		}
		pageRows, isData := dataRows(h)
		// Each data page holds rows, all inside the row group (CHECK-5): a
		// page of none, or one past the group's end, does not add up.
		if isData && (pageRows <= 0 || pageRows > cp.rows-cp.walked) {
			return c.corrupt(fmt.Errorf("a page of %d rows at row %d of a %d-row group", pageRows, cp.walked, cp.rows))
		}
		if isData && cp.walked+pageRows > row {
			data, err := c.payload(h, cp.off, length, raw)
			if err != nil {
				return err
			}
			values, err := c.decode(h, data)
			if err != nil {
				return err
			}
			if int64(len(values)) != pageRows {
				return c.corrupt(fmt.Errorf("a page declares %d rows and holds %d", pageRows, len(values)))
			}
			cp.values, cp.first = values, cp.walked
		}
		cp.walked += pageRows
		cp.off += length + int64(h.CompressedPageSize)
	}
	return nil
}

// decodeBooleanRLEInto accepts only final bit-packed padding (at most seven bits).
// out already has ceil(n/8) bytes, bounded by the caller before allocation.
func decodeBooleanRLEInto(out, data []byte, n int) ([]byte, error) {
	if err := checkCounts(format.RLE, data, n, false); err != nil {
		return nil, err
	}
	src := data[4 : 4+binary.LittleEndian.Uint32(data)]
	clear(out)
	at := 0
	for len(src) > 0 {
		h, k := binary.Uvarint(src)
		src = src[k:]
		count := int(h >> 1)
		packed := h&1 != 0
		size := 1
		if packed {
			size = count
			count *= 8
		}
		if count == 0 || at >= n || (!packed && count > n-at) || (packed && count > n-at && len(src) != size) {
			return nil, errors.New("invalid Boolean run count or padding")
		}
		for i := 0; i < count && at+i < n; i++ {
			bit := src[0] & 1
			if packed {
				bit = (src[i/8] >> uint(i%8)) & 1
			}
			out[(at+i)/8] |= bit << uint((at+i)%8)
		}
		at += count
		src = src[size:]
	}
	if at < n {
		return nil, errors.New("Boolean stream holds fewer values than the page")
	}
	return out, nil
}
