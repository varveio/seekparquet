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
	scratch    *pageScratch // nil: allocating key/dictionary path
	ctx        context.Context
	p          *part
	physical   int
	col        leaf
	codec      format.CompressionCodec
	start, end int64
	// code refuses a page its index entry does not describe:
	// invalid_page_index for the key column (RG-3, RG-7), invalid_parquet
	// for a value column (CHECK-5).
	code Code
	// first is the offset of the chunk's first data page when an
	// OffsetIndex gives it, so that what precedes it is the dictionary's
	// extent; zero when not known.
	first    int64
	dict     parquet.Dictionary
	dictRead bool
	stats    *ReadStats
	pages    *int64 // the counter of the data pages decoded: key or value pages
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
func (c *chunk) located(off, size int64) (*format.PageHeader, int64, []byte, error) {
	if lim := &c.p.r.limits; size > lim.PageBytes+lim.headerBytes() {
		return c.header(off)
	}
	return c.p.pageExtent(c.ctx, c.physical, c.col.index, off, size, c.end, c.stats, c.scratch)
}

// payload returns the page data after a header of length n at off, from
// raw when it holds them, after checking the CRC its header carries, when
// it carries one (zero included).
func (c *chunk) payload(h *format.PageHeader, off, n int64, raw []byte) ([]byte, error) {
	size := int64(h.CompressedPageSize)
	var data []byte
	if int64(len(raw)) >= n+size {
		data = raw[n : n+size]
	} else {
		data = c.scratch.readBuffer(int(size))
		if err := readFull(c.p.at(c.ctx, c.stats), data, off+n); err != nil {
			return nil, c.corrupt(err)
		}
	}
	if h.CRC != 0 || c.hasCRC(off, n, raw) {
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
		if err := readFull(c.p.at(c.ctx, c.stats), raw, off); err != nil {
			return false // the header was read before; a failing reread is not the page's
		}
	}
	var f crcField
	return thrift.Unmarshal(new(thrift.CompactProtocol), raw[:n], &f) == nil && f.CRC != nil
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
	bad := func(format string, args ...any) error {
		return refusePart(c.code, c.p.name, c.physical, "column chunk %d page %d: "+format, append([]any{c.col.index, i}, args...)...)
	}
	h, n, raw, err := c.located(loc.offset, loc.size)
	if err != nil {
		if CodeOf(err) == CodeInvalidParquet {
			return nil, bad("its OffsetIndex entry does not locate a page header")
		}
		return nil, err
	}
	rows, ok := dataRows(h)
	switch {
	case !ok:
		return nil, bad("its OffsetIndex entry locates a page of type %s, not a data page", h.Type)
	case n+int64(h.CompressedPageSize) != loc.size:
		return nil, bad("its OffsetIndex entry does not locate exactly one page")
	case rows != loc.rows:
		return nil, bad("the page holds %d rows, its OffsetIndex entry %d", rows, loc.rows)
	}
	data, err := c.payload(h, loc.offset, n, raw)
	if err != nil {
		return nil, err
	}
	return c.decode(h, data)
}

// decode decodes a data page: one value per row, null where the
// definition level is zero.
func (c *chunk) decode(h *format.PageHeader, payload []byte) (_ []parquet.Value, err error) {
	defer func() {
		// A last line of defence: a panic inside parquet-go's decoders is
		// reported as corrupt data. The checks before decoding are the
		// protection; this is not a substitute for them.
		if r := recover(); r != nil {
			err = c.corrupt(fmt.Errorf("parquet-go failed on a page: %v", r))
		}
	}()
	var n int
	var enc format.Encoding
	var levels, data []byte
	switch h.Type {
	case format.DataPage:
		d := &h.DataPageHeader.V
		n, enc = int(d.NumValues), d.Encoding
		if data, err = c.scratch.decompress(c.codec, payload, int(h.UncompressedPageSize)); err != nil {
			return nil, c.corrupt(err)
		}
		if c.col.optional {
			if d.DefinitionLevelEncoding != format.RLE {
				return nil, &UnsupportedError{Code: UnsupportedEncoding, Detail: "definition levels encoded " + d.DefinitionLevelEncoding.String() + " in part " + c.p.name}
			}
			if len(data) < 4 || int64(binary.LittleEndian.Uint32(data)) > int64(len(data)-4) {
				return nil, c.corrupt(errors.New("the definition levels exceed the page"))
			}
			m := 4 + int(binary.LittleEndian.Uint32(data))
			levels, data = data[4:m], data[m:]
		}
	case format.DataPageV2:
		d := &h.DataPageHeaderV2.V
		n, enc = int(d.NumValues), d.Encoding
		if d.NumRows != d.NumValues {
			return nil, c.corrupt(errors.New("a page of a non-repeated column holds a different number of rows and values"))
		}
		rep, def := int(d.RepetitionLevelsByteLength), int(d.DefinitionLevelsByteLength)
		levels, data = payload[rep:rep+def], payload[rep+def:]
		codec := c.codec
		if d.IsCompressed.Valid && !d.IsCompressed.V {
			codec = format.Uncompressed
		}
		if data, err = c.scratch.decompress(codec, data, int(h.UncompressedPageSize)-rep-def); err != nil {
			return nil, c.corrupt(err)
		}
	}
	if !c.col.optional {
		values, err := c.values(enc, data, n)
		if err == nil {
			*c.pages++
		}
		return values, err
	}
	if err := checkRuns(levels, n, 1); err != nil {
		return nil, c.corrupt(fmt.Errorf("definition levels: %w", err))
	}
	s := c.scratch
	if s == nil {
		s = new(pageScratch)
	}
	s.levels = resize(s.levels, n+8)
	defined, err := (&rle.Encoding{BitWidth: 1}).DecodeLevels(s.levels[:0:n+8], levels)
	s.levels = decodedBuffer(s.levels, defined)
	if err != nil || len(defined) < n {
		return nil, c.corrupt(errors.New("the page holds fewer definition levels than values"))
	}
	defined = defined[:n]
	present := 0
	for _, level := range defined {
		present += int(level)
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
	s := c.scratch
	if s == nil {
		s = new(pageScratch)
	}
	// Drop pointers into the previous page/dictionary, including a long page's
	// unused tail, before decoding a smaller page or another chunk.
	s.clearValues()
	s.values = resize(s.values, n)
	out := s.values
	if enc == format.PlainDictionary || enc == format.RLEDictionary {
		dict, err := c.dictionary()
		if err != nil {
			return nil, err
		}
		if dict == nil {
			return nil, c.corrupt(errors.New("a dictionary-encoded page in a chunk without a dictionary"))
		}
		if len(data) == 0 || data[0] > 32 {
			return nil, c.corrupt(errors.New("the dictionary indexes have no valid bit width"))
		}
		width := int(data[0])
		if err := checkRuns(data[1:], n, width); err != nil {
			return nil, c.corrupt(fmt.Errorf("dictionary indexes: %w", err))
		}
		s.indexes = resize(s.indexes, n+8)
		indexes, err := (&rle.Encoding{BitWidth: width}).DecodeInt32(s.indexes[:0:n+8], data[1:])
		s.indexes = decodedBuffer(s.indexes, indexes)
		if err != nil || len(indexes) < n {
			return nil, c.corrupt(errors.New("the page holds fewer dictionary indexes than values"))
		}
		indexes = indexes[:n]
		for _, i := range indexes {
			if i < 0 || int(i) >= dict.Len() {
				return nil, c.corrupt(fmt.Errorf("dictionary index %d is outside the dictionary of %d values", i, dict.Len()))
			}
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

// decodeValues decodes n values encoded with enc, after checking the value
// counts the encoding declares and the decoded size, and requires the
// decoded values to hold exactly n of them.
func (c *chunk) decodeValues(enc format.Encoding, data []byte, n int) (encoding.Values, error) {
	var none encoding.Values
	s := c.scratch
	if s == nil {
		s = new(pageScratch)
	}
	if !supportedEncoding(enc) {
		return none, &UnsupportedError{Code: UnsupportedEncoding, Detail: "encoding " + enc.String() + " in part " + c.p.name}
	}
	if err := checkCounts(enc, data, n); err != nil {
		return none, c.corrupt(err)
	}
	// The dependency's Boolean hybrid decoder copies a repeated bit as a byte,
	// which turns a run of true into true,false,... . Expand into a bounded
	// bitmap ourselves, including runs that begin between byte boundaries.
	if c.col.typ.Kind() == parquet.Boolean && enc == format.RLE {
		size := (n + 7) / 8
		if int64(size) > c.p.r.limits.PageBytes {
			return none, exceeded("Boolean page decoding to %d bytes", size)
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
		return none, exceeded("part %s: a page decoding to %d bytes (Limits.PageBytes %d)", c.p.name, size, limit)
	}
	var offsets []uint32
	if typ.Kind() == parquet.ByteArray {
		s.offsets = resize(s.offsets, n+1)
		offsets = s.offsets[: 0 : n+1]
	}
	s.data = resize(s.data, size)
	decoded, err := typ.Decode(typ.NewValues(s.data[:0:size], offsets), data, e)
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
func (c *chunk) dictionary() (parquet.Dictionary, error) {
	if c.dictRead {
		return c.dict, nil
	}
	c.dictRead = true
	// A lazy read must not overwrite the active data page or its decode buffers.
	dictionaryChunk := *c
	dictionaryChunk.scratch = nil
	var err error
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
		h, n, raw, err = c.located(c.start, c.first-c.start)
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

// checkCounts checks the value counts an encoding declares inside a page
// against the n values the page header declares, before a decoder
// allocates for them.
func checkCounts(enc format.Encoding, data []byte, n int) error {
	switch enc {
	case format.RLE: // booleans: a 4-byte length, then runs of one bit
		if len(data) < 4 || int64(binary.LittleEndian.Uint32(data)) > int64(len(data)-4) {
			return errors.New("the RLE length exceeds the page")
		}
		return checkRuns(data[4:4+binary.LittleEndian.Uint32(data)], n, 1)
	case format.DeltaBinaryPacked, format.DeltaLengthByteArray:
		_, err := deltaStream(data, n)
		return err
	case format.DeltaByteArray: // prefix lengths, then suffix lengths
		rest, err := deltaStream(data, n)
		if err != nil {
			return err
		}
		_, err = deltaStream(rest, n)
		return err
	}
	return nil
}

// checkRuns requires an RLE/bit-packed hybrid stream of the given bit width
// to hold at most n values, without decoding it: a repeated run may not
// hold more values than remain, and a bit-packed run only the padding of
// its final group of eight; no run with values may follow the nth. A run
// of no values holds nothing and is stepped over (parquet-go writes one
// for a page of no indexes, and decodes one anywhere).
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
		total += count
		src = src[size:]
	}
	return nil
}

// deltaStream checks the header of a DELTA_BINARY_PACKED stream, requires
// its value count to be at most n, and returns the bytes after its blocks.
func deltaStream(src []byte, n int) ([]byte, error) {
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
			if w > 64 {
				return nil, errors.New("a delta miniblock bit width is invalid")
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
		h, n, err := p.decodeHeader(raw, end-off, int64(len(raw)) == ceiling, bad)
		if h != nil || err != nil {
			return h, n, raw, err
		}
	}
}

// pageExtent reads the size bytes at off that a page index locates as one
// page, header included, in a single read, and decodes and checks its
// header as pageHeader does, in a column chunk ending at end. The caller
// bounds size (chunk.located).
func (p *part) pageExtent(ctx context.Context, physical, column int, off, size, end int64, st *ReadStats, scratch *pageScratch) (*format.PageHeader, int64, []byte, error) {
	bad := p.badPage(physical, column, off)
	if off < 0 || off >= end || size > end-off {
		return nil, 0, nil, bad("outside the column chunk")
	}
	raw, err := p.readPage(ctx, off, size, st, bad, scratch)
	if err != nil {
		return nil, 0, nil, err
	}
	h, n, err := p.decodeHeader(raw, end-off, true, bad)
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
	if err := readFull(p.at(ctx, st), raw, off); err != nil {
		if isEnvironment(ctx, err) {
			return nil, err
		}
		return nil, bad("%v", err)
	}
	return raw, nil
}

// decodeHeader decodes the page header at the start of raw and checks it
// against the limits and against avail, the bytes from the header to the
// end of its column chunk. A header that does not decode from raw is
// refused when raw is all there is to read (final), and otherwise reported
// by a nil header and error, so that the caller reads more.
func (p *part) decodeHeader(raw []byte, avail int64, final bool, bad func(string, ...any) error) (*format.PageHeader, int64, error) {
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
		return nil, 0, exceeded("part %s: a page of %d values (Limits.PageValues %d)", p.name, values, lim.PageValues)
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
	var index format.OffsetIndex
	if _, err := decodeBounded(raw, &index, p.r.limits.MetadataMemory); err != nil {
		return nil, err
	}
	return &index, nil
}

// readIndexBytes reads a page index of length bytes at off, after checking
// that it lies in the file and within Limits.PageIndexBytes.
func (p *part) readIndexBytes(ctx context.Context, physical int, off int64, length int32, st *ReadStats) ([]byte, error) {
	if off < 0 || length <= 0 || off > p.size-int64(length) {
		return nil, refusePart(CodeInvalidPageIndex, p.name, physical, "a page index lies outside the file")
	}
	if int64(length) > p.r.limits.PageIndexBytes {
		return nil, exceeded("part %s: a page index of %d bytes (Limits.PageIndexBytes %d)", p.name, length, p.r.limits.PageIndexBytes)
	}
	raw := make([]byte, length)
	if err := readFull(p.at(ctx, st), raw, off); err != nil {
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
	scratch.clearValues()
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
	index, err := p.loadOffsetIndex(ctx, physical, col.index, st)
	if err != nil {
		return nil, c.corrupt(fmt.Errorf("the OffsetIndex: %w", err))
	}
	if index != nil {
		if cp.locs, err = checkLocations(index, cp.rows, c.start, c.end); err != nil {
			return nil, c.corrupt(err)
		}
		c.first = cp.locs[0].offset
	}
	return cp, nil
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
	c := cp.c
	for next := 0; next < len(selected); {
		row := selected[next].row
		if row >= cp.first && row < cp.first+int64(len(cp.values)) {
			emit(next, cp.values[row-cp.first])
			next++
			continue
		}
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
// checkCounts bounds declared counts before any output allocation.
func decodeBooleanRLEInto(out, data []byte, n int) ([]byte, error) {
	if err := checkCounts(format.RLE, data, n); err != nil {
		return nil, err
	}
	src := data[4 : 4+binary.LittleEndian.Uint32(data)]
	out = resize(out, (n+7)/8)
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
