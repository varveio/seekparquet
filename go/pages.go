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
	ctx        context.Context
	p          *part
	physical   int
	col        leaf
	codec      format.CompressionCodec
	start, end int64
	// code refuses a page its index entry does not describe:
	// invalid_page_index for the key column (RG-3, RG-7), invalid_parquet
	// for a value column (CHECK-5).
	code     Code
	dict     parquet.Dictionary
	dictRead bool
	stats    *stats
}

// openChunk checks a column chunk before any of its pages is read: a codec
// this reader decodes (else unsupported_encoding) and a byte range inside
// the file (else invalid_footer).
func (p *part) openChunk(ctx context.Context, physical int, col leaf, st *stats) (*chunk, error) {
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
	return &chunk{ctx: ctx, p: p, physical: physical, col: col, codec: meta.Codec, start: start, end: end, code: code, stats: st}, nil
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
	return c.p.pageHeader(c.ctx, c.physical, c.col.index, off, c.end)
}

// payload returns the page data after a header of length n at off, from
// raw when it holds them, after checking its checksum when it has one.
func (c *chunk) payload(h *format.PageHeader, off, n int64, raw []byte) ([]byte, error) {
	size := int64(h.CompressedPageSize)
	var data []byte
	if int64(len(raw)) >= n+size {
		data = raw[n : n+size]
	} else {
		data = make([]byte, size)
		if err := readFull(c.p.at(c.ctx), data, off+n); err != nil {
			return nil, c.corrupt(err)
		}
	}
	if h.CRC != 0 && uint32(h.CRC) != crc32.ChecksumIEEE(data) {
		return nil, c.corrupt(errors.New("the page fails its checksum"))
	}
	return data, nil
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
	h, n, raw, err := c.header(loc.offset)
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
		if data, err = decompress(c.codec, payload, int(h.UncompressedPageSize)); err != nil {
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
		if !d.IsCompressed.Valid || d.IsCompressed.V {
			if data, err = decompress(c.codec, data, int(h.UncompressedPageSize)-rep-def); err != nil {
				return nil, c.corrupt(err)
			}
		}
	}
	c.stats.pages++
	if !c.col.optional {
		return c.values(enc, data, n)
	}
	if err := checkRuns(levels, n, 1); err != nil {
		return nil, c.corrupt(fmt.Errorf("definition levels: %w", err))
	}
	defined, err := (&rle.Encoding{BitWidth: 1}).DecodeLevels(make([]byte, 0, n+8), levels)
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
	out := make([]parquet.Value, n)
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
	out := make([]parquet.Value, n)
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
		indexes, err := (&rle.Encoding{BitWidth: width}).DecodeInt32(make([]int32, 0, n+8), data[1:])
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
// decoded values to hold n of them.
func (c *chunk) decodeValues(enc format.Encoding, data []byte, n int) (encoding.Values, error) {
	var none encoding.Values
	if !supportedEncoding(enc) {
		return none, &UnsupportedError{Code: UnsupportedEncoding, Detail: "encoding " + enc.String() + " in part " + c.p.name}
	}
	if err := checkCounts(enc, data, n); err != nil {
		return none, c.corrupt(err)
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
		offsets = make([]uint32, 0, n+1)
	}
	decoded, err := typ.Decode(typ.NewValues(make([]byte, 0, size), offsets), data, e)
	if err != nil {
		return none, c.corrupt(err)
	}
	if held(typ, &decoded) < int64(n) {
		return none, c.corrupt(fmt.Errorf("the page decodes to fewer than its %d values", n))
	}
	return decoded, nil
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
// dictionary page that begins the chunk, decoded on first use.
func (c *chunk) dictionary() (parquet.Dictionary, error) {
	if c.dictRead {
		return c.dict, nil
	}
	c.dictRead = true
	h, n, raw, err := c.header(c.start)
	if err != nil {
		return nil, err
	}
	if h.Type != format.DictionaryPage || !h.DictionaryPageHeader.Valid {
		if c.p.footer.Metadata().RowGroups[c.physical].Columns[c.col.index].MetaData.DictionaryPageOffset != 0 {
			return nil, c.corrupt(errors.New("no dictionary page at its dictionary offset"))
		}
		return nil, nil
	}
	d := &h.DictionaryPageHeader.V
	if d.Encoding != format.Plain && d.Encoding != format.PlainDictionary {
		return nil, &UnsupportedError{Code: UnsupportedEncoding, Detail: "a dictionary encoded " + d.Encoding.String() + " in part " + c.p.name}
	}
	payload, err := c.payload(h, c.start, n, raw)
	if err != nil {
		return nil, err
	}
	data, err := decompress(c.codec, payload, int(h.UncompressedPageSize))
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
// to hold at most n values, allowing the padding of a final bit-packed
// group, without decoding it.
func checkRuns(src []byte, n, width int) error {
	limit := uint64(n) + 7 // the values, and the padding of a final bit-packed group
	total := uint64(0)
	for len(src) > 0 {
		h, k := binary.Uvarint(src)
		if k <= 0 {
			return errors.New("a run header is malformed")
		}
		src = src[k:]
		count, size := h>>1, uint64(width+7)/8 // a run of one repeated value
		if h&1 == 1 { // bit-packed groups of eight values
			if count > limit/8 {
				return fmt.Errorf("the runs hold more than the page's %d values", n)
			}
			count, size = 8*count, count*uint64(width)
		}
		if count > limit-total {
			return fmt.Errorf("the runs hold more than the page's %d values", n)
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
	if _, k := binary.Varint(src); k <= 0 { // the first value
		return nil, errors.New("the delta header is malformed")
	} else {
		src = src[k:]
	}
	block, miniblocks, count := header[0], header[1], header[2]
	if block == 0 || block%128 != 0 || miniblocks == 0 || block%miniblocks != 0 || (block/miniblocks)%32 != 0 {
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
func (p *part) pageHeader(ctx context.Context, physical, column int, off, end int64) (*format.PageHeader, int64, []byte, error) {
	lim := &p.r.limits
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidParquet, p.name, physical, "column chunk %d page at %d: "+format, append([]any{column, off}, args...)...)
	}
	if off < 0 || off >= end {
		return nil, 0, nil, bad("outside the column chunk")
	}
	avail := end - off
	var header format.PageHeader
	var length int
	var raw []byte
	// A header is read in growing pieces up to 16 MiB, far beyond any
	// real header; a small page arrives with its header.
	ceiling := min(avail, 16<<20, lim.MetadataMemory)
	for want := int64(4 << 10); ; want *= 16 {
		n := min(want, ceiling)
		raw = make([]byte, n)
		if err := readFull(p.at(ctx), raw, off); err != nil {
			if isEnvironment(ctx, err) {
				return nil, 0, nil, err
			}
			return nil, 0, nil, bad("%v", err)
		}
		header = format.PageHeader{}
		var err error
		length, err = decodeBounded(raw, &header, lim.MetadataMemory)
		if errors.Is(err, ErrLimitExceeded) {
			return nil, 0, nil, fmt.Errorf("part %s: page header: %w", p.name, err)
		}
		if err == nil {
			break
		}
		if n == ceiling {
			return nil, 0, nil, bad("the page header does not decode: %v", err)
		}
	}
	if header.CompressedPageSize < 0 || header.UncompressedPageSize < 0 || int64(length)+int64(header.CompressedPageSize) > avail {
		return nil, 0, nil, bad("the page sizes are invalid")
	}
	if size := int64(max(header.CompressedPageSize, header.UncompressedPageSize)); size > lim.PageBytes {
		return nil, 0, nil, exceeded("part %s: a page of %d bytes (Limits.PageBytes %d)", p.name, size, lim.PageBytes)
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
			return nil, 0, nil, bad("the data page header is invalid")
		}
	case format.DictionaryPage:
		h, _ := header.DictionaryPageHeader.Get()
		values = int64(h.NumValues)
	}
	if values < 0 {
		return nil, 0, nil, bad("the value count is negative")
	}
	if values > lim.PageValues {
		return nil, 0, nil, exceeded("part %s: a page of %d values (Limits.PageValues %d)", p.name, values, lim.PageValues)
	}
	return &header, int64(length), raw, nil
}

// loadOffsetIndex returns the OffsetIndex of a column chunk, nil when it
// has none. Its encoded size is bounded by Limits.PageIndexBytes and its
// decoding like a footer's.
func (p *part) loadOffsetIndex(ctx context.Context, physical, column int) (*format.OffsetIndex, error) {
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
	return &index, nil
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

// readColumn hands n rows of a value column of a physical row group from
// row on to emit, a page at a time. Through the chunk's OffsetIndex it
// reads only the pages holding them, each checked against its entry;
// without one (COL-4) it walks the page headers from the chunk's start,
// reading and decoding only the pages holding them.
func (p *part) readColumn(ctx context.Context, physical int, col leaf, row int64, n int, st *stats, emit func([]parquet.Value)) error {
	c, err := p.openChunk(ctx, physical, col, st)
	if err != nil {
		return err
	}
	rows := p.footer.Metadata().RowGroups[physical].NumRows
	index, err := p.loadOffsetIndex(ctx, physical, col.index)
	if err != nil {
		return c.corrupt(fmt.Errorf("the OffsetIndex: %w", err))
	}
	end := row + int64(n)
	clip := func(values []parquet.Value, first int64) []parquet.Value {
		return values[max(row-first, 0):min(end-first, int64(len(values)))]
	}
	if index != nil {
		locs, err := checkLocations(index, rows, c.start, c.end)
		if err != nil {
			return c.corrupt(err)
		}
		for i := pageAt(locs, row); i < len(locs) && locs[i].first < end; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			values, err := c.indexed(i, locs[i])
			if err != nil {
				return err
			}
			emit(clip(values, locs[i].first))
		}
		return nil
	}
	first := int64(0)
	for off := c.start; first < end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if off >= c.end {
			return c.corrupt(fmt.Errorf("the chunk holds %d of its row group's %d rows", first, rows))
		}
		h, length, raw, err := c.header(off)
		if err != nil {
			return err
		}
		pageRows, isData := dataRows(h)
		if isData && first+pageRows > row {
			data, err := c.payload(h, off, length, raw)
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
			emit(clip(values, first))
		}
		first += pageRows
		off += length + int64(h.CompressedPageSize)
	}
	return nil
}
