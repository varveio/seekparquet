// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// The tests in this file come from differential testing of the Go and Java
// readers on malformed pages and footers: each covers an input on which the
// Go reader crashed, returned bytes that are not the page's, or answered
// differently from the Java reader.

// TestRLEValuesOnlyForBooleans: Parquet defines RLE values only for
// Booleans. A page of another type labelled RLE was decoded by parquet-go
// as runs of bit width 0, where the PLAIN INT32 values 0, -1, 15 read as a
// bit-packed run of 2^31-1 groups occupying no bytes: the process ran out
// of memory. It is invalid_parquet before anything is decoded.
func TestRLEValuesOnlyForBooleans(t *testing.T) {
	payload := []byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f, 0, 0, 0}
	for _, typ := range []parquet.Type{parquet.Int32Type, parquet.Int64Type, parquet.FloatType, parquet.DoubleType,
		parquet.ByteArrayType, parquet.FixedLenByteArrayType(4)} {
		if _, err := testChunk(typ).values(format.RLE, payload, 3); CodeOf(err) != CodeInvalidParquet {
			t.Errorf("%s: %v, want invalid_parquet", typ, err)
		}
	}
	// Booleans: a 4-byte length, then one repeated run of three trues.
	values, err := testChunk(parquet.BooleanType).values(format.RLE, []byte{2, 0, 0, 0, 6, 1}, 3)
	must(t, err)
	if len(values) != 3 || !values[0].Boolean() || !values[2].Boolean() {
		t.Fatalf("Booleans: %v", values)
	}
}

// TestPlainByteArrayLengthsStayInThePage: parquet-go bounds a PLAIN
// BYTE_ARRAY value's length by its whole buffer, not by the bytes after the
// value, so a last value one byte short was completed from the bytes after
// the page whenever the slice's capacity reached them (the next page's
// header, read with this one). Every length must lie inside the page.
func TestPlainByteArrayLengthsStayInThePage(t *testing.T) {
	buffer := append(plainByteArray("aaaa", "bbbb"), 0x15, 0x16) // two bytes of what follows the page
	page := buffer[:len(buffer)-3]                               // "bbbb" one byte short; capacity beyond
	if v, err := testChunk(parquet.ByteArrayType).values(format.Plain, page, 2); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("a value overrunning its page: %v, %v", v, err)
	}
	if _, err := testChunk(parquet.ByteArrayType).decodeValues(format.Plain, page, 2); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("a dictionary value overrunning its page: %v", err)
	}
	if _, err := testChunk(parquet.ByteArrayType).values(format.Plain, buffer[:len(buffer)-2], 2); err != nil {
		t.Fatalf("the exact page: %v", err)
	}
}

// valueRow is a row with a dictionary-encoded string and a PLAIN byte
// array, for pages edited in place.
type valueRow struct {
	Key  []byte `parquet:"key"`
	Name string `parquet:"name,dict"`
	Blob []byte `parquet:"blob,plain"`
	N    int64  `parquet:"n"`
}

func valueRows(n int) []valueRow {
	rows := make([]valueRow, n)
	for i := range rows {
		rows[i] = valueRow{Key: []byte{'k', byte('a' + i/26), byte('a' + i%26)}, Name: []string{"x", "yy", "zzz"}[i%3],
			Blob: bytes.Repeat([]byte{byte('a' + i%26)}, 4), N: int64(i)}
	}
	return rows
}

// lengthenLastValue adds one to the length of the last PLAIN BYTE_ARRAY
// value of the uncompressed page at off, in place, refreshing the page's
// CRC when it has one: the page keeps its size, and its last value now
// overruns it by a byte.
func lengthenLastValue(t *testing.T, path string, off int64, values int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	must(t, err)
	var h format.PageHeader
	n, err := decodeBounded(raw[off:], &h, 1<<20)
	must(t, err)
	body := raw[off+int64(n) : off+int64(n)+int64(h.CompressedPageSize)]
	if h.Type == format.DataPageV2 || h.Type == format.DataPage && h.DataPageHeader.V.Encoding != format.Plain {
		t.Fatalf("the page is encoded %s", h.DataPageHeader.V.Encoding)
	}
	at := 0
	for range values - 1 {
		at += 4 + int(binary.LittleEndian.Uint32(body[at:]))
	}
	binary.LittleEndian.PutUint32(body[at:], binary.LittleEndian.Uint32(body[at:])+1)
	must(t, os.WriteFile(path, raw, 0o644))
	if h.CRC != 0 {
		editPageHeader(t, path, off, func(h *format.PageHeader) { h.CRC = int32(crc32.ChecksumIEEE(body)) })
	}
}

// TestShortDictionaryValue: a dictionary page whose last value overruns
// it. A planned scan, and full validation, read the dictionary outside the
// recover() that page decoding has, and parquet-go's PLAIN decoder
// panicked there: the process died. Query, plan and validation are refused
// with invalid_parquet.
func TestShortDictionaryValue(t *testing.T) {
	rows := valueRows(30)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, Compression: parquet.Compression(&parquet.Uncompressed)}}})
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	chunk := meta.RowGroups[0].Columns[1].MetaData // name
	if chunk.DictionaryPageOffset == 0 {
		t.Fatal("the name column has no dictionary")
	}
	lengthenLastValue(t, partFile(dir, 0), chunk.DictionaryPageOffset, 3)
	d := open(t, dir, OpenOptions{})
	if _, err := d.Rows(ctx, Query{Limit: 100, Columns: []string{"name"}}, nil); CodeOf(err) != CodeInvalidParquet {
		t.Errorf("Range: %v", err)
	}
	plan, err := open(t, dir, OpenOptions{}).Plan(ctx, []Selection{{}}, []string{"name"}, nil)
	if err == nil {
		err = scanRows(ctx, plan, nil, func(Row) error { return nil })
	}
	if CodeOf(err) != CodeInvalidParquet {
		t.Errorf("plan: %v", err)
	}
	report, err := ValidateDir(ctx, dir, ValidateOptions{})
	must(t, err)
	if full, _ := report.Level(LevelFull); full.Result != Failed || full.Failure.Code != CodeInvalidParquet {
		t.Errorf("validation: %+v", full)
	}
}

// TestShortValueWithoutOffsetIndex: a PLAIN BYTE_ARRAY page of a column
// without an OffsetIndex (COL-4) is read with the bytes after it (the walk
// reads a header's worth), and its last value, one byte short, was
// completed with the next page header's first byte. It is invalid_parquet.
func TestShortValueWithoutOffsetIndex(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: valueRows(30), PageBytes: 64, DataPageVersion: 1,
		Compression: parquet.Compression(&parquet.Uncompressed)}}})
	rewrite(t, dir, 0, func(m *format.FileMetaData) {
		c := &m.RowGroups[0].Columns[2] // blob
		c.OffsetIndexOffset, c.OffsetIndexLength, c.ColumnIndexOffset, c.ColumnIndexLength = 0, 0, 0, 0
	})
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	off := meta.RowGroups[0].Columns[2].MetaData.DataPageOffset
	raw, err := os.ReadFile(partFile(dir, 0))
	must(t, err)
	var h format.PageHeader
	_, err = decodeBounded(raw[off:], &h, 1<<20)
	must(t, err)
	values := int(h.DataPageHeader.V.NumValues)
	if values >= 30 {
		t.Fatalf("the blob column has one page of %d values", values)
	}
	lengthenLastValue(t, partFile(dir, 0), off, values)
	rows, err := open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 100, Columns: []string{"blob"}}, nil)
	if CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("got %d rows (row %d: %q), %v; want invalid_parquet", len(rows), values-1, valueAt(rows, values-1), err)
	}
}

func valueAt(rows []Row, i int) []byte {
	if i < len(rows) && len(rows[i].Values) > 0 {
		return rows[i].Values[0].Bytes()
	}
	return nil
}

// TestRunValuesFitTheirWidth: a repeated run of an RLE/bit-packed hybrid
// stream must repeat a value of its bit width. Go's Boolean decoder kept a
// larger value's low bit (0x02 read as false) where parquet-java reads any
// non-zero value as true; both are invalid_parquet.
func TestRunValuesFitTheirWidth(t *testing.T) {
	for _, c := range []struct {
		src      []byte
		n, width int
		ok       bool
	}{
		{[]byte{2, 1}, 1, 1, true},
		{[]byte{2, 2}, 1, 1, false},
		{[]byte{2, 0xff}, 1, 1, false},
		{[]byte{0, 2, 2, 1}, 1, 1, false}, // an empty run's value too
		{[]byte{2, 0x7f}, 1, 7, true},
		{[]byte{2, 0x80}, 1, 7, false},
		{[]byte{2, 0xff, 0x01}, 1, 9, true},
		{[]byte{2, 0x00, 0x02}, 1, 9, false},
		{[]byte{2, 0xff, 0xff, 0xff, 0xff}, 1, 32, true},
		{[]byte{2}, 1, 0, true},
	} {
		if err := checkRuns(c.src, c.n, c.width); (err == nil) != c.ok {
			t.Errorf("checkRuns(%x, %d, %d): %v", c.src, c.n, c.width, err)
		}
	}
	// Booleans [true, false, true] as repeated runs of 02, 00, 02.
	if v, err := testChunk(parquet.BooleanType).values(format.RLE, []byte{6, 0, 0, 0, 2, 2, 2, 0, 2, 2}, 3); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("Boolean runs of 2: %v, %v", v, err)
	}
}

// v1Page returns a DATA_PAGE header of n values for payload.
func v1Page(n int32, payload []byte) *format.PageHeader {
	h := &format.PageHeader{Type: format.DataPage, UncompressedPageSize: int32(len(payload)), CompressedPageSize: int32(len(payload))}
	h.DataPageHeader = thrift.New(format.DataPageHeader{NumValues: n, Encoding: format.Plain, DefinitionLevelEncoding: format.RLE, RepetitionLevelEncoding: format.RLE})
	return h
}

// TestDefinitionLevelsAboveOne: a top-level optional column's levels are 0
// or 1. Go summed them to count the non-null values, so levels 2, 0, 2
// with four values read as a valid page; it is invalid_parquet.
func TestDefinitionLevelsAboveOne(t *testing.T) {
	levels := []byte{2, 2, 2, 0, 2, 2} // repeated runs of one: 2, 0, 2
	payload := append(binary.LittleEndian.AppendUint32(nil, uint32(len(levels))), levels...)
	payload = append(payload, make([]byte, 8*4)...)
	c := testChunk(parquet.Int64Type)
	c.col.optional = true
	if v, err := c.decode(v1Page(3, payload), payload); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("levels 2, 0, 2: %v, %v", v, err)
	}
}

// TestAllNullDictionaryPageWithoutIndexes: a dictionary-encoded page with
// no non-null value may omit its index stream entirely, bit width
// included; Java and PyArrow read it as all nulls.
func TestAllNullDictionaryPageWithoutIndexes(t *testing.T) {
	c := testChunk(parquet.Int64Type)
	one, err := c.decodeValues(format.Plain, make([]byte, 8), 1)
	must(t, err)
	c.dict, c.dictRead = parquet.Int64Type.NewDictionary(1, 1, one), true
	if v, err := c.values(format.RLEDictionary, nil, 0); err != nil || len(v) != 0 {
		t.Fatalf("no index for no value: %v, %v", v, err)
	}
	if _, err := c.values(format.RLEDictionary, nil, 1); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("no index for one value: %v", err)
	}
	// A whole page: three null rows, no index bytes.
	opt := testChunk(parquet.Int64Type)
	opt.col.optional = true
	opt.dict, opt.dictRead = c.dict, true
	levels := []byte{6, 0}
	payload := append(binary.LittleEndian.AppendUint32(nil, uint32(len(levels))), levels...)
	h := v1Page(3, payload)
	h.DataPageHeader.V.Encoding = format.RLEDictionary
	if v, err := opt.decode(h, payload); err != nil || len(v) != 3 || !v[0].IsNull() || !v[2].IsNull() {
		t.Fatalf("an all-null page: %v, %v", v, err)
	}
}

// TestDataPageV2Levels: a DATA_PAGE_V2 page of a top-level, non-repeated
// column declares no repetition levels, no definition levels when the
// column is REQUIRED, and as many nulls as its definition levels hold. Go
// skipped declared level bytes and ignored num_nulls; each is
// invalid_parquet, as in Java.
func TestDataPageV2Levels(t *testing.T) {
	page := func(n, nulls, rep, def int32, payload []byte) *format.PageHeader {
		h := &format.PageHeader{Type: format.DataPageV2, UncompressedPageSize: int32(len(payload)), CompressedPageSize: int32(len(payload))}
		h.DataPageHeaderV2 = thrift.New(format.DataPageHeaderV2{NumValues: n, NumNulls: nulls, NumRows: n, Encoding: format.Plain,
			DefinitionLevelsByteLength: def, RepetitionLevelsByteLength: rep})
		return h
	}
	optional := testChunk(parquet.Int64Type)
	optional.col.optional = true
	levels := []byte{3, 0b101} // one bit-packed group: 1, 0, 1
	payload := append(slices.Clone(levels), make([]byte, 16)...)
	if v, err := optional.decode(page(3, 1, 0, 2, payload), payload); err != nil || len(v) != 3 || !v[1].IsNull() {
		t.Fatalf("a valid page: %v, %v", v, err)
	}
	for _, nulls := range []int32{0, 2} {
		if _, err := optional.decode(page(3, nulls, 0, 2, payload), payload); CodeOf(err) != CodeInvalidParquet {
			t.Errorf("num_nulls %d for one null: %v", nulls, err)
		}
	}
	required := testChunk(parquet.Int64Type)
	values := make([]byte, 24)
	if _, err := required.decode(page(3, 0, 0, 0, values), values); err != nil {
		t.Fatalf("a valid required page: %v", err)
	}
	withRep := append([]byte{0}, values...)
	if _, err := required.decode(page(3, 0, 1, 0, withRep), withRep); CodeOf(err) != CodeInvalidParquet {
		t.Errorf("repetition levels: %v", err)
	}
	withDef := append([]byte{2, 0}, values...)
	if _, err := required.decode(page(3, 0, 0, 2, withDef), withDef); CodeOf(err) != CodeInvalidParquet {
		t.Errorf("definition levels of a REQUIRED column: %v", err)
	}
	if _, err := required.decode(page(3, 1, 0, 0, values), values); CodeOf(err) != CodeInvalidParquet {
		t.Errorf("a null in a REQUIRED column: %v", err)
	}
	// The optional page under an index of the column as REQUIRED (its schema
	// says so) is refused, not read as values shifted by the level bytes.
	if v, err := required.decode(page(3, 1, 0, 2, payload), payload); CodeOf(err) != CodeInvalidParquet {
		t.Errorf("levels under a REQUIRED schema: %v, %v", v, err)
	}
}

// numberRow is a row of one required INT64 column, for pages of two rows.
type numberRow struct {
	Key []byte `parquet:"key"`
	N   int64  `parquet:"n"`
}

// insertAtChunkStart inserts page bytes at the start of a column chunk's
// data pages (the chunk must have no dictionary), shifting what follows,
// and removes that chunk's page indexes, so that the reader walks its
// page headers (COL-4).
func insertAtChunkStart(t *testing.T, path string, column int, page []byte) {
	t.Helper()
	raw, err := os.ReadFile(path)
	must(t, err)
	meta, err := testutil.ReadFooter(path)
	must(t, err)
	target := &meta.RowGroups[0].Columns[column]
	at := target.MetaData.DataPageOffset
	if target.MetaData.DictionaryPageOffset != 0 {
		t.Fatal("the chunk has a dictionary")
	}
	shift := func(off *int64) {
		if *off > at {
			*off += int64(len(page))
		}
	}
	for g := range meta.RowGroups {
		for c := range meta.RowGroups[g].Columns {
			cc := &meta.RowGroups[g].Columns[c]
			shift(&cc.MetaData.DataPageOffset)
			shift(&cc.MetaData.DictionaryPageOffset)
			shift(&cc.ColumnIndexOffset)
			shift(&cc.OffsetIndexOffset)
		}
	}
	target.MetaData.TotalCompressedSize += int64(len(page))
	target.OffsetIndexOffset, target.OffsetIndexLength, target.ColumnIndexOffset, target.ColumnIndexLength = 0, 0, 0, 0
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), meta)
	must(t, err)
	footerLen := int64(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
	body := raw[:int64(len(raw))-8-footerLen]
	out := slices.Concat(body[:at], page, body[at:], footer, binary.LittleEndian.AppendUint32(nil, uint32(len(footer))), []byte("PAR1"))
	must(t, os.WriteFile(path, out, 0o644))
	dir := path[:len(path)-len(testutil.PartPath(0))]
	must(t, testutil.SyncSizes(dir))
}

// TestWalkedPagesAddUp: the pages of a column chunk without an OffsetIndex
// hold rows, all inside the row group (CHECK-5). Go's walk stepped over a
// page of no rows, and took rows from a page that ran past the group's end
// (a duplicated first page), answering with the duplicate's values.
func TestWalkedPagesAddUp(t *testing.T) {
	rows := make([]numberRow, 5)
	for i := range rows {
		rows[i] = numberRow{Key: []byte{'k', byte('a' + i)}, N: int64(i)}
	}
	build := func(t *testing.T) (string, []byte) {
		dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 16, DataPageVersion: 1,
			Compression: parquet.Compression(&parquet.Uncompressed)}}})
		meta, err := testutil.ReadFooter(partFile(dir, 0))
		must(t, err)
		raw, err := os.ReadFile(partFile(dir, 0))
		must(t, err)
		off := meta.RowGroups[0].Columns[1].MetaData.DataPageOffset
		var h format.PageHeader
		n, err := decodeBounded(raw[off:], &h, 1<<20)
		must(t, err)
		if first := h.DataPageHeader.V.NumValues; first != 2 {
			t.Fatalf("the first page holds %d rows, the test wants 2 of 5 (pages 2, 2, 1)", first)
		}
		return dir, raw[off : off+int64(n)+int64(h.CompressedPageSize)]
	}
	t.Run("a page past the group's end", func(t *testing.T) {
		dir, first := build(t)
		// Pages 2, 2, 2, 1 rows: the third (rows 4 and 5) straddles the end.
		insertAtChunkStart(t, partFile(dir, 0), 1, first)
		if got, err := open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 10, Columns: []string{"n"}}, nil); CodeOf(err) != CodeInvalidParquet {
			t.Fatalf("%d rows, %v", len(got), err)
		}
	})
	t.Run("a page of no rows", func(t *testing.T) {
		dir, _ := build(t)
		h := v1Page(0, nil)
		zero, err := thrift.Marshal(new(thrift.CompactProtocol), h)
		must(t, err)
		insertAtChunkStart(t, partFile(dir, 0), 1, zero)
		if got, err := open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 10, Columns: []string{"n"}}, nil); CodeOf(err) != CodeInvalidParquet {
			t.Fatalf("%d rows, %v", len(got), err)
		}
		report, err := ValidateDir(ctx, dir, ValidateOptions{})
		must(t, err)
		if full, _ := report.Level(LevelFull); full.Result != Failed || full.Failure.Code != CodeInvalidParquet {
			t.Fatalf("validation: %+v", full)
		}
	})
}

// TestDeltaLengthsCoverTheBytes: the lengths of a DELTA_LENGTH_BYTE_ARRAY
// page, and the suffix lengths of a DELTA_BYTE_ARRAY page, add up to
// exactly the bytes after them. With surplus bytes, parquet-go's amd64
// DELTA_BYTE_ARRAY decoder read its last values from the end of the page
// and returned the wrong ones.
func TestDeltaLengthsCoverTheBytes(t *testing.T) {
	var data []byte
	var offsets []uint32
	for i := range 8 {
		offsets = append(offsets, uint32(len(data)))
		data = append(append(data, "value-0"...), bytes.Repeat([]byte{byte('A' + i)}, 13)...)
	}
	offsets = append(offsets, uint32(len(data)))
	for _, c := range []struct {
		enc   format.Encoding
		codec interface {
			EncodeByteArray([]byte, []byte, []uint32) ([]byte, error)
		}
	}{
		{format.DeltaByteArray, &parquet.DeltaByteArray},
		{format.DeltaLengthByteArray, &parquet.DeltaLengthByteArray},
	} {
		page, err := c.codec.EncodeByteArray(nil, data, offsets)
		must(t, err)
		values, err := testChunk(parquet.ByteArrayType).values(c.enc, page, 8)
		must(t, err)
		if got := string(values[3].ByteArray()); got != string(data[offsets[3]:offsets[4]]) {
			t.Fatalf("%s: value 3 is %q", c.enc, got)
		}
		for name, edited := range map[string][]byte{"surplus bytes": append(slices.Clone(page), "XYZ"...), "a missing byte": page[:len(page)-1]} {
			if v, err := testChunk(parquet.ByteArrayType).values(c.enc, edited, 8); CodeOf(err) != CodeInvalidParquet {
				got := ""
				if len(v) > 3 {
					got = string(v[3].ByteArray())
				}
				t.Errorf("%s, %s: %v (value 3 %q)", c.enc, name, err, got)
			}
		}
	}
}

// TestDeltaBitWidthByValueType: a DELTA_BINARY_PACKED miniblock of more
// than 32 bits is corrupt in a stream of INT32 values (an INT32 column, or
// the lengths of the byte-array delta encodings), and refused before a
// size estimate trusts the garbage it decodes to; INT64 streams keep 64.
func TestDeltaBitWidthByValueType(t *testing.T) {
	stream := func(width byte, tail int) []byte {
		out := binary.AppendUvarint(nil, 128)               // block size
		out = binary.AppendUvarint(out, 4)                  // miniblocks
		out = binary.AppendUvarint(out, 2)                  // values
		out = binary.AppendVarint(out, 0)                   // first value
		out = binary.AppendVarint(out, 0)                   // the block's minimum delta
		out = append(out, width, 0, 0, 0)                   // miniblock widths
		out = append(out, make([]byte, 32*int(width)/8)...) // the first miniblock
		return append(out, make([]byte, tail)...)
	}
	if _, err := testChunk(parquet.Int32Type).values(format.DeltaBinaryPacked, stream(33, 0), 2); CodeOf(err) != CodeInvalidParquet {
		t.Errorf("INT32, width 33: %v", err)
	}
	if v, err := testChunk(parquet.Int64Type).values(format.DeltaBinaryPacked, stream(33, 0), 2); err != nil || len(v) != 2 {
		t.Errorf("INT64, width 33: %v, %v", v, err)
	}
	for _, enc := range []format.Encoding{format.DeltaLengthByteArray, format.DeltaByteArray} {
		page := stream(33, 64)
		if enc == format.DeltaByteArray {
			page = slices.Concat(stream(0, 0), page)
		}
		if _, err := testChunk(parquet.ByteArrayType).values(enc, page, 2); CodeOf(err) != CodeInvalidParquet {
			t.Errorf("%s lengths, width 33: %v", enc, err)
		}
	}
}

// TestChunkMetadataMatchesSchema: a column chunk's metadata names its leaf
// and its physical type; one that names another path or type is
// invalid_footer, as in Java, rather than read with the schema's type.
func TestChunkMetadataMatchesSchema(t *testing.T) {
	for name, change := range map[string]func(m *format.FileMetaData){
		"path_in_schema": func(m *format.FileMetaData) { m.RowGroups[0].Columns[1].MetaData.PathInSchema = []string{"nope"} },
		"type":           func(m *format.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Type = format.Int32 },
	} {
		t.Run(name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
			rewrite(t, dir, 0, change)
			if err := openErr(dir, OpenOptions{}); CodeOf(err) != CodeInvalidFooter {
				t.Fatalf("got %v, want invalid_footer", err)
			}
		})
	}
}

// TestRowGroupOrdinals: RowGroup.ordinal serves encryption, which this
// draft leaves out (FILE-2), and the specification gives it no meaning.
// parquet-go refuses ordinals that are set and differ from the row
// groups' positions; the part is read, as Java and PyArrow read it.
func TestRowGroupOrdinals(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	for name, ordinals := range map[string][]int16{
		"positions":        {0, 1},
		"all zero":         {0, 0},
		"swapped":          {1, 0},
		"one out of range": {5, 1},
		"repeated, as when an empty row group is appended by copying the last": {1, 1},
	} {
		t.Run(name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{GroupRows: 2}, keys))
			rewrite(t, dir, 0, func(m *format.FileMetaData) {
				if len(m.RowGroups) != 2 {
					t.Fatalf("%d row groups", len(m.RowGroups))
				}
				for g := range m.RowGroups {
					m.RowGroups[g].Ordinal = ordinals[g]
				}
			})
			if err := openErr(dir, OpenOptions{}); err != nil {
				t.Fatalf("open: %v", err)
			}
			rows, err := open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 10, Columns: []string{"size", "name"}}, nil)
			if err != nil || len(rows) != 4 {
				t.Fatalf("Range: %d rows, %v", len(rows), err)
			}
			if _, err := openPartsOf(t, dir, 1, unique, false, OpenOptions{}); err != nil {
				t.Fatalf("openParts: %v", err)
			}
		})
	}
}

// TestDeltaInt32SumMatchesParquetGo: the allocation-free sum of a
// DELTA_BINARY_PACKED stream of lengths equals the sum of parquet-go's own
// decoding, over streams parquet-go encodes (lengths small and large,
// deltas wrapping INT32, counts across block and miniblock boundaries),
// and a stream holding a negative value is refused.
func TestDeltaInt32SumMatchesParquetGo(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	gens := []func(i int) int32{
		func(int) int32 { return rng.Int32N(40) },
		func(int) int32 { return rng.Int32N(1 << 20) },
		func(i int) int32 { return int32(i % 7) },
		func(i int) int32 { return []int32{0, math.MaxInt32, 0, 1}[i%4] }, // deltas wrap INT32
		func(int) int32 { return math.MaxInt32 - rng.Int32N(3) },
		func(int) int32 { return rng.Int32() - math.MaxInt32/2 }, // some negative
	}
	for g, gen := range gens {
		for _, n := range []int{0, 1, 2, 31, 32, 33, 127, 128, 129, 1000, 4097} {
			values := make([]int32, n)
			for i := range values {
				values[i] = gen(i)
			}
			stream, err := parquet.DeltaBinaryPacked.EncodeInt32(nil, values)
			must(t, err)
			decoded, err := parquet.DeltaBinaryPacked.DecodeInt32(nil, stream)
			must(t, err)
			var want int64
			negative := false
			for _, v := range decoded {
				want += int64(v)
				negative = negative || v < 0
			}
			if _, err := deltaStream(stream, n, 32); err != nil {
				t.Fatalf("generator %d, %d values: %v", g, n, err)
			}
			got, err := deltaInt32Sum(stream)
			switch {
			case negative && err == nil:
				t.Fatalf("generator %d, %d values: a negative length accepted", g, n)
			case !negative && (err != nil || got != want):
				t.Fatalf("generator %d, %d values: sum %d, %v; parquet-go %d", g, n, got, err, want)
			}
		}
	}
}

// TestOffsetIndexRowsBeforeValueLimit (#44): a page its OffsetIndex locates
// with other rows than its header declares breaks RG-3 and is refused by
// its caller, even when the declared count is beyond Limits.PageValues;
// only a header that agrees with its entry, or one no entry located, is
// held to the limit and fails with ErrLimitExceeded.
func TestOffsetIndexRowsBeforeValueLimit(t *testing.T) {
	huge := int32(1<<31 - 1)
	h := format.PageHeader{Type: format.DataPage}
	h.DataPageHeader = thrift.New(format.DataPageHeader{NumValues: huge, Encoding: format.Plain})
	raw, err := thrift.Marshal(new(thrift.CompactProtocol), &h)
	must(t, err)
	p := testChunk(parquet.Int64Type).p
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidParquet, "test", 0, format, args...)
	}
	// Located by an entry of 5 rows: returned, for indexed to refuse.
	got, _, err := p.decodeHeader(raw, int64(len(raw)), true, 5, bad)
	if err != nil || got == nil {
		t.Fatalf("a header contradicting its OffsetIndex entry: %v", err)
	}
	if rows, _ := dataRows(got); rows == 5 {
		t.Fatal("the header's own rows were replaced")
	}
	// No entry, or an entry agreeing with the header: the limit applies.
	for _, rows := range []int64{-1, int64(huge)} {
		if _, _, err := p.decodeHeader(raw, int64(len(raw)), true, rows, bad); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("rows %d: %v, want ErrLimitExceeded", rows, err)
		}
	}
}
