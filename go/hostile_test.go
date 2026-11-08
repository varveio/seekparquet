// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/pierrec/lz4/v4"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestSortingColumnIndexes: a sorting_columns entry naming no column made
// parquet-go index out of range while opening the footer. Such footers are
// refused before parquet-go sees them, through both open paths.
func TestSortingColumnIndexes(t *testing.T) {
	cases := []struct {
		name  string
		code  Code
		apply func(m *format.FileMetaData)
	}{
		{"first entry negative", CodeMissingSortingColumns, func(m *format.FileMetaData) { m.RowGroups[0].SortingColumns[0].ColumnIdx = -1 }},
		{"first entry beyond the leaves", CodeMissingSortingColumns, func(m *format.FileMetaData) { m.RowGroups[0].SortingColumns[0].ColumnIdx = 3 }},
		{"extra entry invalid", CodeInvalidFooter, func(m *format.FileMetaData) {
			m.RowGroups[0].SortingColumns = append(m.RowGroups[0].SortingColumns, format.SortingColumn{ColumnIdx: 99})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
			rewrite(t, dir, 0, c.apply)
			if err := openErr(dir, Options{}); CodeOf(err) != c.code {
				t.Fatalf("OpenDir: %v, want %s", err, c.code)
			}
			if _, err := openPartsOf(t, dir, 1, unique, false, Options{}); CodeOf(err) != c.code {
				t.Fatalf("openParts: %v, want %s", err, c.code)
			}
		})
	}
}

// TestHostileFooters refuses footers that would make parquet-go panic or
// allocate without bound, before it sees them.
func TestHostileFooters(t *testing.T) {
	cases := []struct {
		name  string
		apply func(m *format.FileMetaData)
	}{
		{"too few column chunks", func(m *format.FileMetaData) { m.RowGroups[0].Columns = m.RowGroups[0].Columns[:1] }},
		{"schema claims more children", func(m *format.FileMetaData) { m.Schema[0].NumChildren.V = 9 }},
		{"schema with elements outside the tree", func(m *format.FileMetaData) { m.Schema[0].NumChildren.V = 1 }},
		{"leaf without a type", func(m *format.FileMetaData) { m.Schema[2].Type.Valid = false }},
		{"leaf with an unknown type", func(m *format.FileMetaData) { m.Schema[2].Type.V = 42 }},
		{"decimal on BOOLEAN", func(m *format.FileMetaData) {
			m.Schema[2].Type.V = format.Boolean
			m.Schema[2].LogicalType.Value = &format.DecimalType{Precision: 5, Scale: 2}
		}},
		{"FIXED_LEN_BYTE_ARRAY without a length", func(m *format.FileMetaData) {
			m.Schema[2].Type.V = format.FixedLenByteArray
			m.Schema[2].TypeLength.Valid = false
		}},
		{"STRING on INT64", func(m *format.FileMetaData) { m.Schema[2].LogicalType.Value = &format.StringType{} }},
		{"bad repetition", func(m *format.FileMetaData) { m.Schema[2].RepetitionType.V = 7 }},
		{"negative row count", func(m *format.FileMetaData) { m.RowGroups[0].NumRows = -1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
			rewrite(t, dir, 0, c.apply)
			if err := openErr(dir, Options{}); CodeOf(err) != CodeInvalidFooter {
				t.Fatalf("got %v, want invalid_footer", err)
			}
		})
	}
}

// replaceFooter writes raw as part 0's footer, keeping its body.
func replaceFooter(t *testing.T, dir string, raw []byte) {
	t.Helper()
	file, err := os.ReadFile(partFile(dir, 0))
	must(t, err)
	n := binary.LittleEndian.Uint32(file[len(file)-8:])
	body := file[:len(file)-8-int(n)]
	body = append(body, raw...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(raw)))
	body = append(body, "PAR1"...)
	must(t, os.WriteFile(partFile(dir, 0), body, 0o644))
	must(t, testutil.SyncSizes(dir))
}

// TestFooterAllocation: a few footer bytes declaring two billion schema
// elements are refused as malformed without allocating for them, and a
// well-formed footer whose decoded form would exceed MetadataMemory fails
// with ErrLimitExceeded, as does one longer than FooterBytes.
func TestFooterAllocation(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	// Field 2 (schema), a list of structs with a varint size of 2^31-1.
	replaceFooter(t, dir, []byte{0x29, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00})
	if err := openErr(dir, Options{}); CodeOf(err) != CodeInvalidFooter {
		t.Fatalf("huge list count: %v", err)
	}
	// 100,000 empty schema elements: 100 KB of input, megabytes decoded.
	raw := []byte{0x29, 0xfc}
	raw = binary.AppendUvarint(raw, 100_000)
	raw = append(raw, bytes.Repeat([]byte{0}, 100_000)...)
	raw = append(raw, 0)
	replaceFooter(t, dir, raw)
	if err := openErr(dir, Options{Limits: Limits{MetadataMemory: 1 << 20}}); !errors.Is(err, ErrLimitExceeded) || CodeOf(err) != "" {
		t.Fatalf("decoded size: %v", err)
	}
	if err := openErr(dir, Options{Limits: Limits{FooterBytes: 1000}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("footer length: %v", err)
	}
}

// TestLimits: each limit fails the operation with ErrLimitExceeded, which
// is not a refusal; the defaults read the same dataset.
func TestLimits(t *testing.T) {
	keys := fixtureKeys(400)
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 512}, split(keys, 2)...))
	read := func(lim Limits) error {
		d, err := OpenDir(ctx, dir, Options{Limits: lim})
		if err != nil {
			return err
		}
		defer d.Close()
		_, err = d.Range(ctx, Range{Limit: 1000, Columns: []string{"size", "name"}})
		return err
	}
	must(t, read(Limits{}))
	for name, lim := range map[string]Limits{
		"manifest bytes":   {ManifestBytes: 10},
		"parts":            {Parts: 1},
		"footer bytes":     {FooterBytes: 100},
		"metadata memory":  {MetadataMemory: 1000},
		"page index bytes": {PageIndexBytes: 8},
		"page bytes":       {PageBytes: 64},
		"page values":      {PageValues: 2},
		"index memory":     {IndexMemory: 64},
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(lim); !errors.Is(err, ErrLimitExceeded) || CodeOf(err) != "" {
				t.Fatalf("%+v: %v", lim, err)
			}
		})
	}
}

// TestFixedLengthConsistency: a column stored as FIXED_LEN_BYTE_ARRAY of
// different lengths in two parts is inconsistent (COL-5), top-level or
// nested.
func TestFixedLengthConsistency(t *testing.T) {
	type four struct {
		Key []byte  `parquet:"key"`
		Fx  [4]byte `parquet:"fx"`
	}
	type eight struct {
		Key []byte  `parquet:"key"`
		Fx  [8]byte `parquet:"fx"`
	}
	type nested4 struct {
		Key []byte `parquet:"key"`
		G   struct {
			Fx [4]byte `parquet:"fx"`
		} `parquet:"g"`
	}
	type nested8 struct {
		Key []byte `parquet:"key"`
		G   struct {
			Fx [8]byte `parquet:"fx"`
		} `parquet:"g"`
	}
	for name, ds := range map[string]testutil.Dataset{
		"top-level": {Parts: []testutil.Part{{Rows: []four{{Key: []byte("a")}}}, {Rows: []eight{{Key: []byte("b")}}}}},
		"nested":    {Parts: []testutil.Part{{Rows: []nested4{{Key: []byte("a")}}}, {Rows: []nested8{{Key: []byte("b")}}}}},
	} {
		if err := openErr(write(t, ds), Options{}); CodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("%s: %v", name, err)
		}
	}
	same := testutil.Dataset{Parts: []testutil.Part{{Rows: []four{{Key: []byte("a")}}}, {Rows: []four{{Key: []byte("b")}}}}}
	d := open(t, write(t, same), Options{})
	if rows, err := d.Range(ctx, Range{Limit: 2, Columns: []string{"fx"}}); err != nil || len(rows) != 2 {
		t.Fatalf("%d rows, %v", len(rows), err)
	}
}

// TestColumnOrdersCardinality: column_orders must have exactly one entry
// per leaf (KEY-5).
func TestColumnOrdersCardinality(t *testing.T) {
	for name, apply := range map[string]func(m *format.FileMetaData){
		"only the key's": func(m *format.FileMetaData) { m.ColumnOrders = m.ColumnOrders[:1] },
		"one too many":   func(m *format.FileMetaData) { m.ColumnOrders = append(m.ColumnOrders, m.ColumnOrders[0]) },
		"none":           func(m *format.FileMetaData) { m.ColumnOrders = nil },
	} {
		dir := write(t, parts(testutil.Part{}, []string{"a"}))
		rewrite(t, dir, 0, apply)
		if err := openErr(dir, Options{}); CodeOf(err) != CodeInvalidKeyColumn {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// readCounter counts the reads of the files a Source opens.
type readCounter struct {
	Source
	reads atomic.Int64
}

type countedReads struct {
	SourceFile
	reads *atomic.Int64
}

func (f countedReads) ReadAt(p []byte, off int64) (int, error) {
	f.reads.Add(1)
	return f.SourceFile.ReadAt(p, off)
}

func (s *readCounter) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return countedReads{f, &s.reads}, nil
}

// TestOpenReads counts the reads of opening: the manifest only, whatever
// the parts; Verify then reads each part's footer and every row group's
// key page index and first page. Both read the same rows afterwards.
func TestOpenReads(t *testing.T) {
	keys := fixtureKeys(2000)
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 512}, split(keys, 4)...))
	reads := func(verify bool) (int64, *Dataset) {
		src := &readCounter{Source: dirOf(t, dir)}
		d, err := OpenDataset(ctx, src, Options{})
		must(t, err)
		t.Cleanup(func() { _ = d.Close() })
		if verify {
			must(t, d.Verify(ctx))
		}
		return src.reads.Load(), d
	}
	lazy, d1 := reads(false)
	eager, d2 := reads(true)
	if lazy != 1 {
		t.Fatalf("a lazy open of 4 parts made %d reads, want the manifest's one", lazy)
	}
	if eager < lazy+4*3+20*3 {
		t.Fatalf("Verify made %d reads, the open %d: it did not read every footer and row group", eager, lazy)
	}
	r1, err1 := d1.Range(ctx, Range{Start: []byte(keys[1234]), Limit: 50})
	r2, err2 := d2.Range(ctx, Range{Start: []byte(keys[1234]), Limit: 50})
	if err1 != nil || err2 != nil || len(r1) == 0 || keysOf(r1)[0] != keys[1234] || len(r1) != len(r2) {
		t.Fatalf("%v %v", err1, err2)
	}
}

// allocated returns the bytes f allocates.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestDecompressIsBounded decompresses 32 MiB of zeros, a few kilobytes
// compressed, with each codec, into a page that declares 4 KiB: it is
// refused without allocating beyond a small multiple of the declaration,
// and the true size decompresses.
func TestDecompressIsBounded(t *testing.T) {
	zeros := make([]byte, 32<<20)
	compress := map[format.CompressionCodec]func() []byte{
		format.Snappy: func() []byte { return snappy.Encode(nil, zeros) },
		format.Zstd: func() []byte {
			enc, _ := zstd.NewWriter(nil)
			defer enc.Close()
			return enc.EncodeAll(zeros, nil)
		},
		format.Gzip: func() []byte {
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			_, _ = w.Write(zeros)
			_ = w.Close()
			return b.Bytes()
		},
		format.Brotli: func() []byte {
			var b bytes.Buffer
			w := brotli.NewWriterOptions(&b, brotli.WriterOptions{Quality: 1, LGWin: 16})
			_, _ = w.Write(zeros)
			_ = w.Close()
			return b.Bytes()
		},
		format.Lz4Raw: func() []byte {
			out := make([]byte, lz4.CompressBlockBound(len(zeros)))
			n, _ := lz4.CompressBlock(zeros, out, nil)
			return out[:n]
		},
	}
	if _, err := zstdDecoder(); err != nil { // allocate the shared decoder first
		t.Fatal(err)
	}
	for codec, encode := range compress {
		t.Run(codec.String(), func(t *testing.T) {
			src := encode()
			var err error
			if n := allocated(func() { _, err = decompress(codec, src, 4096) }); n > 2<<20 {
				t.Fatalf("a 4 KiB page allocated %d bytes", n)
			}
			if err == nil {
				t.Fatal("a page decompressing beyond its declared size was accepted")
			}
			if _, err := decompress(codec, src, len(zeros)+1); err == nil {
				t.Fatal("a page decompressing to less than its declared size was accepted")
			}
			if out, err := decompress(codec, src, len(zeros)); err != nil || len(out) != len(zeros) {
				t.Fatalf("the true size: %d bytes, %v", len(out), err)
			}
		})
	}
	// A snappy block declaring 2 GiB in six bytes.
	if _, err := decompress(format.Snappy, []byte{0x80, 0x80, 0x80, 0x80, 0x08, 0x00}, 4096); err == nil {
		t.Fatal("a snappy block declaring more than the page was accepted")
	}
}

// testChunk returns a chunk of an in-memory part for decoding page
// payloads directly.
func testChunk(typ parquet.Type) *chunk {
	r := &reader{limits: Limits{}.withDefaults()}
	return &chunk{ctx: ctx, p: &part{r: r, name: "test"}, col: leaf{index: 1, typ: typ}, codec: format.Uncompressed, code: CodeInvalidParquet, stats: new(ReadStats), pages: new(int64)}
}

// TestEncodedCountsAreChecked decodes small page payloads whose encodings
// declare far more values, or decode to far more bytes, than the page
// header: each is refused before a decoder allocates for it.
func TestEncodedCountsAreChecked(t *testing.T) {
	uvarint := func(v uint64) []byte { return binary.AppendUvarint(nil, v) }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	delta := func(values ...int32) []byte {
		out, err := parquet.DeltaBinaryPacked.EncodeInt32(nil, values)
		must(t, err)
		return out
	}
	// A DELTA_BINARY_PACKED header: blocks of 128 values in 4 miniblocks,
	// 2^31-1 values, first value 0.
	hugeDelta := cat(uvarint(128), uvarint(4), uvarint(1<<31-1), []byte{0})
	cases := []struct {
		name  string
		typ   parquet.Type
		enc   format.Encoding
		data  []byte
		limit bool // ErrLimitExceeded rather than invalid_parquet
	}{
		{"delta count", parquet.Int64Type, format.DeltaBinaryPacked, hugeDelta, false},
		{"delta length count", parquet.ByteArrayType, format.DeltaLengthByteArray, hugeDelta, false},
		{"delta suffix count", parquet.ByteArrayType, format.DeltaByteArray, cat(delta(0, 0), hugeDelta), false},
		{"delta block geometry", parquet.Int32Type, format.DeltaBinaryPacked, cat(uvarint(100), uvarint(3), uvarint(2), []byte{0}), false},
		{"boolean run", parquet.BooleanType, format.RLE, cat([]byte{6, 0, 0, 0}, uvarint(1<<31), []byte{1}), false},
		{"prefix lengths beyond the limit", parquet.ByteArrayType, format.DeltaByteArray, cat(delta(1<<28, 1<<28), delta(0, 0)), true},
		{"prefix lengths overflowing", parquet.ByteArrayType, format.DeltaByteArray, cat(delta(1<<30, 1<<30), delta(0, 0)), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error
			if n := allocated(func() { _, err = testChunk(c.typ).decodeValues(c.enc, c.data, 10) }); n > 1<<20 {
				t.Fatalf("allocated %d bytes", n)
			}
			if c.limit && !errors.Is(err, ErrLimitExceeded) || !c.limit && CodeOf(err) != CodeInvalidParquet {
				t.Fatalf("got %v", err)
			}
		})
	}
	// Dictionary indexes: a run of 2^30 indexes, and bit-packed groups of
	// width 0, in a few bytes.
	c := testChunk(parquet.Int64Type)
	one, err := c.decodeValues(format.Plain, make([]byte, 8), 1)
	must(t, err)
	c.dict, c.dictRead = parquet.Int64Type.NewDictionary(1, 1, one), true
	for name, data := range map[string][]byte{
		"repeated run": cat([]byte{1}, uvarint(1<<31), []byte{0}),
		"packed run":   cat([]byte{0}, uvarint(1<<29|1)),
		"index range":  cat([]byte{1}, uvarint(10<<1), []byte{1}),
	} {
		t.Run("dictionary "+name, func(t *testing.T) {
			if _, err := c.values(format.RLEDictionary, data, 10); CodeOf(err) != CodeInvalidParquet {
				t.Fatalf("got %v", err)
			}
		})
	}
	if v, err := c.values(format.RLEDictionary, cat([]byte{1}, uvarint(10<<1), []byte{0}), 10); err != nil || len(v) != 10 {
		t.Fatalf("a valid run: %d values, %v", len(v), err)
	}
}

// editPageHeader decodes the page header at off in the file at path,
// applies change, and writes it back in place; the encoded header must
// keep its length.
func editPageHeader(t *testing.T, path string, off int64, change func(*format.PageHeader)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	must(t, err)
	var h format.PageHeader
	n, err := decodeBounded(raw[off:], &h, 1<<20)
	must(t, err)
	change(&h)
	encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &h)
	must(t, err)
	if len(encoded) != n {
		t.Fatalf("the edited header is %d bytes, the original %d", len(encoded), n)
	}
	copy(raw[off:], encoded)
	must(t, os.WriteFile(path, raw, 0o644))
}

// TestPageDeclarationsAreChecked edits the uncompressed size a dictionary
// page and a data page declare, so that their payloads decompress to more
// or fewer bytes than declared: reading them is invalid_parquet.
func TestPageDeclarationsAreChecked(t *testing.T) {
	type dictRow struct {
		Key  []byte `parquet:"key"`
		Name string `parquet:"name,dict"`
		Size int64  `parquet:"size"`
	}
	var rows []dictRow
	for i := range 40 {
		rows = append(rows, dictRow{Key: []byte{'k', byte('a' + i/26), byte('a' + i%26)}, Name: []string{"x", "y"}[i%2], Size: int64(i)})
	}
	for _, codec := range []parquet.WriterOption{parquet.Compression(&parquet.Gzip), parquet.Compression(&parquet.Uncompressed)} {
		for _, c := range []struct {
			name, column string
			delta        int32
		}{
			{"dictionary page larger than declared", "name", -1},
			{"dictionary page smaller than declared", "name", 1},
			{"data page larger than declared", "size", -1},
		} {
			t.Run(c.name, func(t *testing.T) {
				dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, Compression: codec}}})
				meta, err := testutil.ReadFooter(partFile(dir, 0))
				must(t, err)
				column := map[string]int{"name": 1, "size": 2}[c.column]
				chunk := meta.RowGroups[0].Columns[column].MetaData
				off := chunk.DataPageOffset
				if c.column == "name" {
					if chunk.DictionaryPageOffset == 0 {
						t.Fatal("the name column has no dictionary")
					}
					off = chunk.DictionaryPageOffset
				}
				editPageHeader(t, partFile(dir, 0), off, func(h *format.PageHeader) { h.UncompressedPageSize += c.delta })
				d := open(t, dir, Options{})
				if _, err := d.Range(ctx, Range{Limit: 100, Columns: []string{c.column}}); CodeOf(err) != CodeInvalidParquet {
					t.Fatalf("got %v, want invalid_parquet", err)
				}
				if rows, err := d.Range(ctx, Range{Limit: 100}); err != nil || len(rows) != 40 {
					t.Fatalf("keys only: %d rows, %v", len(rows), err)
				}
			})
		}
	}
}

// TestCorruptValueOffsetIndex edits a value column's OffsetIndex so that
// an entry claims rows or bytes its page does not hold: a read of those
// rows is refused with invalid_parquet instead of decoding past the page
// the entry describes.
func TestCorruptValueOffsetIndex(t *testing.T) {
	var keys []string
	for i := range 200 {
		keys = append(keys, string([]byte{'k', byte('a' + i/26), byte('a' + i%26)}))
	}
	edits := map[string]func(index *format.OffsetIndex){
		"rows":   func(index *format.OffsetIndex) { index.PageLocations[1].FirstRowIndex++ },
		"size":   func(index *format.OffsetIndex) { index.PageLocations[0].CompressedPageSize-- },
		"offset": func(index *format.OffsetIndex) { index.PageLocations[1].Offset++ },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{PageBytes: 64}, keys))
			meta, err := testutil.ReadFooter(partFile(dir, 0))
			must(t, err)
			chunk := meta.RowGroups[0].Columns[1] // size
			raw, err := os.ReadFile(partFile(dir, 0))
			must(t, err)
			at := raw[chunk.OffsetIndexOffset : chunk.OffsetIndexOffset+int64(chunk.OffsetIndexLength)]
			var index format.OffsetIndex
			must(t, thrift.Unmarshal(new(thrift.CompactProtocol), at, &index))
			if len(index.PageLocations) < 3 {
				t.Fatalf("the size column has %d pages", len(index.PageLocations))
			}
			edit(&index)
			encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &index)
			must(t, err)
			if len(encoded) != len(at) {
				t.Fatalf("the edited index is %d bytes, the original %d", len(encoded), len(at))
			}
			copy(at, encoded)
			must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
			d := open(t, dir, Options{})
			if _, err := d.Range(ctx, Range{Limit: 200, Columns: []string{"size"}}); CodeOf(err) != CodeInvalidParquet {
				t.Fatalf("got %v, want invalid_parquet", err)
			}
		})
	}
}
