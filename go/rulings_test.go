// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

func TestRulingsOptionalMetadata(t *testing.T) {
	for _, key := range append([]string{"ordinary", "seekparquet.future"}, stampKeys[:]...) {
		for _, duplicate := range []bool{false, true} {
			t.Run(key+map[bool]string{false: "", true: "/duplicate"}[duplicate], func(t *testing.T) {
				dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
				raw, err := os.ReadFile(partFile(dir, 0))
				must(t, err)
				n := int(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
				var wire footerMetadata
				must(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw[len(raw)-8-n:len(raw)-8], &wire))
				listed := false
				for i := range wire.KeyValueMetadata {
					if wire.KeyValueMetadata[i].Key == key {
						listed = true
						if !duplicate {
							wire.KeyValueMetadata[i].Value.Valid = false
						}
					}
				}
				if duplicate || !listed {
					wire.KeyValueMetadata = append(wire.KeyValueMetadata, footerKeyValue{Key: key})
				}
				encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &wire)
				must(t, err)
				replaceFooter(t, dir, encoded)
				d, err := OpenDir(ctx, dir, OpenOptions{})
				must(t, err)
				defer d.Close()
				rows, err := d.Rows(ctx, Query{Limit: 10}, nil)
				if listed {
					if RefusalCodeOf(err) != CodeStampMismatch {
						t.Fatalf("got %v, want stamp_mismatch", err)
					}
				} else if err != nil || len(rows) != 2 {
					t.Fatalf("ignored metadata: %d rows, %v", len(rows), err)
				}
			})
		}
	}
	// Key remains required even though Value is optional.
	var kv footerKeyValue
	if _, err := decodeBounded([]byte{0x28, 0, 0}, &kv, 1024); err == nil {
		t.Fatal("missing Key accepted")
	}
}

func TestRulingsIndexedHeaders(t *testing.T) {
	cases := []struct {
		name string
		edit func(*format.PageHeader)
		code RefusalCode
	}{
		{"negative V1 count", func(h *format.PageHeader) { h.DataPageHeader.V.NumValues = -5 }, CodeInvalidParquet},
		{"negative compressed size", func(h *format.PageHeader) { h.CompressedPageSize = -1 }, CodeInvalidParquet},
		{"negative uncompressed size", func(h *format.PageHeader) { h.UncompressedPageSize = -1 }, CodeInvalidParquet},
		{"negative V2 count", func(h *format.PageHeader) {
			h.Type = format.DataPageV2
			h.DataPageHeaderV2 = thrift.New(format.DataPageHeaderV2{NumValues: -5, NumRows: 2})
		}, CodeInvalidParquet},
		{"negative V2 level length", func(h *format.PageHeader) {
			h.Type = format.DataPageV2
			h.DataPageHeaderV2 = thrift.New(format.DataPageHeaderV2{NumValues: 2, NumRows: 2, DefinitionLevelsByteLength: -1})
		}, CodeInvalidParquet},
		{"V2 levels past payload", func(h *format.PageHeader) {
			h.Type = format.DataPageV2
			h.DataPageHeaderV2 = thrift.New(format.DataPageHeaderV2{NumValues: 2, NumRows: 2, DefinitionLevelsByteLength: 17})
		}, CodeInvalidParquet},
		{"intrinsic before extent", func(h *format.PageHeader) { h.DataPageHeader.V.NumValues = -5; h.CompressedPageSize = 1000 }, CodeInvalidParquet},
		{"extent past chunk", func(h *format.PageHeader) { h.CompressedPageSize = 1000 }, CodeInvalidPageIndex},
		{"extent short", func(h *format.PageHeader) { h.CompressedPageSize = 1 }, CodeInvalidPageIndex},
		{"valid row mismatch", func(h *format.PageHeader) { h.DataPageHeader.V.NumValues = 3 }, CodeInvalidPageIndex},
		{"dictionary location", func(h *format.PageHeader) {
			h.Type = format.DictionaryPage
			h.DictionaryPageHeader = thrift.New(format.DictionaryPageHeader{NumValues: 2, Encoding: format.Plain})
		}, CodeInvalidPageIndex},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := v1Page(2, make([]byte, 16))
			tc.edit(h)
			raw, err := thrift.Marshal(new(thrift.CompactProtocol), h)
			must(t, err)
			raw = append(raw, make([]byte, 16)...)
			c := testChunk(parquet.Int64Type)
			c.code = CodeInvalidPageIndex
			c.p.file = bytes.NewReader(raw)
			c.end = int64(len(raw))
			_, _, err = c.indexedPage(0, pageLoc{offset: 0, size: int64(len(raw)), rows: 2})
			if RefusalCodeOf(err) != tc.code {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
	t.Run("undecodable", func(t *testing.T) {
		c := testChunk(parquet.Int64Type)
		c.code = CodeInvalidPageIndex
		c.p.file = bytes.NewReader([]byte{0xff})
		c.end = 1
		_, _, err := c.indexedPage(0, pageLoc{size: 1, rows: 2})
		if RefusalCodeOf(err) != CodeInvalidPageIndex {
			t.Fatalf("got %v", err)
		}
	})
}

func TestRulingsAnnotations(t *testing.T) {
	for _, tc := range []struct {
		name              string
		typ               format.Type
		length, precision int32
		logical           bool
		valid             bool
	}{
		{"int32 ceiling", format.Int32, 0, 10, true, false}, {"int64 ceiling", format.Int64, 0, 19, true, false},
		{"flba ceiling", format.FixedLenByteArray, 1, 3, true, false}, {"int32 legal", format.Int32, 0, 9, true, true},
		{"int64 legal", format.Int64, 0, 18, true, true}, {"flba legal", format.FixedLenByteArray, 1, 2, true, true},
		{"converted ceiling", format.Int64, 0, 19, false, false}, {"byte array unbounded", format.ByteArray, 0, 100, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			se := format.SchemaElement{Type: thrift.New(tc.typ), TypeLength: thrift.New(tc.length), Name: "value"}
			if tc.logical {
				se.LogicalType.Value = &format.DecimalType{Precision: tc.precision}
			} else {
				se.ConvertedType = thrift.New(deprecated.Decimal)
				se.Precision = thrift.New(tc.precision)
			}
			if err := checkLeaf(&se); (err == nil) != tc.valid {
				t.Fatalf("checkLeaf: %v, valid %v", err, tc.valid)
			}
		})
	}
	for _, typ := range []format.Type{format.Int64, format.FixedLenByteArray, format.Int96} {
		se := format.SchemaElement{Type: thrift.New(typ), TypeLength: thrift.New(int32(12)), Name: "ts", LogicalType: format.LogicalType{Value: &format.TimestampType{Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}}}
		if err := checkLeaf(&se); (err == nil) != (typ != format.Int96) {
			t.Fatalf("TIMESTAMP on %s: %v", typ, err)
		}
	}
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "unprojected", true: "nested unprojected"}[nested], func(t *testing.T) {
			var dir string
			if nested {
				dir = nestedDataset(t, testutil.Part{})
			} else {
				dir = write(t, parts(testutil.Part{}, []string{"a", "b"}))
			}
			rewrite(t, dir, 0, func(m *format.FileMetaData) {
				se := &m.Schema[len(m.Schema)-1]
				se.Type = thrift.New(format.Int64)
				se.ConvertedType.Valid = false
				se.LogicalType.Value = &format.DecimalType{Precision: 19}
			})
			d, err := OpenDir(ctx, dir, OpenOptions{})
			must(t, err)
			defer d.Close()
			_, err = d.Rows(ctx, Query{Limit: 10}, nil)
			if RefusalCodeOf(err) != CodeInvalidFooter {
				t.Fatalf("first touch: %v", err)
			}
		})
	}
}

func TestRulingsUnknownAndEmptyUnits(t *testing.T) {
	// Required unit present with an unknown alternative, versus an empty union.
	for _, raw := range [][]byte{{0x11, 0x1c, 0x4c, 0, 0, 0}, {0x11, 0x1c, 0, 0}} {
		var ts format.TimestampType
		_, err := decodeBounded(raw, &ts, 1024, reflect.TypeFor[format.TimeUnit]())
		unknown := len(raw) > 4
		if (err == nil) != unknown {
			t.Fatalf("raw %x: %v", raw, err)
		}
		if unknown {
			se := format.SchemaElement{Type: thrift.New(format.Int64), Name: "ts", LogicalType: format.LogicalType{Value: &ts}}
			must(t, checkLeaf(&se))
		}
	}
}

func TestRulingsEnums(t *testing.T) {
	// Unknown value codecs must not affect a query that only needs keys.
	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[1].MetaData.Codec = 99 })
	d := open(t, dir, OpenOptions{})
	rows, err := d.Rows(ctx, Query{Limit: 10}, nil)
	must(t, err)
	if len(rows) != 2 {
		t.Fatalf("rows: %d", len(rows))
	}
	_, err = d.Rows(ctx, Query{Limit: 10, Columns: []string{"size"}}, nil)
	var u *UnsupportedError
	if !errors.As(err, &u) || u.Code != UnsupportedEncoding {
		t.Fatalf("projected codec: %v", err)
	}
	dir = write(t, parts(testutil.Part{}, []string{"a", "b"}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Codec = 99 })
	keyCodec := open(t, dir, OpenOptions{})
	rows, err = keyCodec.Rows(ctx, Query{Limit: 10, Selection: Selection{Span: Span{Start: []byte("z")}}}, nil)
	must(t, err)
	if len(rows) != 0 {
		t.Fatal("query beyond the part returned rows")
	}
	_, err = keyCodec.Rows(ctx, Query{Limit: 10}, nil)
	if !errors.As(err, &u) || u.Code != UnsupportedEncoding {
		t.Fatalf("needed key codec: %v", err)
	}

	for _, v2 := range []bool{false, true} {
		c := testChunk(parquet.Int64Type)
		h := v1Page(2, make([]byte, 16))
		h.DataPageHeader.V.Encoding = 99
		if v2 {
			h.Type = format.DataPageV2
			h.DataPageHeaderV2 = thrift.New(format.DataPageHeaderV2{NumValues: 2, NumRows: 2, Encoding: 99})
		}
		var u *UnsupportedError
		if _, err := c.decode(h, make([]byte, 16)); !errors.As(err, &u) || u.Code != UnsupportedEncoding {
			t.Fatalf("unknown encoding v2=%v: %v", v2, err)
		}
	}
	// Repetition encoding is required but its numeric value is unused here.
	h := v1Page(2, make([]byte, 16))
	h.DataPageHeader.V.RepetitionLevelEncoding = 99
	if values, err := testChunk(parquet.Int64Type).decode(h, make([]byte, 16)); err != nil || len(values) != 2 {
		t.Fatalf("ignored repetition: %v, %v", values, err)
	}
	dir = write(t, parts(testutil.Part{}, []string{"a", "b"}))
	rewriteIndexes(t, dir, 0, 0, func(ci *format.ColumnIndex, _ *format.OffsetIndex) { ci.BoundaryOrder = 99 })
	rows, err = open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 10}, nil)
	must(t, err)
	if len(rows) != 2 {
		t.Fatalf("ignored boundary: %d rows", len(rows))
	}
	// Ignoring numeric enum values does not waive required field presence.
	var dataHeader format.DataPageHeader
	if _, err := decodeBounded([]byte{0x15, 4, 0x15, 0, 0x15, 6, 0}, &dataHeader, 1024); err == nil {
		t.Fatal("missing repetition encoding accepted")
	}
	type noBoundary struct {
		NullPages []bool   `thrift:"1,required"`
		MinValues [][]byte `thrift:"2,required"`
		MaxValues [][]byte `thrift:"3,required"`
	}
	raw, err := thrift.Marshal(new(thrift.CompactProtocol), &noBoundary{NullPages: []bool{false}, MinValues: [][]byte{[]byte("a")}, MaxValues: [][]byte{[]byte("b")}})
	must(t, err)
	var ci format.ColumnIndex
	if _, err := decodeBounded(raw, &ci, 1024); err == nil {
		t.Fatal("missing boundary_order accepted")
	}

}

func TestRulingsPhysicalFallback(t *testing.T) {
	type row struct {
		Key []byte   `parquet:"key"`
		Ts  [12]byte `parquet:"ts"`
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: []row{{Key: []byte("a"), Ts: [12]byte{1}}}}}})
	rewrite(t, dir, 0, func(m *format.FileMetaData) {
		m.Schema[2].LogicalType.Value = &format.TimestampType{Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}
	})
	rows, err := open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 10, Columns: []string{"ts"}}, nil)
	must(t, err)
	if len(rows) != 1 || !bytes.Equal(rows[0].Values[0].Bytes(), []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("TIMESTAMP FLBA12: %+v", rows)
	}
	for _, empty := range []bool{false, true} {
		dir = write(t, parts(testutil.Part{}, []string{"a", "b"}))
		rewrite(t, dir, 0, func(m *format.FileMetaData) {
			m.Schema[2].LogicalType.Value = &format.TimestampType{Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}
		})
		raw, err := os.ReadFile(partFile(dir, 0))
		must(t, err)
		n := int(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
		footer := raw[len(raw)-8-n : len(raw)-8]
		// TIMESTAMP(false, MICROS), with unit changed to an unknown member
		// or an empty required union. The whole footer length is repaired.
		old := []byte{0x8c, 0x12, 0x1c, 0x2c, 0, 0, 0}
		next := []byte{0x8c, 0x12, 0x1c, 0x4c, 0, 0, 0}
		if empty {
			next = []byte{0x8c, 0x12, 0x1c, 0, 0}
		}
		if !bytes.Contains(footer, old) {
			t.Fatal("timestamp unit not found")
		}
		replaceFooter(t, dir, bytes.Replace(footer, old, next, 1))
		d, err := OpenDir(ctx, dir, OpenOptions{})
		must(t, err)
		rows, err = d.Rows(ctx, Query{Limit: 10, Columns: []string{"size"}}, nil)
		must(t, d.Close())
		if empty {
			if RefusalCodeOf(err) != CodeInvalidFooter {
				t.Fatalf("empty unit: %v", err)
			}
		} else if err != nil || len(rows) != 2 {
			t.Fatalf("unknown unit: %v, %v", rows, err)
		}
	}
	dir = write(t, parts(testutil.Part{}, []string{"a", "b"}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.Schema[2].ConvertedType = thrift.New(deprecated.ConvertedType(99)) })
	rows, err = open(t, dir, OpenOptions{}).Rows(ctx, Query{Limit: 10, Columns: []string{"size"}}, nil)
	must(t, err)
	if len(rows) != 2 {
		t.Fatal("unknown converted annotation lost rows")
	}
}

func TestRulingsUnknownLogicalAnnotation(t *testing.T) {
	for _, column := range []int{1, 2} {
		dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
		rewrite(t, dir, 0, func(m *format.FileMetaData) {
			m.Schema[column].LogicalType.Value = &format.NullType{}
			if column == 2 {
				m.Schema[column].ConvertedType = thrift.New(deprecated.Int64)
			}
		})
		raw, err := os.ReadFile(partFile(dir, 0))
		must(t, err)
		n := int(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
		footer := raw[len(raw)-8-n : len(raw)-8]
		known := []byte{0xbc, 0, 0}            // LogicalType.UNKNOWN (NullType)
		unknown := []byte{0x0c, 0xc6, 1, 0, 0} // a future logical alternative, field 99
		if bytes.Count(footer, known) != 1 {
			t.Fatal("logical annotation not found exactly once")
		}
		replaceFooter(t, dir, bytes.Replace(footer, known, unknown, 1))
		d, err := OpenDir(ctx, dir, OpenOptions{})
		must(t, err)
		defer d.Close()
		rows, err := d.Rows(ctx, Query{Limit: 10, Columns: []string{"size"}}, nil)
		if column == 1 {
			if RefusalCodeOf(err) != CodeInvalidKeyColumn {
				t.Fatalf("unknown key annotation: %v", err)
			}
		} else {
			must(t, err)
			if len(rows) != 2 {
				t.Fatal("lost physical value rows")
			}
			columns, err := d.Columns(ctx, 0)
			must(t, err)
			if columns[1].Logical != (Logical{Kind: LogicalInt, BitWidth: 64, Signed: true}) {
				t.Fatalf("legacy fallback: %+v", columns[1].Logical)
			}
		}
	}
}
