// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil"
	"github.com/varveio/seekparquet/go/seekparquettest"
	"github.com/varveio/seekparquet/go/source/local"
)

// openPartsOf opens the parts of dir directly, without their manifest.
func openPartsOf(t *testing.T, dir string, n int, opt seekparquet.PartsOptions) (*seekparquet.Reader, error) {
	t.Helper()
	var sources []seekparquet.PartSource
	for i := range n {
		raw, err := os.ReadFile(partFile(dir, i))
		must(t, err)
		sources = append(sources, seekparquet.PartSource{Name: seekparquettest.PartPath(i), File: bytes.NewReader(raw), Size: int64(len(raw))})
	}
	r, err := seekparquet.OpenParts(ctx, sources, opt)
	if err == nil {
		t.Cleanup(func() { _ = r.Close() })
	}
	return r, err
}

// TestSortingColumnIndexes: a sorting_columns entry naming no column made
// parquet-go index out of range while opening the footer. Such footers are
// refused before parquet-go sees them, through both open paths.
func TestSortingColumnIndexes(t *testing.T) {
	cases := []struct {
		name  string
		code  seekparquet.Code
		apply func(m *format.FileMetaData)
	}{
		{"first entry negative", seekparquet.CodeMissingSortingColumns, func(m *format.FileMetaData) { m.RowGroups[0].SortingColumns[0].ColumnIdx = -1 }},
		{"first entry beyond the leaves", seekparquet.CodeMissingSortingColumns, func(m *format.FileMetaData) { m.RowGroups[0].SortingColumns[0].ColumnIdx = 3 }},
		{"extra entry invalid", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) {
			m.RowGroups[0].SortingColumns = append(m.RowGroups[0].SortingColumns, format.SortingColumn{ColumnIdx: 99})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, parts(seekparquettest.Part{}, []string{"a", "b"}))
			rewrite(t, dir, 0, c.apply)
			if _, err := openErr(dir, seekparquet.Options{}); seekparquet.CodeOf(err) != c.code {
				t.Fatalf("OpenDataset: %v, want %s", err, c.code)
			}
			if _, err := openPartsOf(t, dir, 1, seekparquet.PartsOptions{}); seekparquet.CodeOf(err) != c.code {
				t.Fatalf("OpenParts: %v, want %s", err, c.code)
			}
		})
	}
}

// TestHostileFooters refuses footers that would make parquet-go panic or
// allocate without bound, before it sees them.
func TestHostileFooters(t *testing.T) {
	cases := []struct {
		name  string
		code  seekparquet.Code
		apply func(m *format.FileMetaData)
	}{
		{"too few column chunks", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.RowGroups[0].Columns = m.RowGroups[0].Columns[:1] }},
		{"schema claims more children", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.Schema[0].NumChildren.V = 9 }},
		{"schema with elements outside the tree", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.Schema[0].NumChildren.V = 1 }},
		{"leaf without a type", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.Schema[2].Type.Valid = false }},
		{"leaf with an unknown type", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.Schema[2].Type.V = 42 }},
		{"decimal on BOOLEAN", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) {
			m.Schema[2].Type.V = format.Boolean
			m.Schema[2].LogicalType.Value = &format.DecimalType{Precision: 5, Scale: 2}
		}},
		{"FIXED_LEN_BYTE_ARRAY without a length", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) {
			m.Schema[2].Type.V = format.FixedLenByteArray
			m.Schema[2].TypeLength.Valid = false
		}},
		{"STRING on INT64", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.Schema[2].LogicalType.Value = &format.StringType{} }},
		{"bad repetition", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.Schema[2].RepetitionType.V = 7 }},
		{"negative row count", seekparquet.CodeInvalidFooter, func(m *format.FileMetaData) { m.RowGroups[0].NumRows = -1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, parts(seekparquettest.Part{}, []string{"a", "b"}))
			rewrite(t, dir, 0, c.apply)
			if _, err := openErr(dir, seekparquet.Options{}); seekparquet.CodeOf(err) != c.code {
				t.Fatalf("got %v, want %s", err, c.code)
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
	dir := write(t, parts(seekparquettest.Part{}, []string{"a", "b"}))
	// Field 2 (schema), a list of structs with a varint size of 2^31-1.
	replaceFooter(t, dir, []byte{0x29, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00})
	if _, err := openErr(dir, seekparquet.Options{}); seekparquet.CodeOf(err) != seekparquet.CodeInvalidFooter {
		t.Fatalf("huge list count: %v", err)
	}
	// 100,000 empty schema elements: 100 KB of input, megabytes decoded.
	raw := []byte{0x29, 0xfc}
	raw = binary.AppendUvarint(raw, 100_000)
	raw = append(raw, bytes.Repeat([]byte{0}, 100_000)...)
	raw = append(raw, 0)
	replaceFooter(t, dir, raw)
	_, err := openErr(dir, seekparquet.Options{Limits: seekparquet.Limits{MetadataMemory: 1 << 20}})
	if !errors.Is(err, seekparquet.ErrLimitExceeded) || seekparquet.CodeOf(err) != "" {
		t.Fatalf("decoded size: %v", err)
	}
	_, err = openErr(dir, seekparquet.Options{Limits: seekparquet.Limits{FooterBytes: 1000}})
	if !errors.Is(err, seekparquet.ErrLimitExceeded) {
		t.Fatalf("footer length: %v", err)
	}
}

// TestLimits: each limit fails the operation with ErrLimitExceeded, which
// is not a refusal; the defaults read the same dataset.
func TestLimits(t *testing.T) {
	keys := fixtureKeys(400)
	dir := write(t, parts(seekparquettest.Part{GroupRows: 100, PageBytes: 512}, split(keys, 2)...))
	read := func(lim seekparquet.Limits) error {
		d, err := local.OpenDataset(ctx, dir, seekparquet.Options{Limits: lim})
		if err != nil {
			return err
		}
		defer d.Close()
		_, err = d.Range(ctx, seekparquet.Range{Limit: 1000, Columns: []string{"size", "name"}}, nil)
		return err
	}
	must(t, read(seekparquet.Limits{}))
	for name, lim := range map[string]seekparquet.Limits{
		"manifest bytes":   {ManifestBytes: 10},
		"parts":            {Parts: 1},
		"footer bytes":     {FooterBytes: 100},
		"metadata memory":  {MetadataMemory: 1000},
		"page index bytes": {PageIndexBytes: 8},
		"page bytes":       {PageBytes: 64},
		"page values":      {PageValues: 2},
		"index memory":     {IndexMemory: 64},
	} {
		err := read(lim)
		if !errors.Is(err, seekparquet.ErrLimitExceeded) || seekparquet.CodeOf(err) != "" {
			t.Fatalf("%s: %v", name, err)
		}
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
	for name, ds := range map[string]seekparquettest.Dataset{
		"top-level": {Parts: []seekparquettest.Part{{Rows: []four{{Key: []byte("a")}}}, {Rows: []eight{{Key: []byte("b")}}}}},
		"nested":    {Parts: []seekparquettest.Part{{Rows: []nested4{{Key: []byte("a")}}}, {Rows: []nested8{{Key: []byte("b")}}}}},
	} {
		if _, err := openErr(write(t, ds), seekparquet.Options{}); seekparquet.CodeOf(err) != seekparquet.CodeInconsistentColumn {
			t.Fatalf("%s: %v", name, err)
		}
	}
	same := seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: []four{{Key: []byte("a")}}}, {Rows: []four{{Key: []byte("b")}}}}}
	d := open(t, write(t, same), seekparquet.Options{})
	if rows, err := d.Range(ctx, seekparquet.Range{Limit: 2, Columns: []string{"fx"}}, nil); err != nil || len(rows) != 2 {
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
		dir := write(t, parts(seekparquettest.Part{}, []string{"a"}))
		rewrite(t, dir, 0, apply)
		if _, err := openErr(dir, seekparquet.Options{}); seekparquet.CodeOf(err) != seekparquet.CodeInvalidKeyColumn {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// countingFiles counts the reads of the files a Source opens.
type countingFiles struct {
	seekparquet.Source
	reads atomic.Int64
}

type countedFile struct {
	seekparquet.SourceFile
	reads *atomic.Int64
}

func (f countedFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads.Add(1)
	return f.SourceFile.ReadAt(p, off)
}

// WithContext hides the wrapped file's ContextBinder, so that every read
// goes through ReadAt and is counted.
func (f countedFile) WithContext(context.Context) io.ReaderAt { return f }

func (s *countingFiles) Open(c context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return countedFile{f, &s.reads}, nil
}

// TestOpenReads counts the reads of opening: by default the manifest and
// each part's footer only, a constant per part whatever its row groups;
// with VerifyOnOpen also every row group's key page index and first page.
// Both read the same rows afterwards.
func TestOpenReads(t *testing.T) {
	keys := fixtureKeys(2000)
	dir := write(t, parts(seekparquettest.Part{GroupRows: 100, PageBytes: 512}, split(keys, 4)...))
	reads := func(verify bool) (int64, *seekparquet.Dataset) {
		src := &countingFiles{Source: local.NewDir(dir, nil)}
		d, err := seekparquet.OpenDataset(ctx, src, seekparquet.Options{VerifyOnOpen: verify})
		must(t, err)
		t.Cleanup(func() { _ = d.Close() })
		return src.reads.Load(), d
	}
	lazy, d1 := reads(false)
	eager, d2 := reads(true)
	if lazy > 1+4*4 {
		t.Fatalf("a lazy open of 4 parts made %d reads", lazy)
	}
	if eager < lazy+20*3 {
		t.Fatalf("VerifyOnOpen made %d reads, the lazy open %d: it did not read every row group", eager, lazy)
	}
	r1, err1 := d1.Range(ctx, seekparquet.Range{Start: []byte(keys[1234]), Limit: 50}, nil)
	r2, err2 := d2.Range(ctx, seekparquet.Range{Start: []byte(keys[1234]), Limit: 50}, nil)
	if err1 != nil || err2 != nil || !bytes.Equal([]byte(keysOf(r1)[0]), []byte(keys[1234])) || len(r1) != len(r2) {
		t.Fatalf("%v %v", err1, err2)
	}
}
