// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package write

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	seekparquet "github.com/varveio/seekparquet/go"
)

type kv struct {
	Key   []byte  `parquet:"key"`
	Value *string `parquet:"value,optional"`
}

var kvSchema = parquet.SchemaOf(new(kv))

func kvRows(keys ...string) []parquet.Row {
	rows := make([]parquet.Row, len(keys))
	for i, k := range keys {
		v := "v-" + k
		rows[i] = kvSchema.Deconstruct(nil, &kv{Key: []byte(k), Value: &v})
	}
	return rows
}

func kvOptions(unique bool) PartOptions {
	return PartOptions{Key: seekparquet.Key{Column: "key", Unique: unique}, SchemaID: "test/kv"}
}

func TestNewPartRefuses(t *testing.T) {
	type optionalKey struct {
		Key *[]byte `parquet:"key,optional"`
	}
	type intKey struct {
		Key int64 `parquet:"key"`
	}
	type jsonKey struct {
		Key string `parquet:"key,json"`
	}
	type nestedKey struct {
		Key struct {
			A []byte `parquet:"a"`
		} `parquet:"key"`
	}
	type stringKey struct {
		Key string `parquet:"key"`
	}
	for name, c := range map[string]struct {
		schema *parquet.Schema
		opt    PartOptions
	}{
		"no key column":  {kvSchema, PartOptions{Key: seekparquet.Key{Column: "id"}, SchemaID: "s"}},
		"empty column":   {kvSchema, PartOptions{SchemaID: "s"}},
		"no schema id":   {kvSchema, PartOptions{Key: seekparquet.Key{Column: "key"}}},
		"optional key":   {parquet.SchemaOf(new(optionalKey)), kvOptions(true)},
		"integer key":    {parquet.SchemaOf(new(intKey)), kvOptions(true)},
		"JSON key":       {parquet.SchemaOf(new(jsonKey)), kvOptions(true)},
		"nested key":     {parquet.SchemaOf(new(nestedKey)), kvOptions(true)},
		"value as key":   {kvSchema, PartOptions{Key: seekparquet.Key{Column: "value"}, SchemaID: "s"}},
		"string key ok?": {parquet.SchemaOf(new(stringKey)), kvOptions(true)},
	} {
		_, err := NewPart(&memFile{}, c.schema, c.opt)
		if name == "string key ok?" {
			if err != nil {
				t.Errorf("STRING key: %v", err)
			}
			continue
		}
		if !errors.Is(err, ErrProfile) {
			t.Errorf("%s: %v, want ErrProfile", name, err)
		}
	}
}

func TestWriteRowsRefuses(t *testing.T) {
	for name, c := range map[string]struct {
		unique bool
		rows   []parquet.Row
		ok     int
		rule   string
	}{
		"descending":         {true, kvRows("a", "c", "b"), 2, "KEY-3"},
		"equal, unique":      {true, kvRows("a", "b", "b"), 2, "KEY-3"},
		"descending, groups": {false, kvRows("a", "b", "b", "a"), 3, "KEY-3"},
		"empty key":          {true, kvRows("a", ""), 1, "KEY-2"},
		"missing column":     {true, append(kvRows("a"), parquet.Row{parquet.ByteArrayValue([]byte("b")).Level(0, 0, 0)}), 1, "does not match"},
		"wrong kind":         {true, append(kvRows("a"), parquet.Row{parquet.Int64Value(1).Level(0, 0, 0), parquet.NullValue().Level(0, 0, 1)}), 1, "does not match"},
		"columns swapped":    {true, append(kvRows("a"), parquet.Row{parquet.NullValue().Level(0, 0, 1), parquet.ByteArrayValue([]byte("b")).Level(0, 0, 0)}), 1, "does not match"},
	} {
		f := &memFile{}
		w, err := NewPart(f, kvSchema, kvOptions(c.unique))
		if err != nil {
			t.Fatal(err)
		}
		n, err := w.WriteRows(c.rows)
		if n != c.ok || !errors.Is(err, ErrProfile) || !strings.Contains(err.Error(), c.rule) {
			t.Errorf("%s: %d rows, %v; want %d rows and %s", name, n, err, c.ok, c.rule)
		}
		if n, again := w.WriteRows(kvRows("zzz")); n != 0 || again != err {
			t.Errorf("%s: a write after the error: %d, %v", name, n, again)
		}
		if _, err := w.Seal(); err == nil || f.closed != 1 {
			t.Errorf("%s: Seal after an error: %v, closed %d times", name, err, f.closed)
		}
	}
	type stringKey struct {
		Key string `parquet:"key"`
	}
	strSchema := parquet.SchemaOf(new(stringKey))
	w, err := NewPart(&memFile{}, strSchema, kvOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	strRows := []parquet.Row{strSchema.Deconstruct(nil, &stringKey{Key: "caf\u00e9"}), strSchema.Deconstruct(nil, &stringKey{Key: "caf\xff"})}
	if n, err := w.WriteRows(strRows); n != 1 || !errors.Is(err, ErrProfile) || !strings.Contains(err.Error(), "KEY-6") {
		t.Errorf("a STRING key that is not UTF-8: %d, %v", n, err)
	}
	if w, err = NewPart(&memFile{}, kvSchema, kvOptions(true)); err != nil {
		t.Fatal(err)
	}
	if n, err := w.WriteRows(kvRows("a", "\xff")); n != 2 || err != nil {
		t.Errorf("an unannotated key that is not UTF-8: %d, %v", n, err)
	}
	w, err = NewPart(&memFile{}, kvSchema, kvOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.WriteRows(kvRows("a", "a", "b", "b", "b", "c")); n != 6 || err != nil {
		t.Errorf("equal keys with groups: %d, %v", n, err)
	}
}

func TestSealAndAbort(t *testing.T) {
	f := &memFile{}
	w, err := NewPart(f, kvSchema, kvOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Seal(); !errors.Is(err, ErrProfile) || !strings.Contains(err.Error(), "PART-2") || f.closed != 1 {
		t.Errorf("sealing an empty part: %v, closed %d", err, f.closed)
	}
	if _, err := w.Seal(); err == nil {
		t.Errorf("sealed twice")
	}
	f = &memFile{}
	w, _ = NewPart(f, kvSchema, kvOptions(true))
	if _, err := w.WriteRows(kvRows("a")); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil || f.closed != 1 || f.synced != 0 {
		t.Errorf("Abort: %v, closed %d, synced %d", err, f.closed, f.synced)
	}
	if err := w.Abort(); err != nil || f.closed != 1 {
		t.Errorf("second Abort: %v, closed %d", err, f.closed)
	}
	if _, err := w.WriteRows(kvRows("b")); err == nil {
		t.Errorf("a write after Abort")
	}
	if _, err := w.Seal(); err == nil {
		t.Errorf("Seal after Abort")
	}
}

// faultyFile fails writes after limit bytes (a short write without an
// error when short), or fails Sync.
type faultyFile struct {
	memFile
	limit    int
	short    bool
	syncFail bool
}

func (f *faultyFile) Write(p []byte) (int, error) {
	room := f.limit - f.Len()
	if room >= len(p) {
		return f.memFile.Write(p)
	}
	f.memFile.Write(p[:max(room, 0)])
	if f.short {
		return max(room, 0), nil
	}
	return max(room, 0), errors.New("disk full")
}

func (f *faultyFile) Sync() error {
	if f.syncFail {
		return errors.New("sync failed")
	}
	return f.memFile.Sync()
}

// TestFileFaults: a short or failed write, and a failed sync, fail the
// part, and the file is closed.
func TestFileFaults(t *testing.T) {
	var keys []string
	for i := range 5000 {
		keys = append(keys, fmt.Sprintf("key-%06d", i))
	}
	rows := kvRows(keys...)
	for name, f := range map[string]*faultyFile{
		"short write":  {limit: 1000, short: true},
		"failed write": {limit: 1000},
		"footer short": {limit: 0, short: true}, // set below to the part's size less a few bytes
		"failed sync":  {limit: 1 << 30, syncFail: true},
	} {
		opt := kvOptions(true)
		opt.RowGroupRows = 1000 // row groups are written as the rows arrive
		if name == "footer short" {
			whole := &memFile{}
			w, _ := NewPart(whole, kvSchema, opt)
			w.WriteRows(rows)
			if _, err := w.Seal(); err != nil {
				t.Fatal(err)
			}
			f.limit = whole.Len() - 10
		}
		w, err := NewPart(f, kvSchema, opt)
		if err != nil {
			t.Fatal(err)
		}
		_, werr := w.WriteRows(rows)
		_, serr := w.Seal()
		if serr == nil || f.closed != 1 {
			t.Errorf("%s: WriteRows %v, Seal %v, closed %d", name, werr, serr, f.closed)
		}
		if f.short && !errors.Is(errors.Join(werr, serr), io.ErrShortWrite) {
			t.Errorf("%s: %v / %v, want io.ErrShortWrite", name, werr, serr)
		}
	}
}

// TestPartProfile: a part with groups across pages and row groups, keys
// over 1,024 bytes beginning with 0xFF bytes and values over 1,024 bytes
// has everything the profile asks of it, page bounds exactly its keys,
// and the same bytes whatever the column workers.
func TestPartProfile(t *testing.T) {
	var keys []string
	for i := range 3000 {
		lead := strings.Repeat("\xff", 1100+i/1000)
		keys = append(keys, lead+fmt.Sprintf("%04d", i/3)) // groups of three
	}
	type wide struct {
		Key     []byte `parquet:"key"`
		Payload []byte `parquet:"payload"`
		N       int64  `parquet:"n"`
	}
	schema := parquet.SchemaOf(new(wide))
	var rows []parquet.Row
	for i, k := range keys {
		rows = append(rows, schema.Deconstruct(nil, &wide{Key: []byte(k), Payload: bytes.Repeat([]byte{byte(i)}, 1500), N: int64(i)}))
	}
	write := func(workers int) []byte {
		opt := kvOptions(false)
		opt.PageRows, opt.RowGroupRows = 100, 700
		opt.ColumnWorkers = func() int { return workers }
		f := &memFile{}
		w, err := NewPart(f, schema, opt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Seal(); err != nil {
			t.Fatal(err)
		}
		return f.Bytes()
	}
	raw := write(1)
	if !bytes.Equal(raw, write(3)) {
		t.Errorf("the bytes depend on the column workers")
	}
	pages, err := seekparquet.InspectKeyPages(bytes.NewReader(raw), int64(len(raw)), "key")
	if err != nil {
		t.Fatal(err)
	}
	var rowsSeen int64
	for i, p := range pages {
		if p.Rows > 100 || p.FirstRow != rowsSeen {
			t.Fatalf("page %d: rows %d from %d", i, p.Rows, p.FirstRow)
		}
		if string(p.Min) != keys[p.FirstRow] || string(p.Max) != keys[p.FirstRow+p.Rows-1] {
			t.Fatalf("page %d: bounds of %d and %d bytes are not its first and last keys", i, len(p.Min), len(p.Max))
		}
		rowsSeen += p.Rows
	}
	if rowsSeen != int64(len(keys)) {
		t.Fatalf("pages hold %d rows of %d", rowsSeen, len(keys))
	}
	ft, err := seekparquet.InspectFooter(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if ft.Key != (seekparquet.Key{Column: "key"}) || ft.SchemaID != "test/kv" || ft.Rows != 3000 || len(ft.Columns) != 3 {
		t.Errorf("footer %+v", ft)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	md := pf.Metadata()
	for _, o := range md.ColumnOrders {
		if _, ok := o.Value.(*format.TypeDefinedOrder); !ok {
			t.Errorf("a column without TYPE_ORDER")
		}
	}
	if len(md.RowGroups) < 4 {
		t.Errorf("%d row groups", len(md.RowGroups))
	}
	for g, rg := range md.RowGroups {
		if len(rg.SortingColumns) == 0 || rg.SortingColumns[0].ColumnIdx != 0 || rg.SortingColumns[0].Descending {
			t.Errorf("row group %d: sorting columns %+v", g, rg.SortingColumns)
		}
		for c, cc := range rg.Columns {
			if cc.OffsetIndexLength == 0 || cc.ColumnIndexLength == 0 {
				t.Errorf("row group %d column %d lacks a page index", g, c)
			}
			if cc.MetaData.Codec != format.Zstd {
				t.Errorf("row group %d column %d: codec %v", g, c, cc.MetaData.Codec)
			}
		}
	}
}
