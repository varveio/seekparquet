// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
	"github.com/varveio/seekparquet/go/source/local"
)

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func partFile(dir string, i int) string {
	return filepath.Join(dir, filepath.FromSlash(seekparquettest.PartPath(i)))
}

// rewrite applies change to part i's footer and fixes the manifest size.
func rewrite(t testing.TB, dir string, i int, change func(*format.FileMetaData)) {
	t.Helper()
	must(t, seekparquettest.RewriteFooter(partFile(dir, i), func(m *format.FileMetaData) error { change(m); return nil }))
	must(t, seekparquettest.SyncSizes(dir))
}

func editManifest(t testing.TB, dir string, edit func(m map[string]any)) {
	t.Helper()
	must(t, seekparquettest.EditManifest(dir, edit))
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// twoParts is a valid two-part dataset of keys a..d and m..p, two row
// groups in the first.
func twoParts() seekparquettest.Dataset {
	return seekparquettest.Dataset{Parts: []seekparquettest.Part{
		{Rows: seekparquettest.Rows("a", "b", "c", "d"), GroupRows: 2},
		{Rows: seekparquettest.Rows("m", "n", "o", "p")},
	}}
}

func stamped(change map[string]string) seekparquettest.Dataset {
	d := twoParts()
	d.Parts[0].Stamps = change
	return d
}

type keyInt struct {
	Key int64 `parquet:"key"`
}
type keyOptional struct {
	Key *string `parquet:"key,optional"`
}
type keyJSON struct {
	Key string `parquet:"key,json"`
}
type keyRepeated struct {
	Key []string `parquet:"key"`
}
type keyString struct {
	Key string `parquet:"key"`
}
type noKey struct {
	Name string `parquet:"name"`
}
type sizeString struct {
	Key  []byte `parquet:"key"`
	Size string `parquet:"size"`
}

// TestOpenRefusals produces each refusal detected on open and requires its
// code. Each case starts from a valid dataset.
func TestOpenRefusals(t *testing.T) {
	cases := []struct {
		name    string
		code    seekparquet.Code
		dataset seekparquettest.Dataset
		mutate  func(t *testing.T, dir string)
	}{
		{"no manifest", seekparquet.CodeNoManifest, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(filepath.Join(dir, "manifest.json")))
		}},
		{"manifest is a directory", seekparquet.CodeNoManifest, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(filepath.Join(dir, "manifest.json")))
			must(t, os.Mkdir(filepath.Join(dir, "manifest.json"), 0o755))
		}},
		{"wrong format", seekparquet.CodeInvalidManifest, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { m["format"] = "parquet" })
		}},
		{"path escape", seekparquet.CodeInvalidManifest, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 0)["path"] = "../x.parquet" })
		}},
		{"draft revision 2", seekparquet.CodeUnsupportedVersion, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { m["draft_revision"] = 2 })
		}},
		{"missing part", seekparquet.CodeMissingPart, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(partFile(dir, 1)))
		}},
		{"part is a directory", seekparquet.CodeMissingPart, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(partFile(dir, 1)))
			must(t, os.Mkdir(partFile(dir, 1), 0o755))
		}},
		{"wrong size", seekparquet.CodePartMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 0)["bytes"] = 7 })
		}},
		{"wrong rows", seekparquet.CodePartMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 1)["rows"] = 3 })
		}},
		{"not parquet", seekparquet.CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			must(t, os.WriteFile(partFile(dir, 0), bytes.Repeat([]byte("x"), 64), 0o644))
			must(t, seekparquettest.SyncSizes(dir))
		}},
		{"footer longer than the file", seekparquet.CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			raw, _ := os.ReadFile(partFile(dir, 0))
			copy(raw[len(raw)-8:], "\xff\xff\xff\x7fPAR1")
			must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
		}},
		{"file_path set", seekparquet.CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].Columns[1].FilePath = "other.parquet" })
		}},
		{"row counts do not sum", seekparquet.CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.NumRows = 5 })
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 0)["rows"] = 5 })
		}},
		{"stamp missing", seekparquet.CodeStampMismatch, stamped(map[string]string{seekparquettest.StampSchemaID: ""}), nil},
		{"draft stamp missing", seekparquet.CodeStampMismatch, stamped(map[string]string{seekparquettest.StampDraftRevision: ""}), nil},
		{"schema id differs", seekparquet.CodeStampMismatch, stamped(map[string]string{seekparquettest.StampSchemaID: "other/v1"}), nil},
		{"non-canonical revision", seekparquet.CodeStampMismatch, stamped(map[string]string{seekparquettest.StampDraftRevision: "01"}), nil},
		{"format version differs", seekparquet.CodeStampMismatch, stamped(map[string]string{seekparquettest.StampFormatVersion: "1"}), nil},
		{"stamp twice", seekparquet.CodeStampMismatch, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 1, func(m *format.FileMetaData) {
				m.KeyValueMetadata = append(m.KeyValueMetadata, format.KeyValue{Key: seekparquettest.StampSchemaID, Value: seekparquettest.SchemaID})
			})
		}},
		{"integer key", seekparquet.CodeInvalidKeyColumn, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: []keyInt{{1}, {2}}}}}, nil},
		{"optional key", seekparquet.CodeInvalidKeyColumn, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: []keyOptional{{ptr("a")}, {ptr("b")}}}}}, nil},
		{"JSON key", seekparquet.CodeInvalidKeyColumn, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: []keyJSON{{`"a"`}, {`"b"`}}}}}, nil},
		{"repeated key", seekparquet.CodeInvalidKeyColumn, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: []keyRepeated{{[]string{"a"}}, {[]string{"b"}}}}}}, nil},
		{"no column orders", seekparquet.CodeInvalidKeyColumn, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.ColumnOrders = nil })
		}},
		{"inconsistent column", seekparquet.CodeInconsistentColumn, seekparquettest.Dataset{Parts: []seekparquettest.Part{
			{Rows: seekparquettest.Rows("a", "b")},
			{Rows: []sizeString{{[]byte("c"), "1"}}},
		}}, nil},
		{"no sorting columns", seekparquet.CodeMissingSortingColumns, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: seekparquettest.Rows("a"), Unsorted: true}}}, nil},
		{"sorting descending", seekparquet.CodeMissingSortingColumns, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].SortingColumns[0].Descending = true })
		}},
		{"manifest bounds overlap", seekparquet.CodeBoundsMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 1)["min_key_base64"] = b64("c") })
		}},
		{"first key differs from the manifest", seekparquet.CodeBoundsMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 0)["min_key_base64"] = b64("\x00") })
		}},
		{"last key differs from the manifest", seekparquet.CodeBoundsMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 1)["max_key_base64"] = b64("q") })
		}},
		// Detected on open because opening decodes every row group's first
		// page (CHECK-3), which the specification allows earlier than read.
		{"no key page index", seekparquet.CodeMissingPageIndex, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: seekparquettest.Rows("a"), NoKeyPageIndex: true}}}, nil},
		{"empty first key", seekparquet.CodeInvalidKey, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: seekparquettest.Rows("", "a")}}}, func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 0)["min_key_base64"] = b64("\x00") })
		}},
		{"row groups out of order", seekparquet.CodeKeyOrder, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: seekparquettest.Rows("a", "c", "b", "d"), GroupRows: 2}}}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, c.dataset)
			if c.mutate != nil {
				c.mutate(t, dir)
			}
			d, err := local.OpenDataset(ctx, dir, seekparquet.Options{})
			if err == nil {
				_ = d.Close()
			}
			var refusal *seekparquet.RefusalError
			if !errors.As(err, &refusal) || refusal.Code != c.code {
				t.Fatalf("got %v, want %s", err, c.code)
			}
		})
	}
	// The unchanged dataset opens.
	open(t, write(t, twoParts()), seekparquet.Options{})
}

func ptr[T any](v T) *T { return &v }

// TestReadRefusals places a fault in a middle page of a row group, which
// opening does not decode (it decodes each row group's first page and each
// part's last), opens the dataset, and requires a full range read and a
// full listing to end with the fault's code.
func TestReadRefusals(t *testing.T) {
	keys := func(mutate func(k []string) []string) []string {
		k := make([]string, 300)
		for i := range k {
			k[i] = fmt.Sprintf("k%04d", i)
		}
		return mutate(k)
	}
	part := func(rows any) seekparquettest.Dataset {
		return seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: rows, PageBytes: 256}}}
	}
	strs := func(k []string) []keyString {
		out := make([]keyString, len(k))
		for i := range k {
			out[i].Key = k[i]
		}
		return out
	}
	cases := []struct {
		name    string
		code    seekparquet.Code
		dataset seekparquettest.Dataset
		check   func(t *testing.T, err error)
	}{
		{"keys out of order", seekparquet.CodeKeyOrder, part(seekparquettest.Rows(keys(func(k []string) []string {
			k[150], k[151] = k[151], k[150]
			return k
		})...)), func(t *testing.T, err error) {
			var r *seekparquet.RefusalError
			errors.As(err, &r)
			if r.Part != seekparquettest.PartPath(0) || r.RowGroup != 0 || r.Row != 151 || string(r.Key) != "k0150" {
				t.Fatalf("located at %s group %d row %d key %q", r.Part, r.RowGroup, r.Row, r.Key)
			}
		}},
		{"duplicate key", seekparquet.CodeKeyOrder, part(seekparquettest.Rows(keys(func(k []string) []string {
			k[151] = k[150]
			return k
		})...)), nil},
		{"blocks out of order", seekparquet.CodeKeyOrder, part(seekparquettest.Rows(keys(func(k []string) []string {
			return append(append(append(append([]string{}, k[:100]...), k[150:200]...), k[100:150]...), k[200:]...)
		})...)), nil},
		{"empty key", seekparquet.CodeInvalidKey, part(seekparquettest.Rows(keys(func(k []string) []string {
			k[150] = ""
			return k
		})...)), nil},
		{"STRING key not UTF-8", seekparquet.CodeInvalidKey, part(strs(keys(func(k []string) []string {
			k[150] = "k0150\xff"
			return k
		}))), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := open(t, write(t, c.dataset), seekparquet.Options{})
			_, err := d.Range(ctx, seekparquet.Range{Limit: 1000}, nil)
			if seekparquet.CodeOf(err) != c.code {
				t.Fatalf("range: %v, want %s", err, c.code)
			}
			if c.check != nil {
				c.check(t, err)
			}
			_, err = d.List(ctx, seekparquet.List{Delimiter: '/', Limit: 1000}, nil)
			if seekparquet.CodeOf(err) != c.code {
				t.Fatalf("list: %v, want %s", err, c.code)
			}
			// A read that never reaches the fault succeeds (CHECK-4).
			if rows, err := d.Range(ctx, seekparquet.Range{Limit: 3}, nil); err != nil || len(rows) != 3 {
				t.Fatalf("a read before the fault: %d rows, %v", len(rows), err)
			}
		})
	}
}

// TestShortenedMaximum: parquet-go shortens a maximum whose retained bytes
// are all 0xFF to a value below the page's last key. The test writer keeps
// the full maximum (RG-5), so every key is found; with KeepShortenedMax the
// broken bound is refused with invalid_page_index, on open or on read.
func TestShortenedMaximum(t *testing.T) {
	ff := strings.Repeat("\xff", 40)
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("a%04d", i))
	}
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("%s%04d", ff, i))
	}
	good := parts(seekparquettest.Part{PageBytes: 512, GroupRows: 150}, keys)
	dir := write(t, good)
	meta, err := seekparquettest.ReadFooter(partFile(dir, 0))
	must(t, err)
	if meta.RowGroups[2].Columns[0].ColumnIndexLength == 0 {
		t.Fatal("no key column index")
	}
	d := open(t, dir, seekparquet.Options{})
	for i, k := range keys {
		got, err := d.Range(ctx, seekparquet.Range{Start: []byte(k), Limit: 1}, nil)
		if err != nil || len(got) != 1 || string(got[0].Key) != k {
			t.Fatalf("from key %d: %d rows, %v", i, len(got), err)
		}
	}

	bad := good
	bad.Parts[0].KeepShortenedMax = true
	d2, err := local.OpenDataset(ctx, write(t, bad), seekparquet.Options{})
	if err == nil {
		defer d2.Close()
		_, err = d2.Range(ctx, seekparquet.Range{Limit: 1000}, nil)
	}
	if seekparquet.CodeOf(err) != seekparquet.CodeInvalidPageIndex {
		t.Fatalf("shortened maximum: %v", err)
	}
}

func descending(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%04d", n-1-i)
	}
	return keys
}

// TestCorruptValuePage overwrites the page header of a value column: the
// key read succeeds, the projection of that column ends with
// invalid_parquet (CHECK-5).
func TestCorruptValuePage(t *testing.T) {
	dir := write(t, parts(seekparquettest.Part{}, []string{"a", "b", "c"}))
	meta, err := seekparquettest.ReadFooter(partFile(dir, 0))
	must(t, err)
	off := meta.RowGroups[0].Columns[1].MetaData.DataPageOffset
	raw, _ := os.ReadFile(partFile(dir, 0))
	copy(raw[off:], bytes.Repeat([]byte{0xff}, 8))
	must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
	d := open(t, dir, seekparquet.Options{})
	if rows, err := d.Range(ctx, seekparquet.Range{Limit: 10}, nil); err != nil || len(rows) != 3 {
		t.Fatalf("keys only: %d rows, %v", len(rows), err)
	}
	if _, err := d.Range(ctx, seekparquet.Range{Limit: 10, Columns: []string{"size"}}, nil); seekparquet.CodeOf(err) != seekparquet.CodeInvalidParquet {
		t.Fatalf("projection: %v", err)
	}
}

// TestUnsupported reports valid data this reader does not handle as
// unsupported, not as a refusal: an encrypted part and an unknown codec.
func TestUnsupported(t *testing.T) {
	dir := write(t, parts(seekparquettest.Part{}, []string{"a", "b"}))
	raw, _ := os.ReadFile(partFile(dir, 0))
	copy(raw[len(raw)-4:], "PARE")
	must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
	_, err := local.OpenDataset(ctx, dir, seekparquet.Options{})
	var u *seekparquet.UnsupportedError
	if !errors.As(err, &u) || u.Code != seekparquet.UnsupportedEncryption || seekparquet.CodeOf(err) != "" {
		t.Fatalf("encrypted footer: %v", err)
	}

	dir = write(t, parts(seekparquettest.Part{}, []string{"a", "b"}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[1].MetaData.Codec = format.LZO })
	d := open(t, dir, seekparquet.Options{})
	_, err = d.Range(ctx, seekparquet.Range{Limit: 10, Columns: []string{"size"}}, nil)
	if !errors.As(err, &u) || u.Code != seekparquet.UnsupportedEncoding {
		t.Fatalf("LZO column: %v", err)
	}
}

// countingSource counts the parts it opened and closed.
type countingSource struct {
	seekparquet.Source
	opened, closed atomic.Int64
}

type countedPart struct {
	seekparquet.Part
	src *countingSource
}

func (p countedPart) Close() error { p.src.closed.Add(1); return p.Part.Close() }

func (s *countingSource) Open(c context.Context, path string) (seekparquet.Part, error) {
	p, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return countedPart{p, s}, nil
}

// TestFailedOpenClosesParts requires every part a failed open opened, the
// manifest included, to be closed, whichever check failed; and a closed
// dataset to close them all once.
func TestFailedOpenClosesParts(t *testing.T) {
	for name, mutate := range map[string]func(dir string){
		"bounds": func(dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 1)["max_key_base64"] = b64("q") })
		},
		"stamps": func(dir string) { rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil }) },
		"size": func(dir string) {
			editManifest(t, dir, func(m map[string]any) { seekparquettest.ManifestPart(m, 1)["bytes"] = 1 })
		},
		"key type": func(dir string) { rewrite(t, dir, 1, func(m *format.FileMetaData) { m.ColumnOrders = nil }) },
	} {
		dir := write(t, twoParts())
		mutate(dir)
		src := &countingSource{Source: local.NewDir(dir, nil)}
		if _, err := seekparquet.OpenDataset(ctx, src, seekparquet.Options{}); err == nil {
			t.Fatalf("%s: opened", name)
		}
		if src.opened.Load() == 0 || src.opened.Load() != src.closed.Load() {
			t.Fatalf("%s: opened %d, closed %d", name, src.opened.Load(), src.closed.Load())
		}
	}
	src := &countingSource{Source: local.NewDir(write(t, twoParts()), nil)}
	d, err := seekparquet.OpenDataset(ctx, src, seekparquet.Options{})
	must(t, err)
	if src.opened.Load() != 3 || src.closed.Load() != 1 {
		t.Fatalf("after open: opened %d, closed %d (the manifest only)", src.opened.Load(), src.closed.Load())
	}
	must(t, d.Close())
	must(t, d.Close())
	if src.closed.Load() != 3 {
		t.Fatalf("after Close: closed %d of 3", src.closed.Load())
	}
	if _, err := d.Range(ctx, seekparquet.Range{Limit: 1}, nil); !errors.Is(err, seekparquet.ErrClosed) {
		t.Fatalf("read after Close: %v", err)
	}
}
