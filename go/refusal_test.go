// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// twoParts is a valid two-part dataset of keys a..d and m..p, two row
// groups in the first.
func twoParts() testutil.Dataset {
	return testutil.Dataset{Parts: []testutil.Part{
		{Rows: testutil.Rows("a", "b", "c", "d"), GroupRows: 2},
		{Rows: testutil.Rows("m", "n", "o", "p")},
	}}
}

func stamped(change map[string]string) testutil.Dataset {
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
type sizeString struct {
	Key  []byte `parquet:"key"`
	Size string `parquet:"size"`
}

// TestOpenRefusals produces each refusal detected on open and requires its
// code. Each case starts from a valid dataset.
func TestOpenRefusals(t *testing.T) {
	cases := []struct {
		name    string
		code    Code
		dataset testutil.Dataset
		mutate  func(t *testing.T, dir string)
	}{
		{"no manifest", CodeNoManifest, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(filepath.Join(dir, "manifest.json")))
		}},
		{"manifest is a directory", CodeNoManifest, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(filepath.Join(dir, "manifest.json")))
			must(t, os.Mkdir(filepath.Join(dir, "manifest.json"), 0o755))
		}},
		{"wrong format", CodeInvalidManifest, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { m["format"] = "parquet" })
		}},
		{"path escape", CodeInvalidManifest, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["path"] = "../x.parquet" })
		}},
		{"draft revision 2", CodeUnsupportedVersion, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { m["draft_revision"] = 2 })
		}},
		{"missing part", CodeMissingPart, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(partFile(dir, 1)))
		}},
		{"part is a directory", CodeMissingPart, twoParts(), func(t *testing.T, dir string) {
			must(t, os.Remove(partFile(dir, 1)))
			must(t, os.Mkdir(partFile(dir, 1), 0o755))
		}},
		{"wrong size", CodePartMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["bytes"] = 7 })
		}},
		{"wrong rows", CodePartMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 1)["rows"] = 3 })
		}},
		{"not parquet", CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			must(t, os.WriteFile(partFile(dir, 0), bytes.Repeat([]byte("x"), 64), 0o644))
			must(t, testutil.SyncSizes(dir))
		}},
		{"footer longer than the file", CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			raw, _ := os.ReadFile(partFile(dir, 0))
			copy(raw[len(raw)-8:], "\xff\xff\xff\x7fPAR1")
			must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
		}},
		{"file_path set", CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].Columns[1].FilePath = "other.parquet" })
		}},
		{"row counts do not sum", CodeInvalidFooter, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.NumRows = 5 })
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["rows"] = 5 })
		}},
		{"stamp missing", CodeStampMismatch, stamped(map[string]string{testutil.StampSchemaID: ""}), nil},
		{"draft stamp missing", CodeStampMismatch, stamped(map[string]string{testutil.StampDraftRevision: ""}), nil},
		{"schema id differs", CodeStampMismatch, stamped(map[string]string{testutil.StampSchemaID: "other/v1"}), nil},
		{"non-canonical revision", CodeStampMismatch, stamped(map[string]string{testutil.StampDraftRevision: "01"}), nil},
		{"format version differs", CodeStampMismatch, stamped(map[string]string{testutil.StampFormatVersion: "1"}), nil},
		{"key column differs", CodeStampMismatch, stamped(map[string]string{testutil.StampKeyColumn: "id"}), nil},
		{"key uniqueness differs", CodeStampMismatch, stamped(map[string]string{testutil.StampKeyUnique: "false"}), nil},
		{"stamp twice", CodeStampMismatch, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 1, func(m *format.FileMetaData) {
				m.KeyValueMetadata = append(m.KeyValueMetadata, format.KeyValue{Key: testutil.StampSchemaID, Value: testutil.SchemaID})
			})
		}},
		{"integer key", CodeInvalidKeyColumn, testutil.Dataset{Parts: []testutil.Part{{Rows: []keyInt{{1}, {2}}}}}, nil},
		{"optional key", CodeInvalidKeyColumn, testutil.Dataset{Parts: []testutil.Part{{Rows: []keyOptional{{ptr("a")}, {ptr("b")}}}}}, nil},
		{"JSON key", CodeInvalidKeyColumn, testutil.Dataset{Parts: []testutil.Part{{Rows: []keyJSON{{`"a"`}, {`"b"`}}}}}, nil},
		{"repeated key", CodeInvalidKeyColumn, testutil.Dataset{Parts: []testutil.Part{{Rows: []keyRepeated{{[]string{"a"}}, {[]string{"b"}}}}}}, nil},
		{"no column orders", CodeInvalidKeyColumn, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.ColumnOrders = nil })
		}},
		{"inconsistent column", CodeInconsistentColumn, testutil.Dataset{Parts: []testutil.Part{
			{Rows: testutil.Rows("a", "b")},
			{Rows: []sizeString{{[]byte("c"), "1"}}},
		}}, nil},
		{"no sorting columns", CodeMissingSortingColumns, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a"), Unsorted: true}}}, nil},
		{"sorting descending", CodeMissingSortingColumns, twoParts(), func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].SortingColumns[0].Descending = true })
		}},
		{"manifest bounds overlap", CodeInvalidManifest, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 1)["min_key_base64"] = b64("c") })
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, c.dataset)
			if c.mutate != nil {
				c.mutate(t, dir)
			}
			err := openErr(dir, OpenOptions{})
			var refusal *RefusalError
			if !errors.As(err, &refusal) || refusal.Code != c.code {
				t.Fatalf("got %v, want %s", err, c.code)
			}
		})
	}
	// The unchanged dataset opens.
	open(t, write(t, twoParts()), OpenOptions{})
}

// TestVerify produces faults in the first and last key pages and the key
// page index, which Verify checks without a read (CHECK-3): the dataset
// opens, and Verify or a full read refuses it with the same code.
func TestVerify(t *testing.T) {
	cases := []struct {
		name    string
		code    Code
		dataset testutil.Dataset
		mutate  func(t *testing.T, dir string)
	}{
		{"first key differs from the manifest", CodeBoundsMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["min_key_base64"] = b64("\x00") })
		}},
		{"last key differs from the manifest", CodeBoundsMismatch, twoParts(), func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 1)["max_key_base64"] = b64("q") })
		}},
		{"no key page index", CodeMissingPageIndex, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a"), NoKeyPageIndex: true}}}, nil},
		{"empty first key", CodeInvalidKey, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("", "a")}}}, func(t *testing.T, dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["min_key_base64"] = b64("\x00") })
		}},
		{"row groups out of order", CodeKeyOrder, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a", "c", "b", "d"), GroupRows: 2}}}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := write(t, c.dataset)
			if c.mutate != nil {
				c.mutate(t, dir)
			}
			if err := verifyErr(dir, OpenOptions{}); CodeOf(err) != c.code {
				t.Fatalf("Verify: %v, want %s", err, c.code)
			}
			d := open(t, dir, OpenOptions{})
			if _, err := d.Rows(ctx, Query{Limit: 100}, nil); CodeOf(err) != c.code {
				t.Fatalf("read: %v, want %s", err, c.code)
			}
		})
	}
}

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
	part := func(rows any) testutil.Dataset {
		return testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256}}}
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
		code    Code
		dataset testutil.Dataset
		check   func(t *testing.T, err error)
	}{
		{"keys out of order", CodeKeyOrder, part(testutil.Rows(keys(func(k []string) []string {
			k[150], k[151] = k[151], k[150]
			return k
		})...)), func(t *testing.T, err error) {
			var r *RefusalError
			errors.As(err, &r)
			if r.Part != testutil.PartPath(0) || r.RowGroup != 0 || r.Row != 151 || string(r.Key) != "k0150" {
				t.Fatalf("located at %s group %d row %d key %q", r.Part, r.RowGroup, r.Row, r.Key)
			}
		}},
		{"duplicate key", CodeKeyOrder, part(testutil.Rows(keys(func(k []string) []string {
			k[151] = k[150]
			return k
		})...)), nil},
		{"blocks out of order", CodeKeyOrder, part(testutil.Rows(keys(func(k []string) []string {
			return append(append(append(append([]string{}, k[:100]...), k[150:200]...), k[100:150]...), k[200:]...)
		})...)), nil},
		{"empty key", CodeInvalidKey, part(testutil.Rows(keys(func(k []string) []string {
			k[150] = ""
			return k
		})...)), nil},
		{"STRING key not UTF-8", CodeInvalidKey, part(strs(keys(func(k []string) []string {
			k[150] = "k0150\xff"
			return k
		}))), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := open(t, write(t, c.dataset), OpenOptions{})
			_, err := d.Rows(ctx, Query{Limit: 1000}, nil)
			if CodeOf(err) != c.code {
				t.Fatalf("range: %v, want %s", err, c.code)
			}
			if c.check != nil {
				c.check(t, err)
			}
			if _, err = d.List(ctx, ListQuery{Delimiter: '/', Limit: 1000}, nil); CodeOf(err) != c.code {
				t.Fatalf("list: %v, want %s", err, c.code)
			}
			// A read that never reaches the fault succeeds (CHECK-4).
			if rows, err := d.Rows(ctx, Query{Limit: 3}, nil); err != nil || len(rows) != 3 {
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
	good := parts(testutil.Part{PageBytes: 512, GroupRows: 150}, keys)
	dir := write(t, good)
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	if meta.RowGroups[2].Columns[0].ColumnIndexLength == 0 {
		t.Fatal("no key column index")
	}
	d := open(t, dir, OpenOptions{})
	for i, k := range keys {
		got, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(k)}}, Limit: 1}, nil)
		if err != nil || len(got) != 1 || string(got[0].Key) != k {
			t.Fatalf("from key %d: %d rows, %v", i, len(got), err)
		}
	}

	bad := good
	bad.Parts[0].KeepShortenedMax = true
	d2, err := OpenDir(ctx, write(t, bad), OpenOptions{})
	if err == nil {
		defer d2.Close()
		_, err = d2.Rows(ctx, Query{Limit: 1000}, nil)
	}
	if CodeOf(err) != CodeInvalidPageIndex {
		t.Fatalf("shortened maximum: %v", err)
	}
}

// TestCorruptValuePage overwrites the page header of a value column: the
// key read succeeds, the projection of that column ends with
// invalid_parquet (CHECK-5).
func TestCorruptValuePage(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, []string{"a", "b", "c"}))
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	off := meta.RowGroups[0].Columns[1].MetaData.DataPageOffset
	raw, _ := os.ReadFile(partFile(dir, 0))
	copy(raw[off:], bytes.Repeat([]byte{0xff}, 8))
	must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
	d := open(t, dir, OpenOptions{})
	if rows, err := d.Rows(ctx, Query{Limit: 10}, nil); err != nil || len(rows) != 3 {
		t.Fatalf("keys only: %d rows, %v", len(rows), err)
	}
	if _, err := d.Rows(ctx, Query{Limit: 10, Columns: []string{"size"}}, nil); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("projection: %v", err)
	}
}

// TestUnsupported reports valid data this reader does not handle as
// unsupported, not as a refusal: an encrypted part and an unknown codec.
func TestUnsupported(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	raw, _ := os.ReadFile(partFile(dir, 0))
	copy(raw[len(raw)-4:], "PARE")
	must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
	err := openErr(dir, OpenOptions{})
	var u *UnsupportedError
	if !errors.As(err, &u) || u.Code != UnsupportedEncryption || CodeOf(err) != "" {
		t.Fatalf("encrypted footer: %v", err)
	}

	dir = write(t, parts(testutil.Part{}, []string{"a", "b"}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[1].MetaData.Codec = format.LZO })
	d := open(t, dir, OpenOptions{})
	_, err = d.Rows(ctx, Query{Limit: 10, Columns: []string{"size"}}, nil)
	if !errors.As(err, &u) || u.Code != UnsupportedEncoding {
		t.Fatalf("LZO column: %v", err)
	}
}

// countingSource counts the files it opened and closed.
type countingSource struct {
	Source
	opened, closed atomic.Int64
}

type countedFile struct {
	SourceFile
	src *countingSource
}

func (f countedFile) Close() error { f.src.closed.Add(1); return f.SourceFile.Close() }

func (s *countingSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return countedFile{f, s}, nil
}

// TestFailedOpenClosesParts requires a part that fails its check to be
// closed at once, whichever check failed; open to close the manifest and
// open nothing else; and a closed dataset to close every file once.
func TestFailedOpenClosesParts(t *testing.T) {
	for name, mutate := range map[string]func(dir string){
		"bounds": func(dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 1)["max_key_base64"] = b64("q") })
		},
		"stamps": func(dir string) { rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil }) },
		"size": func(dir string) {
			editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 1)["bytes"] = 1 })
		},
		"key type": func(dir string) { rewrite(t, dir, 1, func(m *format.FileMetaData) { m.ColumnOrders = nil }) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := write(t, twoParts())
			mutate(dir)
			src := &countingSource{Source: dirOf(t, dir)}
			d, err := OpenDataset(ctx, src, OpenOptions{})
			must(t, err)
			if err := d.Verify(ctx); err == nil {
				t.Fatal("verified")
			}
			// Part 0 stays open; part 1 is closed when its check refuses it,
			// and kept open when only its key pages are refused (bounds).
			stillOpen := int64(1)
			if name == "bounds" {
				stillOpen = 2
			}
			if src.opened.Load() != src.closed.Load()+stillOpen {
				t.Fatalf("after Verify: opened %d, closed %d", src.opened.Load(), src.closed.Load())
			}
			must(t, d.Close())
			if src.opened.Load() == 0 || src.opened.Load() != src.closed.Load() {
				t.Fatalf("opened %d, closed %d", src.opened.Load(), src.closed.Load())
			}
		})
	}
	src := &countingSource{Source: dirOf(t, write(t, twoParts()))}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	if src.opened.Load() != 1 || src.closed.Load() != 1 {
		t.Fatalf("after open: opened %d, closed %d (the manifest only)", src.opened.Load(), src.closed.Load())
	}
	must(t, d.Verify(ctx))
	if src.opened.Load() != 3 || src.closed.Load() != 1 {
		t.Fatalf("after Verify: opened %d, closed %d", src.opened.Load(), src.closed.Load())
	}
	must(t, d.Close())
	must(t, d.Close())
	if src.closed.Load() != 3 {
		t.Fatalf("after Close: closed %d of 3", src.closed.Load())
	}
	if _, err := d.Rows(ctx, Query{Limit: 1}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after Close: %v", err)
	}
}
