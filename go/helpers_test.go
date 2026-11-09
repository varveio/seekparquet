// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

var ctx = context.Background()

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func ptr[T any](v T) *T { return &v }

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// write writes d into a new temporary directory and returns it.
func write(t testing.TB, d testutil.Dataset) string {
	t.Helper()
	dir := t.TempDir()
	must(t, testutil.Write(dir, d))
	return dir
}

// open opens the dataset at dir, closed when the test ends.
func open(t testing.TB, dir string, opt Options) *Dataset {
	t.Helper()
	d, err := OpenDir(ctx, dir, opt)
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// dirOf returns the Source OpenDir opens dir through.
func dirOf(t testing.TB, dir string) Source {
	t.Helper()
	root, err := os.OpenRoot(dir)
	must(t, err)
	t.Cleanup(func() { _ = root.Close() })
	return dirSource{root}
}

// openErr opens the dataset at dir, checks every part as the first read
// to touch it would (CHECK-1), and returns the first error, closing the
// dataset.
func openErr(dir string, opt Options) error {
	d, err := OpenDir(ctx, dir, opt)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.r.checkParts(ctx, new(ReadStats))
}

// verifyErr opens the dataset at dir, verifies it (Dataset.Verify) and
// returns the first error, closing the dataset.
func verifyErr(dir string, opt Options) error {
	d, err := OpenDir(ctx, dir, opt)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Verify(ctx)
}

// verified opens the dataset at dir and verifies it.
func verified(t testing.TB, dir string, opt Options) *Dataset {
	t.Helper()
	d := open(t, dir, opt)
	must(t, d.Verify(ctx))
	return d
}

// openPartsOf opens the n parts of dir directly, without their manifest,
// with their manifest bounds when bounded.
func openPartsOf(t testing.TB, dir string, n int, key Key, bounded bool, opt Options) (*reader, error) {
	t.Helper()
	var bounds []PartInfo
	if bounded {
		d := open(t, dir, Options{})
		bounds = d.Metadata().Parts
	}
	var inputs []partInput
	for i := range n {
		raw, err := os.ReadFile(partFile(dir, i))
		must(t, err)
		in := partInput{name: testutil.PartPath(i), file: bytes.NewReader(raw), size: int64(len(raw))}
		if bounded {
			in.minKey, in.maxKey, in.rows = bounds[i].MinKey, bounds[i].MaxKey, bounds[i].Rows
		}
		inputs = append(inputs, in)
	}
	r, err := openParts(ctx, inputs, key, opt)
	if err == nil {
		t.Cleanup(func() { _ = r.close() })
	}
	return r, err
}

// unique is the default key: column "key", unique.
var unique = Key{Column: testutil.KeyColumn, Unique: true}

func partFile(dir string, i int) string {
	return filepath.Join(dir, filepath.FromSlash(testutil.PartPath(i)))
}

// rewrite applies change to part i's footer and fixes the manifest size.
func rewrite(t testing.TB, dir string, i int, change func(*format.FileMetaData)) {
	t.Helper()
	must(t, testutil.RewriteFooter(partFile(dir, i), func(m *format.FileMetaData) error { change(m); return nil }))
	must(t, testutil.SyncSizes(dir))
}

func editManifest(t testing.TB, dir string, edit func(m map[string]any)) {
	t.Helper()
	must(t, testutil.EditManifest(dir, edit))
}

// parts returns one dataset part per key slice, with the given geometry.
func parts(geometry testutil.Part, keys ...[]string) testutil.Dataset {
	var d testutil.Dataset
	for _, k := range keys {
		p := geometry
		p.Rows = testutil.Rows(k...)
		d.Parts = append(d.Parts, p)
	}
	return d
}

func keysOf(rows []Row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r.Key)
	}
	return out
}

// fixtureKeys returns n distinct sorted keys shaped like a folder tree, with
// keys equal to prefixes, long shared stems and non-ASCII bytes.
func fixtureKeys(n int) []string {
	keys := []string{"a", "b/", "d01/", "zz", "é/x", "é/y", "\xff", "\xff/\xff"}
	long := "long/" + strings.Repeat("x", 90) + "/"
	for i := 0; len(keys) < n; i++ {
		switch i % 5 {
		case 0:
			keys = append(keys, fmt.Sprintf("%s%05d", long, i))
		case 1:
			keys = append(keys, fmt.Sprintf("root%05d", i))
		default:
			keys = append(keys, fmt.Sprintf("d%02d/s%02d/obj%05d", i%13, i%7, i))
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)[:n]
}

// split splits keys into n parts of nearly equal size.
func split(keys []string, n int) [][]string {
	var out [][]string
	per := (len(keys) + n - 1) / n
	for i := 0; i < len(keys); i += per {
		out = append(out, keys[i:min(i+per, len(keys))])
	}
	return out
}

// afterCursor reports whether a row of key at offset comes after cur
// (PAGE-1): a row cursor admits the rest of its group and later keys, a
// prefix cursor the keys after every key that begins with it.
func afterCursor(cur *Cursor, key []byte, offset int64) bool {
	switch {
	case cur == nil:
		return true
	case cur.Prefix != nil:
		return bytes.Compare(key, cur.Prefix) > 0 && !bytes.HasPrefix(key, cur.Prefix)
	}
	cmp := bytes.Compare(key, cur.Key)
	return cmp > 0 || cmp == 0 && offset > cur.Offset
}

// rangeOracle answers a range request over sorted distinct keys by the
// definition (RANGE-1).
func rangeOracle(keys []string, rng Range) []string {
	var out []string
	for _, k := range keys {
		key := []byte(k)
		if !bytes.HasPrefix(key, rng.Prefix) || !afterCursor(rng.Cursor, key, 0) {
			continue
		}
		if rng.Start != nil {
			c := bytes.Compare(key, rng.Start)
			if c < 0 || c == 0 && rng.StartExclusive {
				continue
			}
		}
		if rng.End != nil && bytes.Compare(key, rng.End) >= 0 || len(out) == rng.Limit {
			break
		}
		out = append(out, k)
	}
	return out
}

// listOracle answers a list request over sorted distinct keys by the
// definition of §6.3: candidates, their entries, distinct, ascending, at
// most limit. Rows are spelled "r:" and prefixes "p:", or "P:" when the
// prefix's first candidate key is the prefix itself (its exact row, LIST-8).
func listOracle(keys []string, req List) []string {
	var out []string
	seen := map[string]bool{}
	for _, k := range keys {
		key := []byte(k)
		if !bytes.HasPrefix(key, req.Prefix) || !afterCursor(req.Cursor, key, 0) || req.End != nil && bytes.Compare(key, req.End) >= 0 {
			continue
		}
		entry, spelling := "r:"+k, ""
		if cp := commonPrefix(key, req.Prefix, req.Delimiter); cp != nil {
			entry, spelling = "p:"+string(cp), "p:"+string(cp)
			if k == string(cp) {
				spelling = "P:" + k
			}
		}
		if !seen[entry] {
			seen[entry] = true
			out = append(out, cmp.Or(spelling, entry))
		}
	}
	slices.SortFunc(out, func(a, b string) int { return strings.Compare(a[2:], b[2:]) })
	if len(out) > req.Limit {
		out = out[:req.Limit]
	}
	return out
}

func entriesOf(entries []Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		switch {
		case e.IsPrefix() && e.Exact == nil:
			out[i] = "p:" + string(e.CommonPrefix)
		case e.IsPrefix() && bytes.Equal(e.Exact.Key, e.CommonPrefix) && e.Exact.Offset == 0:
			out[i] = "P:" + string(e.CommonPrefix)
		case e.IsPrefix():
			out[i] = fmt.Sprintf("p:%s with exact row %q/%d", e.CommonPrefix, e.Exact.Key, e.Exact.Offset)
		default:
			out[i] = "r:" + string(e.Row.Key)
		}
	}
	return out
}

// probes returns interesting bounds for keys: every key, the bytes just
// before and after it, and its parent prefixes.
func probes(keys []string, every int) [][]byte {
	set := map[string]bool{"": true, "\x00": true, "\xff\xff\xff": true}
	for i, k := range keys {
		if i%every != 0 && i != len(keys)-1 {
			continue
		}
		set[k] = true
		set[k+"\x00"] = true
		set[k[:len(k)-1]] = true
		if j := strings.LastIndexByte(k, '/'); j >= 0 {
			set[k[:j+1]] = true
		}
	}
	out := make([][]byte, 0, len(set))
	for k := range set {
		out = append(out, []byte(k))
	}
	slices.SortFunc(out, bytes.Compare)
	return out
}

// short quotes b, eliding the middle of a long one.
func short(b []byte) string {
	if b == nil {
		return "nil"
	}
	if len(b) > 24 {
		return fmt.Sprintf("%q…(%d)", b[:24], len(b))
	}
	return fmt.Sprintf("%q", b)
}

func cursorString(c *Cursor) string {
	switch {
	case c == nil:
		return "nil"
	case c.Prefix != nil:
		return "prefix " + short(c.Prefix)
	}
	return fmt.Sprintf("row %s/%d", short(c.Key), c.Offset)
}

// describe names a request in failure messages.
func describe(req any) string {
	switch r := req.(type) {
	case Range:
		return fmt.Sprintf("Range{prefix %s start %s exclusive %v end %s cursor %s limit %d columns %q}",
			short(r.Prefix), short(r.Start), r.StartExclusive, short(r.End), cursorString(r.Cursor), r.Limit, r.Columns)
	case List:
		return fmt.Sprintf("List{prefix %s delimiter %q end %s cursor %s limit %d columns %q}",
			short(r.Prefix), r.Delimiter, short(r.End), cursorString(r.Cursor), r.Limit, r.Columns)
	}
	return fmt.Sprint(req)
}
