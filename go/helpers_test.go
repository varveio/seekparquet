// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
	"github.com/varveio/seekparquet/go/source/local"
)

// write writes d into a new temporary directory and returns it.
func write(t testing.TB, d seekparquettest.Dataset) string {
	t.Helper()
	dir := t.TempDir()
	if err := seekparquettest.Write(dir, d); err != nil {
		t.Fatal(err)
	}
	return dir
}

// open opens the dataset at dir, closed when the test ends.
func open(t testing.TB, dir string, opt seekparquet.Options) *seekparquet.Dataset {
	t.Helper()
	d, err := local.OpenDataset(context.Background(), dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// parts returns one dataset part per key slice, with the given geometry.
func parts(geometry seekparquettest.Part, keys ...[]string) seekparquettest.Dataset {
	var d seekparquettest.Dataset
	for _, k := range keys {
		p := geometry
		p.Rows = seekparquettest.Rows(k...)
		d.Parts = append(d.Parts, p)
	}
	return d
}

func keysOf(rows []seekparquet.Row) []string {
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

// rangeOracle answers a range request over sorted keys by the definition
// (RANGE-1).
func rangeOracle(keys []string, rng seekparquet.Range) []string {
	var out []string
	for _, k := range keys {
		if !bytes.HasPrefix([]byte(k), rng.Prefix) {
			continue
		}
		if rng.Start != nil {
			c := bytes.Compare([]byte(k), rng.Start)
			if c < 0 || c == 0 && rng.StartExclusive {
				continue
			}
		}
		if rng.End != nil && bytes.Compare([]byte(k), rng.End) >= 0 || len(out) == rng.Limit {
			break
		}
		out = append(out, k)
	}
	return out
}

// listOracle answers a list request over sorted keys by the definition of
// §6.3: candidates, their entries, LIST-5, distinct, ascending, at most
// limit. Rows are spelled "r:" and prefixes "p:".
func listOracle(keys []string, req seekparquet.List) []string {
	var out []string
	seen := map[string]bool{}
	for _, k := range keys {
		key := []byte(k)
		if !bytes.HasPrefix(key, req.Prefix) || req.After != nil && bytes.Compare(key, req.After) <= 0 ||
			req.End != nil && bytes.Compare(key, req.End) >= 0 {
			continue
		}
		entry := "r:" + k
		if i := bytes.IndexByte(key[len(req.Prefix):], req.Delimiter); i >= 0 {
			cp := key[:len(req.Prefix)+i+1]
			if req.Continuation && req.After != nil && bytes.Compare(cp, req.After) <= 0 {
				continue
			}
			entry = "p:" + string(cp)
		}
		if !seen[entry] {
			seen[entry] = true
			out = append(out, entry)
		}
	}
	slices.SortFunc(out, func(a, b string) int { return strings.Compare(a[2:], b[2:]) })
	if len(out) > req.Limit {
		out = out[:req.Limit]
	}
	return out
}

func entriesOf(entries []seekparquet.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		if e.IsPrefix() {
			out[i] = "p:" + string(e.CommonPrefix)
		} else {
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

func short(b []byte) string {
	if len(b) > 24 {
		return fmt.Sprintf("%q…(%d)", b[:24], len(b))
	}
	return fmt.Sprintf("%q", b)
}

var ctx = context.Background()

// openErr opens the dataset at dir and returns the error, closing a dataset
// that opened.
func openErr(dir string, opt seekparquet.Options) (*seekparquet.Dataset, error) {
	d, err := local.OpenDataset(context.Background(), dir, opt)
	if err == nil {
		_ = d.Close()
	}
	return d, err
}
