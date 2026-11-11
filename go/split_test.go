// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// splitCounts are the shard counts the split tests ask for.
var splitCounts = []int{1, 2, 3, 7, 64}

// scanRow is a scanned row as the split tests compare it: key, offset and
// the first projected value.
func scanRow(row Row) string {
	s := fmt.Sprintf("%q/%d", row.Key, row.Offset)
	if len(row.Values) > 0 {
		if v := row.Values[0]; v.IsNull() || v.Absent() { // the oracle's value is null when absent
			s += "=null"
		} else {
			s += fmt.Sprintf("=%d", v.Int64())
		}
	}
	return s
}

// scanShard scans plan, returning its rows and keys.
func scanShard(t *testing.T, plan *RangePlan, st *ReadStats) ([]string, [][]byte) {
	t.Helper()
	var rows []string
	var keys [][]byte
	must(t, plan.Scan(ctx, st, func(row Row) error {
		rows = append(rows, scanRow(row))
		keys = append(keys, row.Key)
		return nil
	}))
	return rows, keys
}

// checkSplit plans ranges over d with column, and without columns, and
// splits each plan into every count of splitCounts: the shards' scans
// together are the plan's scan, which is the oracle's rows; each shard is
// non-empty, its count exact as the plan's and equal to its rows; the
// counts and projected bytes add up to the plan's; with exact bytes, each
// shard's scan asks for exactly its projected bytes; no group of equal
// keys crosses two shards.
func checkSplit(t *testing.T, d *Dataset, src *extentSource, rows []oracleRow, extents []chunkExtent, column string, ranges []Range) {
	t.Helper()
	var want []string
	for _, row := range rows {
		if slices.ContainsFunc(ranges, func(rng Range) bool { return selects(rng, row.key) && afterCursor(rng.Cursor, row.key, row.offset) }) {
			s := fmt.Sprintf("%q/%d", row.key, row.offset)
			if row.value.IsNull() {
				s += "=null"
			} else {
				s += fmt.Sprintf("=%d", row.value.Int64())
			}
			want = append(want, s)
		}
	}
	for _, columns := range [][]string{{column}, nil} {
		name := fmt.Sprintf("%v over %s", columns, describe(ranges))
		plan, err := d.PlanRanges(ctx, ranges, columns)
		must(t, err)
		count, countExact := plan.Count()
		bytesTotal, bytesExact := plan.ProjectedBytes()
		for _, n := range splitCounts {
			shards, err := plan.Split(ctx, n)
			if err != nil {
				t.Fatalf("%s: split %d: %v", name, n, err)
			}
			if len(want) == 0 && len(shards) != 0 || len(shards) > n || len(want) > 0 && len(shards) == 0 {
				t.Fatalf("%s: %d rows split %d ways into %d shards", name, len(want), n, len(shards))
			}
			var got []string
			var sumRows, sumBytes int64
			var lastKey []byte
			for i, shard := range shards {
				src.take()
				shardRows, keys := scanShard(t, shard, nil)
				served := chunkBytes(t, name, src.take(), extents)
				rowsN, exact := shard.Count()
				if len(shardRows) == 0 || rowsN != int64(len(shardRows)) || exact != countExact && !exact {
					t.Fatalf("%s: shard %d of %d scanned %d rows, counts %d (exact %v)", name, i, n, len(shardRows), rowsN, exact)
				}
				b, bExact := shard.ProjectedBytes()
				if bExact != bytesExact || columns == nil && b != 0 || bExact && columns != nil && b != served {
					t.Fatalf("%s: shard %d of %d projects %d bytes (exact %v), its scan asked for %d", name, i, n, b, bExact, served)
				}
				if i > 0 && bytes.Equal(lastKey, keys[0]) {
					t.Fatalf("%s: the group of %q crosses shards %d and %d of %d", name, lastKey, i-1, i, n)
				}
				lastKey = keys[len(keys)-1]
				got = append(got, shardRows...)
				sumRows += rowsN
				sumBytes += b
			}
			w := want
			if columns == nil {
				w = make([]string, len(want))
				for i, s := range want {
					w[i], _, _ = strings.Cut(s, "=")
				}
			}
			if !slices.Equal(got, w) {
				t.Fatalf("%s: %d shards of %d:\n got %q\nwant %q", name, len(shards), n, got, w)
			}
			if sumRows != count || sumBytes != bytesTotal {
				t.Fatalf("%s: shards of %d count %d rows and %d bytes, the plan %d and %d", name, n, sumRows, sumBytes, count, bytesTotal)
			}
		}
	}
}

// TestSplitConformanceOracle splits plans over every valid conformance
// dataset: the whole dataset, a sample of the ranges the plan oracle
// uses, and random sets of them.
func TestSplitConformanceOracle(t *testing.T) {
	dirs, err := filepath.Glob("../conformance/datasets/*/STRUCTURE.txt")
	must(t, err)
	for _, structure := range dirs {
		raw, err := os.ReadFile(structure)
		must(t, err)
		if !strings.Contains(string(raw), "\nvalid: yes\n") {
			continue
		}
		dir := filepath.Dir(structure)
		t.Run(filepath.Base(dir), func(t *testing.T) {
			column := "size"
			if strings.Contains(string(raw), "column i64: INT64") {
				column = "i64"
			}
			d, src, rows, extents := openOracle(t, dir, column)
			keys := make([][]byte, len(rows))
			for i, row := range rows {
				keys[i] = row.key
			}
			ranges := planRanges(keys)
			checkSplit(t, d, src, rows, extents, column, []Range{{}})
			for i := 0; i < len(ranges); i += 11 {
				checkSplit(t, d, src, rows, extents, column, []Range{ranges[i]})
			}
			random := rand.New(rand.NewPCG(2, uint64(len(rows))))
			for range 10 {
				var set []Range
				for range 2 + random.IntN(3) {
					set = append(set, ranges[random.IntN(len(ranges))])
				}
				checkSplit(t, d, src, rows, extents, column, set)
			}
		})
	}
}

// TestSplitGeometries splits plans over datasets of several parts and row
// groups: unique keys, and runs of equal keys that cross row groups, in
// several geometries; with ranges ending everywhere, a cursor inside a
// group, and a selection inside one row group.
func TestSplitGeometries(t *testing.T) {
	keys := fixtureKeys(240)
	stem := strings.Repeat("x", 40)
	var long []string
	for _, k := range []string{"a", "b", "c", "d"} {
		for range map[string]int{"a": 2, "b": 60, "c": 3, "d": 30}[k] {
			long = append(long, k+stem)
		}
	}
	for _, c := range []struct {
		name     string
		repeated bool
		keys     [][]string
		geometry testutil.Part
	}{
		{"unique", false, split(keys, 3), testutil.Part{GroupRows: 40, PageBytes: 256}},
		{"unique groups of 7", false, split(keys, 4), testutil.Part{GroupRows: 7, PageBytes: 128}},
		{"repeated groups of 7", true, repeatedParts(), testutil.Part{GroupRows: 7, PageBytes: 16}},
		{"repeated groups of 19", true, repeatedParts(), testutil.Part{GroupRows: 19, PageBytes: 32}},
		{"repeated one row per group", true, repeatedParts(), testutil.Part{GroupRows: 1}},
		{"long groups", true, [][]string{long}, testutil.Part{GroupRows: 25, PageBytes: 128}},
	} {
		t.Run(c.name, func(t *testing.T) {
			doc := parts(c.geometry, c.keys...)
			doc.RepeatedKeys = c.repeated
			d, src, rows, extents := openOracle(t, write(t, doc), "size")
			distinct := slices.CompactFunc(slices.Clone(rows), func(a, b oracleRow) bool { return bytes.Equal(a.key, b.key) })
			step := max(1, len(distinct)/12)
			checkSplit(t, d, src, rows, extents, "size", []Range{{}})
			for i := 0; i < len(distinct); i += step {
				k := distinct[i].key
				after := distinct[min(i+len(distinct)/3, len(distinct)-1)].key
				for _, rng := range []Range{
					{Start: k},
					{End: k},
					{Start: k, End: after},
					{Start: k, Cursor: &Cursor{Key: k, Offset: 1}},
					KeyRange(k, 0),
				} {
					checkSplit(t, d, src, rows, extents, "size", []Range{rng})
				}
				checkSplit(t, d, src, rows, extents, "size", []Range{{End: k}, {Start: after}})
			}
		})
	}
}

// TestSplitBalance: split boundaries fall at row groups and balance the
// projected bytes, or the rows of a plan without columns; a plan of one
// row group is one shard; boundaries in a repeated-key dataset decode the
// key pages around them, in unique mode nothing, and a cancelled context
// fails a split in either mode.
func TestSplitBalance(t *testing.T) {
	keys := fixtureKeys(240)
	for _, repeated := range []bool{false, true} {
		doc := parts(testutil.Part{GroupRows: 20, PageBytes: 256}, split(keys, 2)...)
		doc.RepeatedKeys = repeated
		d := open(t, write(t, doc), Options{})
		for _, columns := range [][]string{{"size"}, nil} {
			plan, err := d.PlanRanges(ctx, []Range{{}}, columns)
			must(t, err)
			shards, err := plan.Split(ctx, 4)
			must(t, err)
			if len(shards) != 4 {
				t.Fatalf("repeated %v, %v: %d shards", repeated, columns, len(shards))
			}
			for i, shard := range shards {
				n, _ := shard.Count()
				if columns != nil && n%20 != 0 || n < 40 || n > 80 {
					t.Fatalf("repeated %v, %v: shard %d has %d rows", repeated, columns, i, n)
				}
				if st := shard.Stats(); (st.KeyPagesDecoded > 0) != (repeated && i < 3 && i != 1) {
					// The boundary after shard 1 is part 1's first row.
					t.Fatalf("repeated %v, %v: shard %d placed its end with %+v", repeated, columns, i, st)
				}
			}
		}
		// A cancelled split fails in both modes: with repeated keys at its
		// first boundary read, with unique keys, which read nothing, by
		// checking its context.
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		for _, columns := range [][]string{{"size"}, nil} {
			plan, err := d.PlanRanges(ctx, []Range{{}}, columns)
			must(t, err)
			if shards, err := plan.Split(cancelled, 4); !errors.Is(err, context.Canceled) || shards != nil {
				t.Fatalf("repeated %v, %v: a cancelled split: %d shards, %v", repeated, columns, len(shards), err)
			}
		}
		plan, err := d.PlanRanges(ctx, []Range{{Start: []byte(keys[3]), End: []byte(keys[15])}}, []string{"size"})
		must(t, err)
		if shards, err := plan.Split(ctx, 5); err != nil || len(shards) != 1 {
			t.Fatalf("a selection inside one row group: %d shards, %v", len(shards), err)
		}
	}
	empty, err := open(t, write(t, parts(testutil.Part{}, keys[:10])), Options{}).PlanRanges(ctx, []Range{{Start: []byte("zzzz")}}, nil)
	must(t, err)
	if shards, err := empty.Split(ctx, 3); err != nil || shards != nil {
		t.Fatalf("an empty plan: %v, %v", shards, err)
	}
}

// TestSplitConcurrentScans scans the shards of plans over a repeated-key
// dataset at once, each with its own stats, and compares them with a
// sequential scan of the plan.
func TestSplitConcurrentScans(t *testing.T) {
	doc := parts(testutil.Part{GroupRows: 7, PageBytes: 16}, repeatedParts()...)
	doc.RepeatedKeys = true
	d := open(t, write(t, doc), Options{})
	plan, err := d.PlanRanges(ctx, []Range{{Start: []byte("a/")}}, []string{"size"})
	must(t, err)
	want, _ := scanShard(t, plan, nil)
	for range 4 {
		shards, err := plan.Split(ctx, 7)
		must(t, err)
		got := make([][]string, len(shards))
		stats := make([]ReadStats, len(shards))
		var wg sync.WaitGroup
		for i, shard := range shards {
			wg.Go(func() {
				err := shard.Scan(ctx, &stats[i], func(row Row) error {
					got[i] = append(got[i], scanRow(row))
					return nil
				})
				if err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if all := slices.Concat(got...); !slices.Equal(all, want) {
			t.Fatalf("concurrent shards:\n got %q\nwant %q", all, want)
		}
		for i, st := range stats {
			if st.KeysDecoded < int64(len(got[i])) || st.ValuePagesDecoded == 0 {
				t.Fatalf("shard %d of %d rows: %+v", i, len(got[i]), st)
			}
		}
	}
}

// TestSplitRequests: a part a read refuses is refused to every shard
// touching it and to no other; a cancelled context fails a shard's scan,
// and a closed dataset a split.
func TestSplitRequests(t *testing.T) {
	keys := fixtureKeys(300)
	dir := write(t, parts(testutil.Part{GroupRows: 25, PageBytes: 256}, split(keys, 3)...))
	rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	d := open(t, dir, Options{})
	plan, err := d.PlanRanges(ctx, []Range{{Start: []byte(keys[10]), End: []byte(keys[290])}}, nil)
	must(t, err)
	shards, err := plan.Split(ctx, 7)
	must(t, err)
	refused := 0
	for i, shard := range shards {
		err := shard.Scan(ctx, nil, func(Row) error { return nil })
		var refusal *RefusalError
		touches := slices.ContainsFunc(shard.spans, func(sp span) bool { return sp.start < 200 && sp.end > 100 })
		if touches != errors.As(err, &refusal) || !touches && err != nil {
			t.Fatalf("shard %d (touching part 1: %v): %v", i, touches, err)
		}
		if touches {
			refused++
		}
	}
	if refused != 1 || len(shards) < 3 {
		t.Fatalf("%d of %d shards refused", refused, len(shards))
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := shards[0].Scan(cancelled, nil, func(Row) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled shard scan: %v", err)
	}
	// An empty plan, or n below 1, has no shard, but a cancelled context
	// still fails the split, and so does a Close.
	empty, err := d.PlanRanges(ctx, []Range{{Start: []byte("zzzz")}}, nil)
	must(t, err)
	if shards, err := empty.Split(cancelled, 4); !errors.Is(err, context.Canceled) || shards != nil {
		t.Fatalf("a cancelled split of an empty plan: %d shards, %v", len(shards), err)
	}
	if shards, err := plan.Split(cancelled, 0); !errors.Is(err, context.Canceled) || shards != nil {
		t.Fatalf("a cancelled split into no shard: %d shards, %v", len(shards), err)
	}
	if shards, err := plan.Split(ctx, 0); err != nil || shards != nil {
		t.Fatalf("n 0: %v, %v", shards, err)
	}
	must(t, d.Close())
	if _, err := plan.Split(ctx, 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("a split after close: %v", err)
	}
	if _, err := empty.Split(ctx, 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("a split of an empty plan after close: %v", err)
	}
}

// hookSource calls hook, when set, before every read of a part file.
type hookSource struct {
	Source
	hook func()
}

type hookFile struct {
	SourceFile
	owner *hookSource
}

func (s *hookSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return hookFile{f, s}, nil
}

func (f hookFile) ReadAt(p []byte, off int64) (int, error) {
	if f.owner.hook != nil {
		f.owner.hook()
	}
	return f.SourceFile.ReadAt(p, off)
}

// boundaryDataset is one repeated-key part of four row groups of 20
// distinct keys, two key pages each, and a plan of every row without
// columns.
func boundaryDataset(t *testing.T) string {
	t.Helper()
	keys := make([]string, 80)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d/%s", i, strings.Repeat("x", 40))
	}
	doc := parts(testutil.Part{GroupRows: 20, PageBytes: 512}, keys)
	doc.RepeatedKeys = true
	dir := write(t, doc)
	d := open(t, dir, Options{})
	p, err := d.r.part(ctx, 0, new(ReadStats))
	must(t, err)
	for _, grp := range p.groups {
		index, err := p.keyIndex(ctx, grp.physical, new(ReadStats))
		must(t, err)
		if len(p.groups) != 4 || len(index.pages) != 2 {
			t.Fatalf("%d row groups, one of %d key pages", len(p.groups), len(index.pages))
		}
	}
	return dir
}

// TestSplitBoundaryStats: each shard's Stats are exactly the key pages
// that placed its end, the last page of its last row group and the first
// of the next, though one scan places every boundary.
func TestSplitBoundaryStats(t *testing.T) {
	d := open(t, boundaryDataset(t), Options{})
	plan, err := d.PlanRanges(ctx, []Range{{}}, nil)
	must(t, err)
	shards, err := plan.Split(ctx, 4)
	must(t, err)
	var got []int64
	for _, shard := range shards {
		got = append(got, shard.Stats().KeyPagesDecoded)
	}
	if !slices.Equal(got, []int64{2, 2, 2, 0}) {
		t.Fatalf("key pages decoded per shard: %v", got)
	}
}

// TestSplitDisjointRanges: a boundary is a selected row that begins a row
// group, never one between two ranges.
func TestSplitDisjointRanges(t *testing.T) {
	keys := fixtureKeys(300)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 256}, keys)), Options{})
	between := func(a, b int) Range { return Range{Start: []byte(keys[a]), End: []byte(keys[b])} }
	plan, err := d.PlanRanges(ctx, []Range{between(10, 20), between(210, 220)}, nil)
	must(t, err)
	if shards, err := plan.Split(ctx, 2); err != nil || len(shards) != 1 {
		t.Fatalf("no row group begins inside the ranges: %d shards, %v", len(shards), err)
	}
	plan, err = d.PlanRanges(ctx, []Range{between(10, 20), between(100, 110)}, nil)
	must(t, err)
	shards, err := plan.Split(ctx, 2)
	must(t, err)
	if len(shards) != 2 || shards[1].spans[0].start != 100 || shards[1].spans[0].ordinal {
		t.Fatalf("row group 1 begins the second range: %d shards", len(shards))
	}
	for i, shard := range shards {
		if _, k := scanShard(t, shard, nil); len(k) != 10 || !bytes.Equal(k[0], []byte(keys[10+90*i])) {
			t.Fatalf("shard %d: %d rows from %q", i, len(k), k[0])
		}
	}
}

// TestSplitInterrupted: a split cancelled while it reads a boundary's key
// page fails, and the same split then succeeds; a key page refused at a
// boundary fails the split with its refusal.
func TestSplitInterrupted(t *testing.T) {
	dir := boundaryDataset(t)
	src := &hookSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	defer d.Close()
	plan, err := d.PlanRanges(ctx, []Range{{}}, nil)
	must(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	src.hook = cancel
	if _, err := plan.Split(cancelled, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("a split cancelled during a read: %v", err)
	}
	src.hook = nil
	shards, err := plan.Split(ctx, 4)
	must(t, err)
	for i, shard := range shards {
		if n, exact := shard.Count(); n != 20 || !exact || len(shards) != 4 {
			t.Fatalf("after a cancelled split, shard %d of %d counts %d", i, len(shards), n)
		}
	}

	d0 := open(t, dir, Options{})
	p, err := d0.r.part(ctx, 0, new(ReadStats))
	must(t, err)
	index, err := p.keyIndex(ctx, p.groups[1].physical, new(ReadStats))
	must(t, err)
	f, err := os.OpenFile(partFile(dir, 0), os.O_WRONLY, 0)
	must(t, err)
	_, err = f.WriteAt(make([]byte, index.pages[0].size), index.pages[0].offset)
	must(t, errors.Join(err, f.Close()))
	broken := open(t, dir, Options{})
	plan, err = broken.PlanRanges(ctx, []Range{{}}, nil)
	must(t, err)
	var refusal *RefusalError
	if _, err := plan.Split(ctx, 4); !errors.As(err, &refusal) {
		t.Fatalf("a refused boundary key page: %v", err)
	}
}
