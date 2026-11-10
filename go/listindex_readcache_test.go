// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

func TestListIndexWarmReadCost(t *testing.T) {
	var keys []string
	for i := 0; i < 500; i++ {
		keys = append(keys, fmt.Sprintf("a/one/%06d", i))
	}
	for i := 0; i < 20000; i++ {
		keys = append(keys, fmt.Sprintf("f%06d", i))
	}
	for i := 0; i < 500; i++ {
		keys = append(keys, fmt.Sprintf("g/d%06d/x", i))
	}
	for i := 0; i < 20; i++ {
		keys = append(keys, fmt.Sprintf("z/%02d/", i))
		for j := 0; j < 50; j++ {
			keys = append(keys, fmt.Sprintf("z/%02d/%03d", i, j))
		}
	}
	dir := write(t, parts(testutil.Part{GroupRows: 500, PageBytes: 2000, IndexSizeLimit: 16}, keys, []string{"zz/x", "zz/y"}))
	rewrite(t, dir, 1, func(m *format.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Codec = format.LZO })
	probe := open(t, dir, Options{})
	full, err := probe.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	if full.GroupsFailed != 1 || full.GroupsSkipped < 40 {
		t.Fatal(full)
	}
	firstCost := 256 + probe.r.listIndex.Load().groups[0].memoryBytes()
	for _, budget := range []int64{0, firstCost} {
		plain := open(t, dir, Options{})
		indexed := open(t, dir, Options{})
		st, err := indexed.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget})
		must(t, err)
		if !st.HasIndex || budget > 0 && !st.BudgetExhausted {
			t.Fatal(st)
		}
		queries := []any{Range{Start: []byte("f010000"), Limit: 3}, List{Prefix: []byte("z/"), Delimiter: '/', Limit: 20, Columns: []string{"size"}},
			List{Prefix: []byte("z/"), Delimiter: '/', Limit: 20, Columns: []string{"size"}, Cursor: &Cursor{Prefix: []byte("z/00/")}}, List{Prefix: []byte("f01"), Delimiter: '/', Limit: 3}, List{Prefix: []byte("g/"), Delimiter: '/', Limit: 20}}
		for _, query := range queries {
			run := func(d *Dataset) (any, ReadStats) {
				var stats ReadStats
				switch q := query.(type) {
				case Range:
					q.Stats = &stats
					out, e := d.Range(ctx, q)
					must(t, e)
					return out, stats
				case List:
					q.Stats = &stats
					out, e := d.List(ctx, q)
					must(t, e)
					return out, stats
				}
				panic("query")
			}
			for round := 0; round < 3; round++ {
				want, ps := run(plain)
				got, is := run(indexed)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("budget=%d query=%+v results differ", budget, query)
				}
				if round > 0 && (is.SourceReads > ps.SourceReads || is.KeyPagesDecoded > ps.KeyPagesDecoded || is.IndexLoads != 0) {
					t.Fatalf("budget=%d query=%+v plain=%+v indexed=%+v", budget, query, ps, is)
				}
			}
		}
	}
}

func TestListIndexUnsupportedGroup(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 2}, []string{"a/x", "a/y", "b/x", "b/y"}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].Columns[0].MetaData.Codec = format.LZO })
	d := open(t, dir, Options{})
	st, err := d.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	if st.GroupsFailed != 1 || st.GroupsIndexed != 1 {
		t.Fatal(st)
	}
	compareListIndex(t, open(t, dir, Options{}), d, List{Prefix: []byte("a/"), Delimiter: '/', Limit: 1})
	for _, r := range []*Dataset{open(t, dir, Options{}), d} {
		_, err = r.List(ctx, List{Prefix: []byte("b/"), Delimiter: '/', Limit: 10})
		var unsupported *UnsupportedError
		if !errors.As(err, &unsupported) || unsupported.Code != UnsupportedEncoding {
			t.Fatal(err)
		}
	}
}

func TestListIndexOrdinaryOrderChecks(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 2}, []string{"a/0", "a/1", "c/0", "c/1", "b/0", "d/0"}))
	for _, indexed := range []bool{false, true} {
		for _, listing := range []bool{false, true} {
			d := open(t, dir, Options{})
			if indexed {
				_, err := d.BuildListIndex(ctx, ListIndexOptions{})
				must(t, err)
			}
			var err error
			if listing {
				_, err = d.List(ctx, List{Delimiter: '/', Limit: 100})
			} else {
				_, err = d.Range(ctx, Range{Limit: 100})
			}
			if CodeOf(err) != CodeKeyOrder {
				t.Fatalf("indexed=%v list=%v: %v", indexed, listing, err)
			}
		}
	}
}

func TestListIndexEmptyDeclinesAndTransientStats(t *testing.T) {
	for _, keys := range [][]string{{"a", "b"}, {"a/x", "b/y"}} {
		dir := write(t, parts(testutil.Part{}, keys))
		d := open(t, dir, Options{})
		st, err := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 1})
		must(t, err)
		if st.HasIndex {
			t.Fatal(st)
		}
		for _, cursor := range []string{"a/", "a", "missing/"} {
			rs := compareListIndex(t, open(t, dir, Options{}), d, List{Delimiter: '/', Limit: 10, Cursor: &Cursor{Prefix: []byte(cursor)}})
			if rs.ListIndexSkips+rs.ListIndexDelimiter+rs.ListIndexMissingGroup+rs.ListIndexDensePage+rs.ListIndexUnsafeRun != 0 {
				t.Fatal(rs)
			}
		}
	}
	d := open(t, write(t, parts(testutil.Part{}, []string{"a/x", "b/y"})), Options{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	var st ReadStats
	_, err := d.r.parts[0].loadKeyIndex(ctx, 0, &st, false)
	must(t, err)
	if st.IndexLoads != 1 || st.SourceReads == 0 || d.r.indexMemory.Load() != 0 {
		t.Fatalf("transient load hidden: %+v", st)
	}
}
