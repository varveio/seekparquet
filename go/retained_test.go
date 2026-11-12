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

// retainedFixture writes four parts of 300 keys in three row groups each,
// with small pages.
func retainedFixture(t *testing.T) (string, []string) {
	keys := fixtureKeys(1200)
	return write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 300}, split(keys, 4)...)), keys
}

// retainedRequests reads every part through Rows, Get, List and a planned
// scan, and returns what each returned.
func retainedRequests(t *testing.T, d *Dataset, keys []string) []string {
	t.Helper()
	var out []string
	add := func(what string, v any, err error) { out = append(out, fmt.Sprintf("%s: %v %v", what, v, err)) }
	for _, k := range []string{keys[0], keys[150], keys[450], keys[777], keys[1199]} {
		rows, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(k))}, Limit: 1, Columns: []string{"size", "name"}}, nil)
		add("rows "+k, keysOf(rows), err)
		got, err := d.Get(ctx, []byte(k), nil, "size")
		add("get "+k, keysOf(got), err)
	}
	entries, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1000}, nil)
	add("list", entriesOf(entries), err)
	plan, err := d.Plan(ctx, []Selection{{Span: Span{Start: []byte(keys[300]), End: []byte(keys[900])}}}, []string{"size"}, nil)
	if err == nil {
		n := 0
		err = scanRows(ctx, plan, nil, func(Row) error { n++; return nil })
		add("plan", n, err)
	} else {
		add("plan", nil, err)
	}
	return out
}

// TestRetainedMetadataReads: with the default policy a part's end, read
// once when it is checked, serves its page indexes: a cold point lookup
// reads no index from the Source, counts their bytes in MetadataHitBytes,
// and returns what a lookup without retained metadata returns.
func TestRetainedMetadataReads(t *testing.T) {
	dir, keys := retainedFixture(t)
	q := Query{Selection: Selection{Span: KeySpan([]byte(keys[450]))}, Limit: 1, Columns: []string{"size"}}
	run := func(opt OpenOptions) ([]Row, ReadStats, *accessSource) {
		src := &accessSource{Source: dirOf(t, dir)}
		d, err := OpenDataset(ctx, src, opt)
		must(t, err)
		t.Cleanup(func() { _ = d.Close() })
		var st ReadStats
		rows, err := d.Rows(ctx, q, &st)
		must(t, err)
		return rows, st, src
	}
	without, off, _ := run(OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
	with, on, src := run(OpenOptions{})
	if !reflect.DeepEqual(with, without) {
		t.Fatalf("rows differ: %v, %v", keysOf(with), keysOf(without))
	}
	// The indexes the lookup loads: the key ColumnIndex and OffsetIndex of
	// each routing probe and of the group read, and the size column's
	// OffsetIndex.
	indexReads := off.IndexLoads*2 + 1
	if on.SourceReads != off.SourceReads-indexReads || on.MetadataHitBytes == 0 || off.MetadataHitBytes != 0 ||
		on.SourceReads != src.reads.Load()-1 || on.IndexLoads != off.IndexLoads { // the Source also read the manifest
		t.Fatalf("with retained metadata %+v\nwithout %+v", on, off)
	}
}

// TestRetainedMetadataEviction: a PageIndexCacheMemory that holds about one
// part's index region keeps at most that much, evicting the part used least
// recently, and changes no answer; one too small for any keeps none.
func TestRetainedMetadataEviction(t *testing.T) {
	dir, keys := retainedFixture(t)
	want := retainedRequests(t, open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}), keys)
	probe := open(t, dir, remote)
	must(t, probe.Verify(ctx, nil))
	one := int64(len(probe.r.parts[0].meta.Load().data))
	for _, budget := range []int64{1, one, one + one/2, 1 << 30} {
		d := open(t, dir, OpenOptions{Limits: Limits{PageIndexCacheMemory: budget}, Read: remote.Read})
		if got := retainedRequests(t, d, keys); !reflect.DeepEqual(got, want) {
			t.Fatalf("budget %d:\n%q\nwant\n%q", budget, got, want)
		}
		kept, held := 0, int64(0)
		for _, p := range d.r.parts {
			if m := p.meta.Load(); m != nil {
				kept++
				held += int64(len(m.data))
			}
		}
		charged := int64(0)
		if c := d.r.meta.Load(); c != nil {
			charged = c.memory
		}
		if held != charged || held > budget || budget < one && kept != 0 || budget >= one && kept == 0 || budget == 1<<30 && kept != 4 {
			t.Fatalf("budget %d: %d parts keep %d bytes (charged %d)", budget, kept, held, charged)
		}
	}
}

// TestRetainedMetadataHostile: a prefetch far beyond a part reads the part
// once, whole; an OffsetIndex of a column no request projects that locates
// garbage refuses only a request projecting that column, with or without
// retained metadata, though its bytes may lie in the retained region.
func TestRetainedMetadataHostile(t *testing.T) {
	dir, keys := retainedFixture(t)
	src := &accessSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: 1 << 50}})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	var st ReadStats
	_, err = d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(keys[0]))}, Limit: 1}, &st)
	must(t, err)
	// One read of the whole part (its head with it), then the key page.
	if st.PartsOpened != 1 || st.SourceReads != 2 || st.SourceBytes-d.r.parts[0].size <= 0 || d.r.parts[0].meta.Load() == nil ||
		int64(len(d.r.parts[0].meta.Load().data)) >= d.r.parts[0].size {
		t.Fatalf("huge prefetch: %+v", st)
	}

	// Point part 1's "name" OffsetIndexes at the head of the file.
	rewrite(t, dir, 1, func(m *format.FileMetaData) {
		for g := range m.RowGroups {
			for c := range m.RowGroups[g].Columns {
				if m.Schema[c+1].Name == "name" {
					m.RowGroups[g].Columns[c].OffsetIndexOffset = 4
					m.RowGroups[g].Columns[c].OffsetIndexLength = 64
				}
			}
		}
	})
	k := []byte(keys[450])
	for _, opt := range []OpenOptions{remote, {Read: ReadPolicy{DisableSpeculation: true}}} {
		d := open(t, dir, opt)
		if rows, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan(k)}, Limit: 1, Columns: []string{"size"}}, nil); err != nil || len(rows) != 1 {
			t.Fatalf("%+v: a request not projecting name: %v", opt.Read, err)
		}
		if _, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan(k)}, Limit: 1, Columns: []string{"name"}}, nil); RefusalCodeOf(err) != CodeInvalidParquet {
			t.Fatalf("%+v: a request projecting name: %v", opt.Read, err)
		}
	}
}

// TestReadPolicyInvalid: a negative policy field is an invalid request.
func TestReadPolicyInvalid(t *testing.T) {
	dir, _ := retainedFixture(t)
	for _, p := range []ReadPolicy{{MetadataPrefetchBytes: -1}} {
		if _, err := OpenDir(ctx, dir, OpenOptions{Read: p}); !isInvalid(err) {
			t.Fatalf("%+v: %v", p, err)
		}
	}
	if _, err := OpenDir(ctx, dir, OpenOptions{Limits: Limits{PageIndexCacheMemory: -1}}); !isInvalid(err) {
		t.Fatal(err)
	}
}

func isInvalid(err error) bool { return errors.Is(err, ErrInvalidRequest) }

// remote is OpenDir with the policy of OpenDataset: its zero policy reads
// as suits a local file, keeping no index bytes (reader.local).
var remote = OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: 2 << 20}}
