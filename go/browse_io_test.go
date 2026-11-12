// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil/fakestore"
)

// ioTinyRepeated is ioTiny with repeated keys.
var ioTinyRepeated = ioFixtureSpec{rows: 20_000, parts: 4, groups: 4, perFolder: 100, repeated: true}

// browseCase is a listing a browser makes: after an unrecorded setup on a
// fresh handle (a list index, or an earlier page whose cursor it takes),
// the listing itself.
type browseCase struct {
	name  string
	setup func(ctx context.Context, d *seekparquet.Dataset) (*seekparquet.Cursor, error)
	q     seekparquet.ListQuery
}

// browseCases are the listings of the trace invariant: final pages,
// Limit=1, cross-part prefixes (the root lists acct-NNNN/ folders across
// every part, past unchecked middle parts), repeated keys (the repeated
// fixture), End clipping inside a prefix, read-ahead, and the shortcut
// and list-index paths.
func browseCases(fx *ioFixture) []browseCase {
	tenant := fx.listPrefix
	child := append(bytes.Clone(tenant), "item-0050/"...)
	page := func(q seekparquet.ListQuery) func(context.Context, *seekparquet.Dataset) (*seekparquet.Cursor, error) {
		return func(ctx context.Context, d *seekparquet.Dataset) (*seekparquet.Cursor, error) {
			ents, err := d.List(ctx, q, nil)
			if err != nil || len(ents) == 0 {
				return nil, err
			}
			return ents[len(ents)-1].Cursor(), nil
		}
	}
	index := func(ctx context.Context, d *seekparquet.Dataset) (*seekparquet.Cursor, error) {
		_, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{})
		return nil, err
	}
	root := seekparquet.ListQuery{Delimiter: '/', Limit: 51}
	var out []browseCase
	for _, limit := range []int{1, 2, 26, 1001} {
		q := seekparquet.ListQuery{Prefix: tenant, Delimiter: '/', Limit: limit, Columns: []string{"size"}}
		out = append(out, browseCase{name: fmt.Sprintf("tenant/limit-%d", limit), q: q})
	}
	out = append(out,
		browseCase{name: "root/page1", q: root},
		browseCase{name: "root/limit-1", q: seekparquet.ListQuery{Delimiter: '/', Limit: 1}},
		browseCase{name: "root/page2", setup: page(root), q: root},
		browseCase{name: "root/final", q: seekparquet.ListQuery{Delimiter: '/', Limit: 5000}},
		browseCase{name: "root/read-ahead", q: seekparquet.ListQuery{Delimiter: '/', Limit: 51, ReadAhead: 8}},
		browseCase{name: "tenant/page2", setup: page(seekparquet.ListQuery{Prefix: tenant, Delimiter: '/', Limit: 26}),
			q: seekparquet.ListQuery{Prefix: tenant, Delimiter: '/', Limit: 26}},
		browseCase{name: "tenant/end-clipped", q: seekparquet.ListQuery{Prefix: tenant, Delimiter: '/', Limit: 1001, End: append(bytes.Clone(child), 'm')}},
		browseCase{name: "child/final", q: seekparquet.ListQuery{Prefix: child, Delimiter: '/', Limit: 5000, Columns: []string{"size"}}},
		browseCase{name: "indexed/root", setup: index, q: root},
		browseCase{name: "indexed/tenant", setup: index, q: seekparquet.ListQuery{Prefix: tenant, Delimiter: '/', Limit: 1001}},
		browseCase{name: "row-cursor", setup: func(ctx context.Context, d *seekparquet.Dataset) (*seekparquet.Cursor, error) {
			return seekparquet.RowCursor(fx.keys[len(fx.keys)/2], 0), nil
		}, q: seekparquet.ListQuery{Delimiter: '/', Limit: 20}},
	)
	return out
}

// browseRun is what one run of a browseCase observed.
type browseRun struct {
	trace   string
	stats   seekparquet.ReadStats
	entries string
	counts  []string
}

// runBrowse runs c through store on a fresh fake store and handle, and,
// with counts, counts every listed entry's rows with PlanRows and Count.
func runBrowse(t *testing.T, fx *ioFixture, store ioStore, c browseCase, counts bool) (run browseRun) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		src := fakestore.New(fx.objects, ioOptions)
		d, err := seekparquet.OpenDataset(ctx, store.wrap(src), store.opt)
		must(t, err)
		defer d.Close()
		q := c.q
		if c.setup != nil {
			cur, err := c.setup(ctx, d)
			must(t, err)
			if cur != nil {
				q.Cursor = cur
			}
		}
		op := src.Begin(c.name)
		ents, err := d.List(ctx, q, &run.stats)
		must(t, err)
		if counts {
			for _, e := range ents {
				run.counts = append(run.counts, countOf(ctx, d, e))
			}
		}
		rep := op.End()
		run.entries, _ = listText(ents, nil)
		run.trace = requestsText(rep)
	})
	return run
}

// countOf counts e's rows as a browser does: PlanRows over its Rows with
// no columns, then Count. An entry with an unknown boundary has none.
func countOf(ctx context.Context, d *seekparquet.Dataset, e seekparquet.ListEntry) string {
	_, okStart := e.Rows.Start.Ordinal()
	_, okEnd := e.Rows.End.Ordinal()
	if !okStart || !okEnd {
		return "unknown"
	}
	p, err := d.PlanRows(ctx, []seekparquet.RowSpan{e.Rows}, nil, nil)
	if err != nil {
		return err.Error()
	}
	n, exact := p.Count()
	return fmt.Sprintf("%d %v", n, exact)
}

// requestsText is every request of a report: its kind, object, range,
// batch and outcome, sorted, then when the last ended on the bubble's
// clock. Concurrent requests (read-ahead goroutines, the ranges of one
// batch) start in an order of the scheduler's, which is left out.
func requestsText(r fakestore.Report) string {
	var lines []string
	var last time.Duration
	for _, q := range r.Requests {
		lines = append(lines, fmt.Sprintf("%s %s %d+%d batch %d %v", q.Kind, q.Object, q.Off, q.Len, q.Batch, q.Err))
		last = max(last, q.End)
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n") + fmt.Sprintf("\nlast ends at %v", last)
}

// TestBrowseCountsTraceInvariant: with identical initial state, a listing
// with counts off and the same listing with counts on (ListEntry.Rows,
// PlanRows with no columns, Count) make identical opens, logical reads,
// batch inputs and physical requests, through every store; and the counts
// are those of the fixture's keys.
func TestBrowseCountsTraceInvariant(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	for _, spec := range []ioFixtureSpec{ioTiny, ioTinyRepeated} {
		fx := ioFixtureOf(t, spec)
		known := 0
		for _, c := range browseCases(fx) {
			for _, store := range ioStores {
				t.Run(fmt.Sprintf("repeated-%v/%s/%s", spec.repeated, c.name, store.name), func(t *testing.T) {
					off := runBrowse(t, fx, store, c, false)
					on := runBrowse(t, fx, store, c, true)
					if off.trace != on.trace {
						t.Fatalf("counts change the requests:\noff\n%s\non\n%s", off.trace, on.trace)
					}
					if off.stats != on.stats || off.entries != on.entries {
						t.Fatalf("counts change the listing: %+v\n%+v", off.stats, on.stats)
					}
					for _, n := range on.counts {
						if n != "unknown" {
							known++
						}
					}
				})
			}
		}
		if known == 0 {
			t.Fatalf("no listed entry had both boundaries")
		}
	}
}

// TestBrowseRowsOracle: every boundary a listing records is that of the
// fixture's keys, and a count of a listed entry is its candidate rows.
func TestBrowseRowsOracle(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	ctx := context.Background()
	for _, spec := range []ioFixtureSpec{ioTiny, ioTinyRepeated} {
		fx := ioFixtureOf(t, spec)
		for _, c := range browseCases(fx) {
			d, err := seekparquet.OpenDir(ctx, fx.dir, seekparquet.OpenOptions{})
			must(t, err)
			q := c.q
			if c.setup != nil {
				cur, err := c.setup(ctx, d)
				must(t, err)
				if cur != nil {
					q.Cursor = cur
				}
			}
			ents, err := d.List(ctx, q, nil)
			must(t, err)
			for i, e := range ents {
				lo, hi := candidateRows(fx.keys, q, e)
				if n, ok := e.Rows.Start.Ordinal(); ok && n != lo {
					t.Errorf("%s entry %d %q: start %d, want %d", c.name, i, e.Bytes(), n, lo)
				}
				if n, ok := e.Rows.End.Ordinal(); ok && n != hi {
					t.Errorf("%s entry %d %q: end %d, want %d", c.name, i, e.Bytes(), n, hi)
				}
				_, okStart := e.Rows.Start.Ordinal()
				_, okEnd := e.Rows.End.Ordinal()
				// Only the last entry of a full page, or a prefix clipped by
				// End, may leave its end unknown.
				if !okStart || !okEnd && (i < len(ents)-1 || len(ents) < q.Limit) && q.End == nil {
					t.Errorf("%s entry %d %q of %d: boundaries %v %v", c.name, i, e.Bytes(), len(ents), okStart, okEnd)
				}
				if okStart && okEnd {
					p, err := d.PlanRows(ctx, []seekparquet.RowSpan{e.Rows}, nil, nil)
					must(t, err)
					if n, _ := p.Count(); n != int64(hi-lo) {
						t.Errorf("%s entry %d: count %d, want %d", c.name, i, n, hi-lo)
					}
				}
			}
			must(t, d.Close())
		}
	}
}

// candidateRows returns the dataset ordinals [lo, hi) of the candidate
// rows of q that e represents.
func candidateRows(keys [][]byte, q seekparquet.ListQuery, e seekparquet.ListEntry) (lo, hi int64) {
	lo, hi = -1, -1
	offset := int64(0)
	for i, k := range keys {
		if i > 0 && bytes.Equal(keys[i-1], k) {
			offset++
		} else {
			offset = 0
		}
		if !bytes.HasPrefix(k, q.Prefix) || q.End != nil && bytes.Compare(k, q.End) >= 0 || !after(q.Cursor, k, offset) {
			continue
		}
		var match bool
		if e.IsPrefix() {
			rest := k[len(q.Prefix):]
			j := bytes.IndexByte(rest, q.Delimiter)
			match = j >= 0 && bytes.Equal(k[:len(q.Prefix)+j+1], e.CommonPrefix)
		} else {
			match = bytes.Equal(k, e.Row.Key) && offset == e.Row.Offset
		}
		if match {
			if lo < 0 {
				lo = int64(i)
			}
			hi = int64(i) + 1
		}
	}
	return lo, hi
}

// after reports whether a row of key at offset comes after cur.
func after(cur *seekparquet.Cursor, key []byte, offset int64) bool {
	switch {
	case cur == nil:
		return true
	case cur.Prefix != nil:
		return bytes.Compare(key, cur.Prefix) > 0 && !bytes.HasPrefix(key, cur.Prefix)
	}
	c := bytes.Compare(key, cur.Key)
	return c > 0 || c == 0 && offset > cur.Offset
}

// TestPlanRowsInvalid: invalid spans fail with ErrInvalidRequest before
// any I/O, and so do positions of another handle on the same source.
func TestPlanRowsInvalid(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	fx := ioFixtureOf(t, ioTiny)
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		src := fakestore.New(fx.objects, ioOptions)
		d, err := seekparquet.OpenDataset(ctx, capableStore.wrap(src), seekparquet.OpenOptions{})
		must(t, err)
		defer d.Close()
		other, err := seekparquet.OpenDataset(ctx, capableStore.wrap(src), seekparquet.OpenOptions{})
		must(t, err)
		defer other.Close()
		ents, err := d.List(ctx, seekparquet.ListQuery{Delimiter: '/', Limit: 3}, nil)
		must(t, err)
		good := ents[0].Rows
		alien, err := other.List(ctx, seekparquet.ListQuery{Delimiter: '/', Limit: 3}, nil)
		must(t, err)
		bad := map[string][]seekparquet.RowSpan{
			"unknown":  {{}},
			"no start": {{End: good.End}},
			"no end":   {{Start: good.Start}},
			"reversed": {{Start: good.End, End: good.Start}},
			"alien":    {alien[0].Rows},
			"mixed":    {{Start: good.Start, End: alien[0].Rows.End}},
		}
		for name, spans := range bad {
			op := src.Begin(name)
			_, err := d.PlanRows(ctx, spans, []string{"size"}, nil)
			rep := op.End()
			if !errors.Is(err, seekparquet.ErrInvalidRequest) || len(rep.Requests) != 0 {
				t.Errorf("%s: %v after %d requests", name, err, len(rep.Requests))
			}
		}
		plan, err := d.PlanRows(ctx, []seekparquet.RowSpan{good}, nil, nil)
		must(t, err)
		if _, err := plan.SelectRows([]seekparquet.RowSpan{alien[0].Rows}); !errors.Is(err, seekparquet.ErrInvalidRequest) {
			t.Errorf("select alien rows: %v", err)
		}
		if _, err := plan.SelectRows([]seekparquet.RowSpan{ents[1].Rows}); !errors.Is(err, seekparquet.ErrInvalidRequest) {
			t.Errorf("select rows outside the plan: %v", err)
		}
		if err := plan.ScanValues(ctx, nil, nil); !errors.Is(err, seekparquet.ErrInvalidRequest) {
			t.Errorf("nil callback: %v", err)
		}
		empty, err := d.PlanRows(ctx, nil, nil, nil)
		must(t, err)
		if n, exact := empty.Count(); n != 0 || !exact {
			t.Errorf("empty plan: %d %v", n, exact)
		}
	})
}

// keyChunks returns, by object, the byte ranges of the fixture's key
// column chunks.
func keyChunks(t *testing.T, fx *ioFixture) map[string][][2]int64 {
	t.Helper()
	out := map[string][][2]int64{}
	for name, o := range fx.objects {
		if !strings.HasSuffix(name, ".parquet") {
			continue
		}
		f, err := parquet.OpenFile(bytes.NewReader(o.Data), int64(len(o.Data)))
		must(t, err)
		for _, rg := range f.Metadata().RowGroups {
			m := rg.Columns[0].MetaData // the key is the first column
			start := m.DataPageOffset
			if m.DictionaryPageOffset > 0 {
				start = min(start, m.DictionaryPageOffset)
			}
			out[name] = append(out[name], [2]int64{start, start + m.TotalCompressedSize})
		}
	}
	return out
}

// TestScanValuesReadsNoKeyPage: summing a listed prefix that spans parts
// (PlanRows over its Rows, then ScanValues) reads no byte of a key column
// chunk, through every store, and sums what the fixture holds.
func TestScanValuesReadsNoKeyPage(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	for _, spec := range []ioFixtureSpec{ioTiny, ioTinyRepeated} {
		fx := ioFixtureOf(t, spec)
		chunks := keyChunks(t, fx)
		for _, store := range ioStores {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				src := fakestore.New(fx.objects, ioOptions)
				d, err := seekparquet.OpenDataset(ctx, store.wrap(src), store.opt)
				must(t, err)
				defer d.Close()
				ents, err := d.List(ctx, seekparquet.ListQuery{Delimiter: '/', Limit: 5000}, nil)
				must(t, err)
				var spans []seekparquet.RowSpan
				var want int64
				for _, e := range ents {
					lo, okLo := e.Rows.Start.Ordinal()
					hi, okHi := e.Rows.End.Ordinal()
					if !okLo || !okHi {
						t.Fatalf("%q: unknown boundary", e.Bytes())
					}
					// Every other folder, so the plan has gaps and crosses parts.
					if len(spans)%2 == 0 || hi-lo > 1000 {
						want += hi - lo
					} else {
						continue
					}
					spans = append(spans, e.Rows)
				}
				op := src.Begin("sum")
				p, err := d.PlanRows(ctx, spans, []string{"size", "etag"}, nil)
				must(t, err)
				var rows int64
				var st seekparquet.ReadStats
				must(t, p.ScanValues(ctx, &st, func(b *seekparquet.ValueBatch) error {
					rows += int64(b.Len())
					return nil
				}))
				rep := op.End()
				if rows != want || st.KeyPagesDecoded != 0 {
					t.Fatalf("%s: %d rows, want %d; %+v", store.name, rows, want, st)
				}
				for _, q := range rep.Requests {
					for _, c := range chunks[q.Object] {
						if q.Kind == fakestore.Range && q.Off < c[1] && c[0] < q.Off+q.Len {
							t.Fatalf("%s: request %d+%d of %s reads key chunk %v", store.name, q.Off, q.Len, q.Object, c)
						}
					}
				}
			})
		}
	}
}
