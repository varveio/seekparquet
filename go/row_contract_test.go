// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"slices"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

func TestInputRowSpansPreservesInputs(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{GroupRows: 10}, fixtureKeys(30))), OpenOptions{})
	inputs := []RowSpan{d.r.rowSpan(5, 15), d.r.rowSpan(2, 8), d.r.rowSpan(4, 4)}
	want := slices.Clone(inputs)
	p, err := d.PlanRows(ctx, inputs, nil, nil)
	must(t, err)
	inputs[0] = RowSpan{}
	got := p.InputRowSpans()
	if !slices.Equal(got, want) {
		t.Fatalf("inputs %v, want %v", got, want)
	}
	got[1] = RowSpan{}
	if !slices.Equal(p.InputRowSpans(), want) {
		t.Fatal("returned slice aliases plan inputs")
	}
	if n, _ := p.Count(); n != 13 {
		t.Fatalf("union count %d, want 13", n)
	}
	shards, err := p.Split(ctx, 2, nil)
	must(t, err)
	for _, shard := range shards {
		if len(shard.InputRowSpans()) != len(shard.spans) {
			t.Fatal("shard inputs must describe its normalized spans")
		}
	}
}

func TestKnownRowPositionsCanHaveInexactCount(t *testing.T) {
	keys := fixtureKeys(30)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 10}, split(keys, 3)...)), OpenOptions{})
	// Resolve the outer ends while leaving the middle part unchecked.
	outer, err := d.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	spans := outer.InputRowSpans()
	for _, sp := range spans {
		if _, ok := sp.Start.Ordinal(); !ok {
			t.Fatal("unknown start")
		}
		if _, ok := sp.End.Ordinal(); !ok {
			t.Fatal("unknown end")
		}
	}
	var st ReadStats
	p, err := d.PlanRows(ctx, spans, nil, &st)
	must(t, err)
	if n, exact := p.Count(); n != 30 || exact || st != (ReadStats{}) {
		t.Fatalf("count %d exact %v, stats %+v", n, exact, st)
	}
	checked, err := d.PlanRows(ctx, spans, []string{"size"}, nil)
	must(t, err)
	if n, exact := checked.Count(); n != 30 || !exact {
		t.Fatalf("projected count %d exact %v", n, exact)
	}
	if _, exact := p.Count(); exact {
		t.Fatal("existing plan's evidence snapshot changed")
	}
}

func TestRowPositionInspectionAfterClose(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{GroupRows: 10}, fixtureKeys(30))), OpenOptions{})
	p, err := d.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	spans := p.InputRowSpans()
	must(t, d.Close())
	if n, known := spans[0].End.Ordinal(); n != 30 || !known {
		t.Fatalf("end after close: %d known %v", n, known)
	}
	sub, err := p.SelectRows(spans)
	must(t, err)
	if n, _ := sub.Count(); n != 30 {
		t.Fatalf("selected count after close: %d", n)
	}
	if err := sub.ScanValues(ctx, nil, func(*ValueBatch) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("scan after close: %v", err)
	}
	// Split is not a pure in-memory transformation under the existing contract.
	if _, err := p.Split(ctx, 2, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("split after close: %v", err)
	}
}

func TestValueBatchAggregationAcrossInputSpans(t *testing.T) {
	type record struct {
		Key  string `parquet:"key"`
		Size int64  `parquet:"size"`
		Body string `parquet:"body,plain"`
	}
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: []record{
		{"a", 1, "A"}, {"b", 2, "B"}, {"c", 3, "C"}, {"d", 4, "D"},
	}}}}), OpenOptions{})
	inputs := []RowSpan{d.r.rowSpan(0, 2), d.r.rowSpan(2, 4), d.r.rowSpan(1, 3), d.r.rowSpan(4, 4)}
	p, err := d.PlanRows(ctx, inputs, []string{"size", "body"}, nil)
	must(t, err)
	sums := make([]int64, len(inputs))
	crossed := false
	var retained RowPosition
	var cloned Value
	must(t, p.ScanValues(ctx, nil, func(b *ValueBatch) error {
		start, _ := b.Start().Ordinal()
		if start < 2 && start+int64(b.Len()) > 2 {
			crossed = true
		}
		retained = b.Start()
		batchCopy := *b
		viewCopy := b.Column(1)
		borrowed := b.Column(1).Bytes(0)
		if &batchCopy.Column(1).Bytes(0)[0] != &borrowed[0] || &viewCopy.Bytes(0)[0] != &borrowed[0] {
			t.Fatal("batch/view copying unexpectedly copied the value bytes")
		}
		cloned = b.Column(1).Value(0).Clone()
		if &cloned.Bytes()[0] == &borrowed[0] {
			t.Fatal("Clone did not independently own its bytes")
		}
		for i := range b.Len() {
			ordinal := start + int64(i)
			for j, sp := range inputs {
				lo, _ := sp.Start.Ordinal()
				hi, _ := sp.End.Ordinal()
				if lo <= ordinal && ordinal < hi {
					sums[j] += b.Column(0).Int64(i)
				}
			}
		}
		return nil
	}))
	if !crossed || !slices.Equal(sums, []int64{3, 7, 5, 0}) {
		t.Fatalf("crossed %v, sums %v", crossed, sums)
	}
	must(t, d.Close())
	if start, known := retained.Ordinal(); !known || start != 0 || string(cloned.Bytes()) != "A" {
		t.Fatalf("owned results after close: %d %v, %v", start, known, cloned)
	}
}

func TestListingBoundaryContracts(t *testing.T) {
	for _, delimiter := range []byte{'/', '|'} {
		key := func(s string) string {
			b := []byte(s)
			for i := range b {
				if b[i] == '/' {
					b[i] = delimiter
				}
			}
			return string(b)
		}
		keys := []string{key("a/"), key("a/b"), key("a/c"), key("a/d"), key("b/a")}
		d := open(t, write(t, parts(testutil.Part{GroupRows: 3, PageBytes: 32}, keys)), OpenOptions{})
		for _, tc := range []struct {
			name      string
			limit     int
			end       []byte
			cursor    *Cursor
			start     int64
			endKnown  bool
			wantCount int64
		}{
			{name: "at-limit", limit: 1, wantCount: 4},
			{name: "traversed", limit: 3, endKnown: true, wantCount: 4},
			{name: "end-clipped", limit: 3, end: []byte(key("a/c")), wantCount: 2},
			{name: "row-cursor", limit: 3, cursor: RowCursor([]byte(key("a/b")), 0), start: 2, endKnown: true, wantCount: 2},
			{name: "prefix-cursor", limit: 3, cursor: PrefixCursor([]byte(key("a/b"))), start: 2, endKnown: true, wantCount: 2},
		} {
			t.Run(string(delimiter)+"/"+tc.name, func(t *testing.T) {
				q := ListQuery{Delimiter: delimiter, Limit: tc.limit, End: tc.end, Cursor: tc.cursor}
				entries, err := d.List(ctx, q, nil)
				must(t, err)
				if len(entries) == 0 || string(entries[0].CommonPrefix) != key("a/") {
					t.Fatal(entries)
				}
				e := entries[0]
				if start, known := e.Rows.Start.Ordinal(); !known || start != tc.start {
					t.Fatalf("start %d known %v, want %d", start, known, tc.start)
				}
				if _, known := e.Rows.End.Ordinal(); known != tc.endKnown {
					t.Fatalf("end known %v, want %v", known, tc.endKnown)
				}
				if !tc.endKnown {
					if _, err := d.PlanRows(ctx, []RowSpan{e.Rows}, nil, nil); !errors.Is(err, ErrInvalidRequest) {
						t.Fatalf("unknown end accepted as input: %v", err)
					}
				}
				// Resolve the entry's candidate rows while preserving restrictions.
				sel := Selection{Span: PrefixSpan(e.CommonPrefix)}
				sel.Span.End = q.End
				if q.Cursor != nil {
					if q.Cursor.Key != nil {
						sel.Cursor = q.Cursor
					} else {
						var ok bool
						sel.Span.Start, ok = successor(q.Cursor.Prefix)
						if !ok {
							t.Fatal("fixture cursor must have a successor")
						}
					}
				}
				p, err := d.Plan(ctx, []Selection{sel}, nil, nil)
				must(t, err)
				if n, exact := p.Count(); n != tc.wantCount || !exact {
					t.Fatalf("fallback count %d exact %v, want %d", n, exact, tc.wantCount)
				}
			})
		}
	}
}
