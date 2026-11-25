// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// foldFixture is a dataset of three parts of 200 rows, with row groups of
// 17 rows and small pages. Row i has key fmt.Sprintf("a/%04d", i) and
// size i, null when i%7 == 0; the middle part has no name column.
func foldFixture(t *testing.T) string {
	t.Helper()
	type keySize struct {
		Key  []byte `parquet:"key"`
		Size *int64 `parquet:"size,optional"`
	}
	var rows [3][]testutil.Row
	var middle []keySize
	for i := range 600 {
		key := []byte(fmt.Sprintf("a/%04d", i))
		var size *int64
		if i%7 != 0 {
			size = ptr(int64(i))
		}
		if i/200 == 1 {
			middle = append(middle, keySize{key, size})
			continue
		}
		rows[i/200] = append(rows[i/200], testutil.Row{Key: key, Size: size, Name: ptr(fmt.Sprintf("n%d", i%5))})
	}
	return write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: rows[0], GroupRows: 17, PageBytes: 256},
		{Rows: middle, GroupRows: 17, PageBytes: 256},
		{Rows: rows[2], GroupRows: 17, PageBytes: 256},
	}})
}

// foldSpans are overlapping, nested, duplicate, empty and part-crossing
// spans of the fold fixture, in no particular order.
func foldSpans(d *Dataset) []RowSpan {
	bounds := [][2]int64{{0, 600}, {100, 350}, {120, 130}, {100, 350}, {300, 300}, {599, 600}, {0, 1}, {190, 410}, {50, 60}, {55, 260}, {600, 600}, {17, 34}, {400, 600}}
	spans := make([]RowSpan, len(bounds))
	for i, b := range bounds {
		spans[i] = d.r.rowSpan(b[0], b[1])
	}
	return spans
}

// sizeTotals is a fold of a row count and the sum and maximum of present,
// non-null sizes.
type sizeTotals struct {
	rows, sum, max int64
	seen           bool
}

// sizeFold folds column 0 of its runs into sizeTotals. It fails when a
// run crosses a boundary of spans.
func sizeFold(spans []RowSpan, columns ...string) Fold[sizeTotals] {
	return Fold[sizeTotals]{
		Columns: columns,
		Add: func(acc *sizeTotals, rows *ValueBatch) error {
			if err := sameSpans(spans, rows); err != nil {
				return err
			}
			acc.rows += int64(rows.Len())
			if len(columns) == 0 {
				return nil
			}
			sizes := rows.Column(0)
			for i := range rows.Len() {
				if !sizes.Present() || sizes.IsNull(i) {
					continue
				}
				n := sizes.Int64(i)
				acc.sum += n
				if !acc.seen || n > acc.max {
					acc.max, acc.seen = n, true
				}
			}
			return nil
		},
		Merge: func(into, from *sizeTotals) {
			into.rows += from.rows
			into.sum += from.sum
			if from.seen && (!into.seen || from.max > into.max) {
				into.max, into.seen = from.max, true
			}
		},
	}
}

// sameSpans returns an error when a boundary of a nonempty span of spans
// lies strictly inside b's run of rows, so that its rows do not all lie in
// the same spans.
func sameSpans(spans []RowSpan, b *ValueBatch) error {
	first := b.start
	end := b.start + int64(b.Len())
	for i, sp := range spans {
		if sp.Start.ordinal == sp.End.ordinal {
			continue
		}
		for _, cut := range []int64{sp.Start.ordinal, sp.End.ordinal} {
			if first < cut && cut < end {
				return fmt.Errorf("a run of rows [%d, %d) crosses a boundary of span %d", first, end, i)
			}
		}
	}
	return nil
}

func TestFoldSameSpans(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	inner := []RowSpan{d.r.rowSpan(120, 130)}
	for _, tc := range []struct {
		start int64
		n     int
		ok    bool
	}{{119, 17, false}, {125, 10, false}, {110, 11, false}, {120, 10, true}, {130, 5, true}, {100, 20, true}} {
		err := sameSpans(inner, &ValueBatch{start: tc.start, n: tc.n})
		if (err == nil) != tc.ok {
			t.Fatalf("run [%d, %d): %v", tc.start, tc.start+int64(tc.n), err)
		}
	}
}

func TestFoldRowsMatchesAggregateRows(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := foldSpans(d)
	reductions := []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}, {Op: Max, Column: "size"}}
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 4} {
			want, err := d.AggregateRows(ctx, spans, reductions, AggregateOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: check}, nil)
			must(t, err)
			var st ReadStats
			got, err := FoldRows(ctx, d, spans, sizeFold(spans, "size"), FoldOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: check}, &st)
			must(t, err)
			if len(got) != len(spans) || st.ValuePagesDecoded == 0 || check != (st.KeyPagesDecoded > 0) {
				t.Fatalf("check=%v workers=%d: %d results, %+v", check, workers, len(got), st)
			}
			for i, r := range got {
				a := want[i]
				if r.Status != AggregateExact || a[0].Status != AggregateExact || r.Value.rows != a[0].Value.Int64() || r.Value.sum != a[1].Value.Int64() || r.Value.seen == a[2].Value.IsNull() || r.Value.seen && r.Value.max != a[2].Value.Int64() {
					t.Fatalf("check=%v workers=%d span %d: fold %+v, aggregate %+v", check, workers, i, r, a)
				}
				start, end := spans[i].Start.ordinal, spans[i].End.ordinal
				if r.Value.rows != end-start {
					t.Fatalf("span %d: %d rows, want %d", i, r.Value.rows, end-start)
				}
			}
		}
	}
}

func TestFoldRowsHandsEachRowOnce(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	// Without the whole span, the union leaves rows [1, 17) and [34, 50) out.
	spans := foldSpans(d)[1:]
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 4} {
			var mu sync.Mutex
			seen := make([]int, 600)
			fold := sizeFold(spans, "size")
			add := fold.Add
			fold.Add = func(acc *sizeTotals, rows *ValueBatch) error {
				mu.Lock()
				for i := range rows.Len() {
					seen[rows.start+int64(i)]++
				}
				mu.Unlock()
				return add(acc, rows)
			}
			_, err := FoldRows(ctx, d, spans, fold, FoldOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: check}, nil)
			must(t, err)

			for o, n := range seen {
				want := 0
				for _, sp := range spans {
					if sp.Start.ordinal <= int64(o) && int64(o) < sp.End.ordinal {
						want = 1
					}
				}
				if n != want {
					t.Fatalf("check=%v workers=%d: Add saw row %d %d times, want %d", check, workers, o, n, want)
				}
			}
		}
	}
}

func TestFoldRowsMergeCopies(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := foldSpans(d)
	// Count rows by name; the middle part lacks the column. New is nil, so
	// Add and Merge start from a nil map.
	byName := Fold[map[string]int]{
		Columns: []string{"name"},
		Add: func(acc *map[string]int, rows *ValueBatch) error {
			if *acc == nil {
				*acc = map[string]int{}
			}
			names := rows.Column(0)
			for i := range rows.Len() {
				name := "absent"
				if names.Present() {
					name = string(names.Bytes(i))
				}
				(*acc)[name]++
			}
			return nil
		},
		Merge: func(into, from *map[string]int) {
			if *into == nil {
				*into = map[string]int{}
			}
			for name, n := range *from {
				(*into)[name] += n
			}
		},
	}
	for _, workers := range []int{1, 3} {
		got, err := FoldRows(ctx, d, spans, byName, FoldOptions{ValueBudgetBytes: -1, Workers: workers}, nil)
		must(t, err)
		if len(got) != len(spans) {
			t.Fatalf("workers=%d: %d results for %d spans", workers, len(got), len(spans))
		}
		for i, r := range got {
			start, end := spans[i].Start.ordinal, spans[i].End.ordinal
			var want map[string]int
			if start < end {
				want = map[string]int{}
			}
			for o := start; o < end; o++ {
				name := "absent"
				if o/200 != 1 {
					name = fmt.Sprintf("n%d", o%5)
				}
				want[name]++
			}
			if r.Status != AggregateExact || !reflect.DeepEqual(r.Value, want) {
				t.Fatalf("workers=%d span %d [%d, %d): %+v, want %v", workers, i, start, end, r, want)
			}
		}

		// Spans 1 and 3 are duplicates: their results own separate maps.
		before := maps.Clone(got[3].Value)
		got[1].Value["changed"]++
		if !reflect.DeepEqual(got[3].Value, before) {
			t.Fatalf("workers=%d: changing one duplicate's result changed the other's", workers)
		}
	}
}

func TestFoldRowsBudgetMatchesAggregateRows(t *testing.T) {
	dir := foldFixture(t)
	d := verified(t, dir, OpenOptions{})
	spans := foldSpans(d)
	p, err := d.PlanRows(ctx, spans[2:3], []string{"size"}, nil)
	must(t, err)
	small, exact := p.ProjectedBytes()
	if !exact || small == 0 {
		t.Fatal(small, exact)
	}
	for _, check := range []bool{false, true} {
		for _, budget := range []int64{0, 1, small, 3 * small, -1} {
			opt := AggregateOptions{ValueBudgetBytes: budget, Workers: 2, CheckKeys: check}
			want, err := d.AggregateRows(ctx, spans, []Reduction{{Op: Sum, Column: "size"}}, opt, nil)
			must(t, err)
			// A run may cross a boundary of a refused span, not of an admitted one.
			var admitted []RowSpan
			for i, r := range want {
				if r[0].Status == AggregateExact {
					admitted = append(admitted, spans[i])
				}
			}
			got, err := FoldRows(ctx, d, spans, sizeFold(admitted, "size"), FoldOptions{ValueBudgetBytes: budget, Workers: 2, CheckKeys: check}, nil)
			must(t, err)
			refused := 0
			for i, r := range got {
				a := want[i][0]
				if r.Status != a.Status {
					t.Fatalf("check=%v budget=%d span %d: fold %v, aggregate %v", check, budget, i, r.Status, a.Status)
				}
				switch {
				case r.Status == AggregateExact:
					if r.Value.sum != a.Value.Int64() {
						t.Fatalf("budget=%d span %d: sum %d, want %d", budget, i, r.Value.sum, a.Value.Int64())
					}
				case r.Value != sizeTotals{}:
					t.Fatalf("budget=%d span %d: inexact value %+v", budget, i, r.Value)
				default:
					refused++
				}
			}
			if budget == -1 && refused != 0 || budget >= 0 && budget < small && refused != len(spans)-2 || budget == small && (refused == 0 || refused == len(spans)-2) {
				t.Fatalf("check=%v budget=%d: %d refused: %+v", check, budget, refused, got)
			}
		}
	}
	// A refused span stays refused where an admitted one overlaps it.
	nested := []RowSpan{spans[0], spans[2]}
	got, err := FoldRows(ctx, d, nested, sizeFold(nested[1:], "size"), FoldOptions{ValueBudgetBytes: small}, nil)
	must(t, err)
	if got[0].Status != AggregateBudgetExceeded || got[1].Status != AggregateExact || got[1].Value.rows != 10 {
		t.Fatalf("%+v", got)
	}

	// Without CheckKeys, a span over a part no read has checked is unknown.
	cold := open(t, dir, OpenOptions{})
	whole := []RowSpan{cold.r.rowSpan(0, 600), cold.r.rowSpan(0, 0)}
	for _, check := range []bool{false, true} {
		want := AggregateUnknown
		if check {
			want = AggregateExact
		}
		got, err := FoldRows(ctx, cold, whole, sizeFold(whole, "size"), FoldOptions{ValueBudgetBytes: -1, CheckKeys: check}, nil)
		must(t, err)
		if got[0].Status != want || check && got[0].Value.rows != 600 || got[1].Status != AggregateExact {
			t.Fatalf("check=%v: %+v", check, got)
		}
		if !check {
			agg, err := cold.AggregateRows(ctx, whole, []Reduction{{Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1}, nil)
			must(t, err)
			if agg[0][0].Status != AggregateUnknown {
				t.Fatal(agg)
			}
		}
	}
}

func TestFoldRowsColumns(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := foldSpans(d)
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 3} {
			opt := FoldOptions{ValueBudgetBytes: 0, Workers: workers, CheckKeys: check}
			counted, err := FoldRows(ctx, d, spans, sizeFold(spans), opt, nil)
			must(t, err)
			keys := Fold[int64]{
				Columns: []string{"size", "key"},
				Add: func(acc *int64, rows *ValueBatch) error {
					keys := rows.Column(1)
					for i := range rows.Len() {
						want := fmt.Sprintf("a/%04d", rows.start+int64(i))
						if !keys.Present() || string(keys.Bytes(i)) != want {
							return fmt.Errorf("row %d has key %q, want %q", rows.start+int64(i), keys.Bytes(i), want)
						}
					}
					*acc += int64(rows.Len())
					return nil
				},
				Merge: func(into, from *int64) { *into += *from },
			}
			opt.ValueBudgetBytes = -1
			keyed, err := FoldRows(ctx, d, spans, keys, opt, nil)
			must(t, err)
			for i := range spans {
				n := spans[i].End.ordinal - spans[i].Start.ordinal
				if counted[i].Status != AggregateExact || counted[i].Value.rows != n || keyed[i].Status != AggregateExact || keyed[i].Value != n {
					t.Fatalf("check=%v workers=%d span %d: %+v %+v, want %d rows", check, workers, i, counted[i], keyed[i], n)
				}
			}
		}
	}
}

func TestFoldRowsErrorsAndInvalid(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := foldSpans(d)
	for _, check := range []bool{false, true} {
		for _, fail := range []error{Stop, errors.New("add failed")} {
			fold := sizeFold(spans, "size")
			fold.Add = func(*sizeTotals, *ValueBatch) error { return fail }
			var st ReadStats
			out, err := FoldRows(ctx, d, spans, fold, FoldOptions{ValueBudgetBytes: -1, Workers: 4, CheckKeys: check}, &st)
			if out != nil || err != fail || st.ValuePagesDecoded == 0 {
				t.Fatalf("check=%v: %+v, %v, %+v", check, out, err, st)
			}
		}
	}

	noAdd := sizeFold(spans, "size")
	noAdd.Add = nil
	noMerge := sizeFold(spans, "size")
	noMerge.Merge = nil
	for _, tc := range []struct {
		fold Fold[sizeTotals]
		opt  FoldOptions
	}{
		{noAdd, FoldOptions{}},
		{noMerge, FoldOptions{}},
		{sizeFold(spans), FoldOptions{ValueBudgetBytes: -2}},
	} {
		out, err := FoldRows(ctx, d, spans, tc.fold, tc.opt, nil)
		if out != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%+v: %+v, %v", tc.opt, out, err)
		}
	}
	if out, err := FoldRows(ctx, d, []RowSpan{{}}, sizeFold(nil), FoldOptions{}, nil); out != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(out, err)
	}
}

func TestFoldRowsErrorPrecedence(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := []RowSpan{d.r.rowSpan(0, 600)}
	failed := errors.New("add failed")
	for _, check := range []bool{false, true} {
		opt := FoldOptions{ValueBudgetBytes: -1, Workers: 2, CheckKeys: check}

		// The first Add waits for an Add of the other worker, so one worker
		// returns Stop and the other an error, however they are scheduled.
		var calls atomic.Int32
		other := make(chan struct{})
		fold := sizeFold(nil, "size")
		fold.Add = func(*sizeTotals, *ValueBatch) error {
			switch calls.Add(1) {
			case 1:
				<-other
				return Stop
			case 2:
				close(other)
				return failed
			}
			return nil
		}
		out, err := FoldRows(ctx, d, spans, fold, opt, nil)
		if out != nil || err != failed {
			t.Fatalf("check=%v: Stop and an error: %+v, %v", check, out, err)
		}

		// The caller's context ends before Add returns Stop.
		c, cancel := context.WithCancel(ctx)
		fold.Add = func(*sizeTotals, *ValueBatch) error {
			cancel()
			return Stop
		}
		out, err = FoldRows(c, d, spans, fold, opt, nil)
		if out != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("check=%v: Stop and cancellation: %+v, %v", check, out, err)
		}
	}
}

// Budget refusal must not skip checking the refused spans' keys.
func TestFoldRowsBudgetRefusalChecksKeys(t *testing.T) {
	for _, tc := range []struct {
		name  string
		parts []testutil.Part
		edit  func(map[string]any)
		code  RefusalCode
	}{
		{"part-count", []testutil.Part{{Rows: testutil.Rows("a")}, {Rows: testutil.Rows("b")}, {Rows: testutil.Rows("c")}}, func(m map[string]any) { testutil.ManifestPart(m, 1)["rows"] = 100 }, CodePartMismatch},
		{"shard-order", []testutil.Part{{Rows: testutil.Rows("a", "b", "c", "z", "d", "e", "f", "zz"), GroupRows: 4}}, nil, CodeKeyOrder},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := write(t, testutil.Dataset{Parts: tc.parts})
			if tc.edit != nil {
				must(t, testutil.EditManifest(dir, tc.edit))
			}
			d := open(t, dir, OpenOptions{})
			p, err := d.Plan(ctx, []Selection{{}}, nil, nil)
			must(t, err)

			spans := p.InputRowSpans()
			out, err := FoldRows(ctx, d, spans, sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: 0, CheckKeys: true, Workers: 2}, nil)
			if out != nil || RefusalCodeOf(err) != tc.code {
				t.Fatalf("got %+v, %v; want %s and no results", out, err, tc.code)
			}
		})
	}
}
