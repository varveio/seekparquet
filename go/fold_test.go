// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/format"
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

// foldWant is the fold of the fold fixture's rows [start, end), from the
// rows the fixture writes: row i has size i, null when i%7 == 0.
func foldWant(start, end int64) sizeTotals {
	var want sizeTotals
	for i := start; i < end; i++ {
		want.rows++
		if i%7 == 0 {
			continue
		}
		want.sum += i
		want.max, want.seen = i, true
	}
	return want
}

func TestFoldRowsMatchesFixtureRows(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := foldSpans(d)
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 4} {
			var st ReadStats
			got, err := FoldRows(ctx, d, spans, sizeFold(spans, "size"), FoldOptions{Workers: workers, CheckKeys: check}, &st)
			must(t, err)
			if len(got) != len(spans) || st.ValuePagesDecoded == 0 || check != (st.KeyPagesDecoded > 0) {
				t.Fatalf("check=%v workers=%d: %d results, %+v", check, workers, len(got), st)
			}
			for i, r := range got {
				want := foldWant(spans[i].Start.ordinal, spans[i].End.ordinal)
				if r.Status != FoldExact || r.Value != want {
					t.Fatalf("check=%v workers=%d span %d: %+v, want %+v", check, workers, i, r, want)
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
			_, err := FoldRows(ctx, d, spans, fold, FoldOptions{Workers: workers, CheckKeys: check}, nil)
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
		got, err := FoldRows(ctx, d, spans, byName, FoldOptions{Workers: workers}, nil)
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
			if r.Status != FoldExact || !reflect.DeepEqual(r.Value, want) {
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

// TestFoldRowsGroupingOverflow: how a span's rows are grouped into
// accumulators depends on the spans folded with it, so ExampleFold sums
// size exactly and checks the range of the result. Sizes MaxInt64, 1 and
// -1 sum to MaxInt64; a partial sum of the first two overflows int64, and
// folding the span of the last two keeps them out of one accumulator.
func TestFoldRowsGroupingOverflow(t *testing.T) {
	d := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: []testutil.Row{
		{Key: []byte("a"), Size: ptr(int64(math.MaxInt64))},
		{Key: []byte("b"), Size: ptr(int64(1))},
		{Key: []byte("c"), Size: ptr(int64(-1))},
	}}}}), OpenOptions{})
	// ExampleFold's arithmetic of size.
	type totals struct{ size *big.Int }
	sum := Fold[totals]{
		Columns: []string{"size"},
		New:     func() totals { return totals{size: new(big.Int)} },
		Add: func(acc *totals, rows *ValueBatch) error {
			sizes := rows.Column(0)
			var n big.Int
			for i := range rows.Len() {
				acc.size.Add(acc.size, n.SetInt64(sizes.Int64(i)))
			}
			return nil
		},
		Merge: func(into, from *totals) { into.size.Add(into.size, from.size) },
	}
	all, last := d.r.rowSpan(0, 3), d.r.rowSpan(1, 3)
	for _, spans := range [][]RowSpan{{all}, {all, last}} {
		out, err := FoldRows(ctx, d, spans, sum, FoldOptions{}, nil)
		must(t, err)
		if r := out[0]; r.Status != FoldExact || !r.Value.size.IsInt64() || r.Value.size.Int64() != math.MaxInt64 {
			t.Fatalf("%d spans: %v %v, want exact %d", len(spans), r.Status, r.Value.size, int64(math.MaxInt64))
		}
	}
}

// budgetModel returns which spans a value budget admits, by the rule
// FoldOptions documents: in ascending order of their starts, a nonempty
// span is admitted when the projected bytes of its rows and those of the
// spans admitted before it are known and fit the budget. It prices each
// trial union with PlanRows and Plan.ProjectedBytes.
func budgetModel(t *testing.T, d *Dataset, spans []RowSpan, columns []string, budget int64) []FoldStatus {
	t.Helper()
	order := make([]int, len(spans))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(x, y int) int { return cmp.Compare(spans[x].Start.ordinal, spans[y].Start.ordinal) })
	want := make([]FoldStatus, len(spans))
	var admitted []RowSpan
	for _, i := range order {
		want[i] = FoldExact
		if spans[i].Start.ordinal == spans[i].End.ordinal {
			continue
		}
		p, err := d.PlanRows(ctx, append(slices.Clone(admitted), spans[i]), columns, nil)
		must(t, err)
		switch cost, exact := p.ProjectedBytes(); {
		case !exact:
			want[i] = FoldUnpriced
		case cost > budget:
			want[i] = FoldBudgetExceeded
		default:
			admitted = append(admitted, spans[i])
		}
	}
	return want
}

func TestFoldRowsBudget(t *testing.T) {
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
		for _, budget := range []int64{1, small, 3 * small, 0} {
			want := make([]FoldStatus, len(spans))
			for i := range want {
				want[i] = FoldExact
			}
			if budget > 0 {
				want = budgetModel(t, d, spans, []string{"size"}, budget)
			}
			// A run may cross a boundary of a refused span, not of an admitted one.
			var admitted []RowSpan
			for i, status := range want {
				if status == FoldExact {
					admitted = append(admitted, spans[i])
				}
			}
			got, err := FoldRows(ctx, d, spans, sizeFold(admitted, "size"), FoldOptions{ValueBudgetBytes: budget, Workers: 2, CheckKeys: check}, nil)
			must(t, err)
			refused := 0
			for i, r := range got {
				if r.Status != want[i] {
					t.Fatalf("check=%v budget=%d span %d: %v, want %v", check, budget, i, r.Status, want[i])
				}
				switch {
				case r.Status == FoldExact:
					if w := foldWant(spans[i].Start.ordinal, spans[i].End.ordinal); r.Value != w {
						t.Fatalf("budget=%d span %d: %+v, want %+v", budget, i, r.Value, w)
					}
				case r.Value != sizeTotals{}:
					t.Fatalf("budget=%d span %d: inexact value %+v", budget, i, r.Value)
				default:
					refused++
				}
			}
			if budget == 0 && refused != 0 || budget > 0 && budget < small && refused != len(spans)-2 || budget == small && (refused == 0 || refused == len(spans)-2) {
				t.Fatalf("check=%v budget=%d: %d refused: %+v", check, budget, refused, got)
			}
		}
	}
	// A refused span stays refused where an admitted one overlaps it.
	nested := []RowSpan{spans[0], spans[2]}
	got, err := FoldRows(ctx, d, nested, sizeFold(nested[1:], "size"), FoldOptions{ValueBudgetBytes: small}, nil)
	must(t, err)
	if got[0].Status != FoldBudgetExceeded || got[1].Status != FoldExact || got[1].Value != foldWant(120, 130) {
		t.Fatalf("%+v", got)
	}

	// Without CheckKeys, a span over a part no read has checked is unknown.
	cold := open(t, dir, OpenOptions{})
	whole := []RowSpan{cold.r.rowSpan(0, 600), cold.r.rowSpan(0, 0)}
	for _, check := range []bool{false, true} {
		want := FoldUnknown
		if check {
			want = FoldExact
		}
		got, err := FoldRows(ctx, cold, whole, sizeFold(whole, "size"), FoldOptions{CheckKeys: check}, nil)
		must(t, err)
		if got[0].Status != want || check && got[0].Value != foldWant(0, 600) || got[1].Status != FoldExact {
			t.Fatalf("check=%v: %+v", check, got)
		}
	}
}

// A zero value budget is unlimited, and a negative one is an invalid
// request that reads nothing.
func TestFoldRowsValueBudgetZeroAndNegative(t *testing.T) {
	dir := foldFixture(t)
	d := verified(t, dir, OpenOptions{})
	spans := foldSpans(d)
	var st ReadStats
	got, err := FoldRows(ctx, d, spans, sizeFold(spans, "size"), FoldOptions{ValueBudgetBytes: 0, Workers: 2}, &st)
	must(t, err)
	if st.ValuePagesDecoded == 0 {
		t.Fatalf("a zero budget read no value: %+v", st)
	}
	for i, r := range got {
		if w := foldWant(spans[i].Start.ordinal, spans[i].End.ordinal); r.Status != FoldExact || r.Value != w {
			t.Fatalf("zero budget, span %d: %+v, want %+v", i, r, w)
		}
	}
	cold := open(t, dir, OpenOptions{})
	for _, budget := range []int64{-1, math.MinInt64} {
		st := ReadStats{SourceReads: 9}
		calls := 0
		fold := sizeFold(nil, "size")
		fold.Add = func(*sizeTotals, *ValueBatch) error { calls++; return nil }
		out, err := FoldRows(ctx, cold, []RowSpan{cold.r.rowSpan(0, 600)}, fold, FoldOptions{ValueBudgetBytes: budget, CheckKeys: true}, &st)
		if out != nil || !errors.Is(err, ErrInvalidRequest) || calls != 0 || st != (ReadStats{}) {
			t.Fatalf("budget %d: %+v, %v, %d calls, %+v", budget, out, err, calls, st)
		}
	}
}

// TestFoldStatusString: each status prints its name, and a value past the
// last prints as a number.
func TestFoldStatusString(t *testing.T) {
	for s, want := range map[FoldStatus]string{
		FoldUnknown:        "unknown",
		FoldExact:          "exact",
		FoldBudgetExceeded: "budget_exceeded",
		FoldUnpriced:       "unpriced",
		FoldUnpriced + 1:   "FoldStatus(4)",
	} {
		if got := s.String(); got != want {
			t.Errorf("%d: %q, want %q", uint8(s), got, want)
		}
	}
}

// TestFoldRowsWorkersZeroAndNegative: zero workers folds on one worker; a
// negative count is ErrInvalidRequest before any I/O, with the statistics
// reset and no callback run.
func TestFoldRowsWorkersZeroAndNegative(t *testing.T) {
	dir := foldFixture(t)
	d := verified(t, dir, OpenOptions{})
	spans := foldSpans(d)
	got, err := FoldRows(ctx, d, spans, sizeFold(spans, "size"), FoldOptions{Workers: 0, CheckKeys: true}, nil)
	must(t, err)
	for i, r := range got {
		if w := foldWant(spans[i].Start.ordinal, spans[i].End.ordinal); r.Status != FoldExact || r.Value != w {
			t.Fatalf("zero workers, span %d: %+v, want %+v", i, r, w)
		}
	}
	cold := open(t, dir, OpenOptions{})
	for _, workers := range []int{-1, math.MinInt} {
		st := ReadStats{SourceReads: 9}
		calls := 0
		fold := sizeFold(nil, "size")
		fold.New = func() sizeTotals { calls++; return sizeTotals{} }
		fold.Add = func(*sizeTotals, *ValueBatch) error { calls++; return nil }
		fold.Merge = func(*sizeTotals, *sizeTotals) { calls++ }
		out, err := FoldRows(ctx, cold, []RowSpan{cold.r.rowSpan(0, 600)}, fold, FoldOptions{Workers: workers, CheckKeys: true}, &st)
		if out != nil || !errors.Is(err, ErrInvalidRequest) || calls != 0 || st != (ReadStats{}) {
			t.Fatalf("%d workers: %+v, %v, %d calls, %+v", workers, out, err, calls, st)
		}
	}
}

func TestFoldRowsColumns(t *testing.T) {
	d := verified(t, foldFixture(t), OpenOptions{})
	spans := foldSpans(d)
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 3} {
			opt := FoldOptions{Workers: workers, CheckKeys: check}
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
			keyed, err := FoldRows(ctx, d, spans, keys, opt, nil)
			must(t, err)
			for i := range spans {
				n := spans[i].End.ordinal - spans[i].Start.ordinal
				if counted[i].Status != FoldExact || counted[i].Value.rows != n || keyed[i].Status != FoldExact || keyed[i].Value != n {
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
			out, err := FoldRows(ctx, d, spans, fold, FoldOptions{Workers: 4, CheckKeys: check}, &st)
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
		{sizeFold(spans), FoldOptions{ValueBudgetBytes: -1}},
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
		opt := FoldOptions{Workers: 2, CheckKeys: check}

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

// Budget refusal must not skip checking the refused spans' keys: these
// exercise the key-only scan of the refused spans.
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
			if tc.edit != nil && d.r.parts[1].ready.Load() {
				t.Fatal("interior part must remain cold")
			}

			spans := p.InputRowSpans()
			out, err := FoldRows(ctx, d, spans, sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: 1, CheckKeys: true, Workers: 2}, nil)
			if out != nil || RefusalCodeOf(err) != tc.code {
				t.Fatalf("got %+v, %v; want %s and no results", out, err, tc.code)
			}
		})
	}
}

// oracleTotals is a fold of the oracle fixture's rows: their count, the
// sum, nulls, absent values and maximum of size, the maximum name, and the
// absent and null values of a column no part has.
type oracleTotals struct {
	rows, sum, nulls, absent   int64
	maxSize                    int64
	sizeSeen                   bool
	maxName                    string
	nameSeen                   bool
	missingAbsent, missingNull int64
}

// oracleFold folds the columns size, name and missing into oracleTotals.
func oracleFold() Fold[oracleTotals] {
	return Fold[oracleTotals]{
		Columns: []string{"size", "name", "missing"},
		Add: func(acc *oracleTotals, rows *ValueBatch) error {
			sizes, names, missing := rows.Column(0), rows.Column(1), rows.Column(2)
			for i := range rows.Len() {
				acc.rows++
				switch {
				case !sizes.Present():
					acc.absent++
				case sizes.IsNull(i):
					acc.nulls++
				default:
					n := sizes.Int64(i)
					acc.sum += n
					if !acc.sizeSeen || n > acc.maxSize {
						acc.maxSize, acc.sizeSeen = n, true
					}
				}
				if names.Present() && !names.IsNull(i) {
					if name := string(names.Bytes(i)); !acc.nameSeen || name > acc.maxName {
						acc.maxName, acc.nameSeen = name, true
					}
				}
				switch {
				case !missing.Present():
					acc.missingAbsent++
				case missing.IsNull(i):
					acc.missingNull++
				}
			}
			return nil
		},
		Merge: func(into, from *oracleTotals) {
			into.rows += from.rows
			into.sum += from.sum
			into.nulls += from.nulls
			into.absent += from.absent
			if from.sizeSeen && (!into.sizeSeen || from.maxSize > into.maxSize) {
				into.maxSize, into.sizeSeen = from.maxSize, true
			}
			if from.nameSeen && (!into.nameSeen || from.maxName > into.maxName) {
				into.maxName, into.nameSeen = from.maxName, true
			}
			into.missingAbsent += from.missingAbsent
			into.missingNull += from.missingNull
		},
	}
}

// Ported from swath TestAggregatesMatchOracle, TestTotalsMatchSerial and
// TestAggregatesAgainstFullScan: key selections, resolved by Plan and
// folded over their input spans, against the raw fixture rows.
func TestFoldRowsSelectionsOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 37))
	for _, repeated := range []bool{false, true} {
		rows := make([]testutil.Row, 600)
		for i := range rows {
			k := i
			if repeated {
				k -= i % 3
			}
			rows[i] = testutil.Row{Key: []byte(fmt.Sprintf("p%02d/x%03d", k/60, k)), Size: ptr(int64(rng.IntN(1000) - 500))}
			if i%7 == 0 {
				rows[i].Size = nil
			}
			if i%3 != 0 {
				rows[i].Name = ptr(fmt.Sprintf("name%03d", rng.IntN(500)))
			}
		}
		dir := write(t, testutil.Dataset{RepeatedKeys: repeated, Parts: []testutil.Part{{Rows: rows[:300], GroupRows: 31, PageBytes: 256}, {Rows: rows[300:], GroupRows: 29, PageBytes: 256}}})
		d := verified(t, dir, OpenOptions{})
		sels := []Selection{{}, {Span: PrefixSpan([]byte("p01/"))}, {Span: PrefixSpan([]byte("p01/"))}, {Span: KeySpan([]byte("missing"))}}
		for range 30 {
			a, b := rng.IntN(len(rows)), rng.IntN(len(rows))
			if a > b {
				a, b = b, a
			}
			sel := Selection{Span: Span{Start: rows[a].Key, End: rows[b].Key, StartExclusive: rng.IntN(2) == 0}}
			if rng.IntN(2) == 0 {
				sel.Cursor = &Cursor{Key: rows[(a+b)/2].Key, Offset: int64(rng.IntN(4))}
			}
			sels = append(sels, sel)
		}
		want := make([]oracleTotals, len(sels))
		for i, sel := range sels {
			w := &want[i]
			for k, row := range rows {
				if !selects(sel.Span, row.Key) {
					continue
				}
				off := int64(0)
				if repeated {
					off = int64(k % 3)
				}
				if c := sel.Cursor; c != nil {
					cmp := bytes.Compare(row.Key, c.Key)
					if cmp < 0 || cmp == 0 && off <= c.Offset {
						continue
					}
				}
				w.rows++
				w.missingAbsent++
				if row.Size == nil {
					w.nulls++
				} else {
					w.sum += *row.Size
					if !w.sizeSeen || *row.Size > w.maxSize {
						w.maxSize, w.sizeSeen = *row.Size, true
					}
				}
				if row.Name != nil && (!w.nameSeen || *row.Name > w.maxName) {
					w.maxName, w.nameSeen = *row.Name, true
				}
			}
		}
		p, err := d.Plan(ctx, sels, nil, nil)
		must(t, err)
		spans := p.InputRowSpans()
		for _, check := range []bool{false, true} {
			for _, workers := range []int{1, 4} {
				for _, budget := range []int64{0, 1 << 20} {
					got, err := FoldRows(ctx, d, spans, oracleFold(), FoldOptions{ValueBudgetBytes: budget, Workers: workers, CheckKeys: check}, nil)
					must(t, err)
					for i := range sels {
						if got[i].Status != FoldExact || got[i].Value != want[i] {
							t.Fatalf("repeated=%v check=%v workers=%d budget=%d input=%d\ngot=%+v\nwant=%+v", repeated, check, workers, budget, i, got[i], want[i])
						}
					}
				}
			}
		}
	}
}

// Ported from swath's counts/no-read, budget rejection and bounded
// index-memory regressions: a refused span reads no value and calls no
// Add; counts plan without reads; page locations not kept within
// OffsetIndexMemory leave a chunk without an OffsetIndex unpriced.
func TestFoldRowsBudgetsAndCounts(t *testing.T) {
	d, p := parallelFixture(t, 600, 30, false)
	spans := p.InputRowSpans()
	for _, budget := range []int64{1, 15, 1 << 20, 0} {
		var st ReadStats
		var calls atomic.Int64
		fold := sizeFold(spans, "size")
		add := fold.Add
		fold.Add = func(acc *sizeTotals, rows *ValueBatch) error { calls.Add(1); return add(acc, rows) }
		got, err := FoldRows(ctx, d, spans, fold, FoldOptions{ValueBudgetBytes: budget}, &st)
		must(t, err)
		if budget > 0 && budget < minValuePageBytes {
			if got[0].Status != FoldBudgetExceeded || st.SourceReads != 0 || calls.Load() != 0 {
				t.Fatalf("budget=%d: %+v %+v, %d calls", budget, got, st, calls.Load())
			}
		} else if got[0].Status != FoldExact || got[0].Value.rows != 600 || got[0].Value.sum != 600*599/2 || st.KeyPagesDecoded != 0 {
			t.Fatalf("budget=%d: %+v %+v", budget, got, st)
		}
	}
	var st ReadStats
	counted, err := d.PlanRows(ctx, spans, nil, &st)
	must(t, err)
	if n, exact := counted.Count(); n != 600 || !exact || st != (ReadStats{}) {
		t.Fatal(n, exact, st)
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a", "b", "c", "d"), GroupRows: 2}}})
	for _, indexed := range []bool{true, false} {
		if !indexed {
			rewrite(t, dir, 0, func(m *format.FileMetaData) {
				for g := range m.RowGroups {
					for c := 1; c < len(m.RowGroups[g].Columns); c++ {
						cc := &m.RowGroups[g].Columns[c]
						cc.OffsetIndexOffset, cc.OffsetIndexLength = 0, 0
					}
				}
			})
		}
		ds := verified(t, dir, OpenOptions{Limits: Limits{OffsetIndexMemory: 1}})
		planned, err := ds.Plan(ctx, []Selection{{}, {Span: KeySpan([]byte("b"))}}, nil, nil)
		must(t, err)
		for range 2 {
			got, err := FoldRows(ctx, ds, planned.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: 1 << 20}, nil)
			must(t, err)
			want := FoldExact
			if !indexed {
				want = FoldUnpriced
			}
			if got[0].Status != want {
				t.Fatalf("indexed=%v: %+v", indexed, got)
			}
		}
	}
}

func TestFoldRowsAbsentEmptyAndUnknown(t *testing.T) {
	type keyOnly struct {
		Key []byte `parquet:"key"`
	}
	rows := testutil.Rows("a", "b")
	rows[1].Size = nil
	d := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows}, {Rows: []keyOnly{{[]byte("c")}, {[]byte("d")}}}}}), OpenOptions{})
	p, err := d.Plan(ctx, []Selection{{}, {Span: KeySpan([]byte("none"))}}, nil, nil)
	must(t, err)
	got, err := FoldRows(ctx, d, p.InputRowSpans(), oracleFold(), FoldOptions{Workers: 3}, nil)
	must(t, err)
	want := oracleTotals{rows: 4, nulls: 1, absent: 2, sizeSeen: true, maxName: "name-1", nameSeen: true, missingAbsent: 4}
	if got[0].Status != FoldExact || got[0].Value != want || got[1].Status != FoldExact || got[1].Value != (oracleTotals{}) {
		t.Fatalf("%+v, want %+v", got, want)
	}
	// Plan touches the first part only: the interior unchecked part keeps
	// the fold unknown, and checking keys refuses it.
	coldDir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a")}, {Rows: testutil.Rows("b")}, {Rows: testutil.Rows("c")}}})
	must(t, testutil.EditManifest(coldDir, func(m map[string]any) { testutil.ManifestPart(m, 1)["rows"] = 100 }))
	cold := open(t, coldDir, OpenOptions{})
	p, err = cold.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	result, err := FoldRows(ctx, cold, p.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{}, nil)
	must(t, err)
	if result[0].Status != FoldUnknown || result[0].Value != (sizeTotals{}) {
		t.Fatal(result)
	}
	strong, err := FoldRows(ctx, cold, p.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{CheckKeys: true}, nil)
	if strong != nil || RefusalCodeOf(err) == "" {
		t.Fatal(strong, err)
	}
}

func TestFoldRowsCancellationAndClose(t *testing.T) {
	d, p := parallelFixture(t, 100, 10, false)
	spans := p.InputRowSpans()
	c, cancel := context.WithCancel(ctx)
	cancel()
	if out, err := FoldRows(c, d, spans, sizeFold(nil, "size"), FoldOptions{}, nil); out != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(out, err)
	}
	c, cancel = context.WithCancel(ctx)
	fold := sizeFold(nil, "size")
	fold.Add = func(*sizeTotals, *ValueBatch) error { cancel(); return nil }
	if out, err := FoldRows(c, d, spans, fold, FoldOptions{Workers: 3}, nil); out != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(out, err)
	}
	must(t, d.Close())
	if out, err := FoldRows(ctx, d, spans, sizeFold(nil, "size"), FoldOptions{}, nil); out != nil || !errors.Is(err, ErrClosed) {
		t.Fatal(out, err)
	}
}

// Ported from swath TestAggregatesSumsStayWithinBudget: mixed admission,
// shared sibling pages, exact totals and actual value bytes within budget.
func TestFoldRowsBudgetExtentAccounting(t *testing.T) {
	var keys []string
	for p := range 6 {
		for i := range 90 {
			keys = append(keys, fmt.Sprintf("p%d/%04d", p, i))
		}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(keys[:270]...), GroupRows: 90, PageBytes: 64}, {Rows: testutil.Rows(keys[270:]...), GroupRows: 90, PageBytes: 64}}})
	var sels []Selection
	for p := range 6 {
		sels = append(sels, Selection{Span: PrefixSpan([]byte(fmt.Sprintf("p%d/", p)))})
	}
	fresh := verified(t, dir, OpenOptions{})
	priced, err := fresh.Plan(ctx, sels, []string{"size"}, nil)
	must(t, err)
	all, exact := priced.ProjectedBytes()
	if !exact {
		t.Fatal("fixture unpriced")
	}
	mixed := false
	for _, budget := range []int64{1, all / 4, all / 2, all - 1, all, 1 << 30} {
		d, src, _, extents := openOracle(t, dir, "size")
		must(t, d.Verify(ctx, nil))
		p, err := d.Plan(ctx, sels, nil, nil)
		must(t, err)
		src.take()
		got, err := FoldRows(ctx, d, p.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: budget}, nil)
		must(t, err)
		read := chunkBytes(t, "size", src.take(), extents)
		if read > budget {
			t.Fatalf("budget=%d read=%d", budget, read)
		}
		count := 0
		for i, r := range got {
			if r.Status == FoldExact {
				count++
				base := int64((i % 3) * 90)
				if want := 90*base + 90*89/2; r.Value.rows != 90 || r.Value.sum != want {
					t.Fatalf("input=%d got=%+v want 90 rows of size %d", i, r, want)
				}
			}
		}
		if count > 0 && count < 6 {
			mixed = true
		}
		if budget == 1 && count != 0 || budget >= all && count != 6 || budget == all-1 && count == 6 {
			t.Fatalf("budget=%d all=%d admitted=%d", budget, all, count)
		}
	}
	if !mixed {
		t.Fatal("no mixed admission")
	}
	ds := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a/1", "b/1")}}}), OpenOptions{})
	p, err := ds.Plan(ctx, []Selection{{Span: PrefixSpan([]byte("a/"))}}, []string{"size"}, nil)
	must(t, err)
	cost, _ := p.ProjectedBytes()
	both, err := ds.Plan(ctx, []Selection{{Span: PrefixSpan([]byte("a/"))}, {Span: PrefixSpan([]byte("b/"))}}, nil, nil)
	must(t, err)
	got, err := FoldRows(ctx, ds, both.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: cost}, nil)
	must(t, err)
	if got[0].Status != FoldExact || got[1].Status != FoldExact || got[1].Value.sum != 1 {
		t.Fatal(got)
	}
}

// A finite budget discovers its spans' parts in order: refusing the first
// part's work opens no later part's value pages.
func TestFoldRowsIncrementalBudget(t *testing.T) {
	var parts []testutil.Part
	for p := range 3 {
		var keys []string
		for i := range 200 {
			keys = append(keys, fmt.Sprintf("p%d/%04d", p, i))
		}
		parts = append(parts, testutil.Part{Rows: testutil.Rows(keys...), GroupRows: 50, PageBytes: 64})
	}
	d := verified(t, write(t, testutil.Dataset{Parts: parts}), OpenOptions{})
	got, err := FoldRows(ctx, d, []RowSpan{d.r.rowSpan(0, 600)}, sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: 64}, nil)
	must(t, err)
	if got[0].Status != FoldBudgetExceeded {
		t.Fatal(got)
	}
	for _, p := range d.r.parts[1:] {
		leaf := p.leaves["size"]
		for _, locs := range p.locs {
			if locs[leaf.index].Load() != nil {
				t.Fatal("budget discovered a later part's value pages")
			}
		}
	}
}

// Ported from swath TestAggregatesSumsPublishNothingOnFailure and
// TestTotalsCancelledDuringScan, using SeekParquet's read-hook fixture.
func TestFoldRowsSourceFailureAndCancellation(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a/1", "a/2", "b/1", "b/2"), GroupRows: 2, PageBytes: 32}}})
	for _, check := range []bool{false, true} {
		for _, warm := range []bool{false, true} {
			for _, cancelled := range []bool{false, true} {
				src := &readHookSource{Source: dirOf(t, dir)}
				ds, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
				must(t, err)
				if warm {
					must(t, ds.Verify(ctx, nil))
				}
				c, cancel := context.WithCancel(ctx)
				failure := errors.New("fold storage failure")
				src.read = func(string, int, int64) error {
					if cancelled {
						cancel()
						return context.Canceled
					}
					return failure
				}
				var st ReadStats
				got, err := FoldRows(c, ds, []RowSpan{ds.r.rowSpan(0, 4)}, sizeFold(nil, "size"), FoldOptions{Workers: 3, CheckKeys: check}, &st)
				cancel()
				must(t, ds.Close())
				if !warm && !check {
					// The cold part is unchecked: the span is unknown, with no work.
					if err != nil || got[0].Status != FoldUnknown || st.SourceReads != 0 {
						t.Fatalf("check=%v warm=%v cancelled=%v: %+v %v %+v", check, warm, cancelled, got, err, st)
					}
					continue
				}
				want := failure
				if cancelled {
					want = context.Canceled
				}
				if got != nil || !errors.Is(err, want) || st.SourceReads == 0 {
					t.Fatalf("check=%v warm=%v cancelled=%v: %+v %v %+v", check, warm, cancelled, got, err, st)
				}
			}
		}
	}
}

// A column no checked part holds has no value work: the smallest budget
// admits it, and its fold reads nothing.
func TestFoldRowsBudgetAbsentColumn(t *testing.T) {
	d, p := parallelFixture(t, 100, 10, false)
	missing := Fold[int64]{
		Columns: []string{"missing"},
		Add: func(acc *int64, rows *ValueBatch) error {
			if rows.Column(0).Present() {
				return errors.New("an absent column is present")
			}
			*acc += int64(rows.Len())
			return nil
		},
		Merge: func(into, from *int64) { *into += *from },
	}
	var st ReadStats
	counted, err := FoldRows(ctx, d, p.InputRowSpans(), missing, FoldOptions{ValueBudgetBytes: 1}, &st)
	must(t, err)
	if counted[0].Status != FoldExact || counted[0].Value != 100 || st.SourceReads != 0 {
		t.Fatal(counted, st)
	}
}

func TestFoldRowsBudgetAbsentPartFloor(t *testing.T) {
	type keyOnly struct {
		Key []byte `parquet:"key"`
	}
	var a, c []string
	for i := range 100 {
		a = append(a, fmt.Sprintf("a/%03d", i))
		c = append(c, fmt.Sprintf("c/%03d", i))
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: testutil.Rows(a...)},
		{Rows: []keyOnly{{[]byte("b")}}},
		{Rows: testutil.Rows(c...)},
		{Rows: []keyOnly{{[]byte("d")}}},
	}})
	priced := verified(t, dir, OpenOptions{})
	p, err := priced.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
	must(t, err)
	budget, exact := p.ProjectedBytes()
	if !exact || budget <= 3*minValuePageBytes {
		t.Fatal(budget, exact)
	}
	d := open(t, dir, OpenOptions{})
	p, err = d.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	// The final checked, absent part has no floor of its own. A stale floor
	// from the cold interior absent part must not refuse it at the exact budget.
	_, err = d.Plan(ctx, []Selection{{Span: KeySpan([]byte("d"))}}, nil, nil)
	must(t, err)
	if d.r.parts[1].ready.Load() {
		t.Fatal("interior absent part must be cold")
	}
	got, err := FoldRows(ctx, d, p.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: budget, CheckKeys: true}, nil)
	must(t, err)
	if got[0].Status != FoldExact || got[0].Value.sum != 2*100*99/2 {
		t.Fatal(got)
	}
}

// Checking keys under a finite budget that admits every span reads each
// key once: a fold with a column decodes the key pages a fold with none
// does.
func TestFoldRowsFiniteBudgetReadsKeysOnce(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a", "b", "c", "d", "e", "f"), GroupRows: 2}}})
	for _, workers := range []int{1, 2} {
		var keys, values ReadStats
		for _, countOnly := range []bool{true, false} {
			d := verified(t, dir, OpenOptions{})
			fold, st := sizeFold(nil), &keys
			if !countOnly {
				fold, st = sizeFold(nil, "size"), &values
			}
			got, err := FoldRows(ctx, d, []RowSpan{d.r.rowSpan(0, 6)}, fold, FoldOptions{ValueBudgetBytes: 1 << 20, CheckKeys: true, Workers: workers}, st)
			must(t, err)
			if got[0].Status != FoldExact || got[0].Value.rows != 6 || !countOnly && got[0].Value.sum != 15 {
				t.Fatal(got)
			}
		}
		if keys.KeyPagesDecoded == 0 || values.ValuePagesDecoded == 0 || values.KeyPagesDecoded != keys.KeyPagesDecoded || values.KeysDecoded != keys.KeysDecoded {
			t.Fatalf("workers=%d keys=%+v keys and values=%+v", workers, keys, values)
		}
	}
}

// Ported from swath 0d204aa4 TestTotalsIgnoreManifestBoundExcludingKey.
func TestFoldRowsIgnoreManifestBoundExcludingKey(t *testing.T) {
	var keys []string
	for _, prefix := range []string{"a/", "b/"} {
		for i := range 10 {
			keys = append(keys, fmt.Sprintf("%s%04d", prefix, i))
		}
	}
	rows := testutil.Rows(keys...)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows[:10]}, {Rows: rows[10:]}}})
	must(t, testutil.EditManifest(dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["max_key_base64"] = b64("a/0005") }))
	sels := []Selection{{Span: PrefixSpan([]byte("a/0009"))}, {Span: PrefixSpan([]byte("a/"))}, {Span: PrefixSpan([]byte("a/0008"))}}
	for _, budget := range []int64{0, 1 << 20} {
		d := open(t, dir, OpenOptions{})
		p, err := d.Plan(ctx, sels, nil, nil)
		var got []FoldResult[sizeTotals]
		if err == nil {
			got, err = FoldRows(ctx, d, p.InputRowSpans(), sizeFold(nil, "size"), FoldOptions{ValueBudgetBytes: budget, CheckKeys: true}, nil)
		}
		if got != nil || RefusalCodeOf(err) != CodeBoundsMismatch {
			t.Fatalf("budget=%d: %+v, %v; want bounds_mismatch and no results", budget, got, err)
		}
	}
}
