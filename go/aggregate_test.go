// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// Ported from swath TestAggregatesMatchOracle, TestTotalsMatchSerial and
// TestAggregatesAgainstFullScan; use raw fixture rows as the independent oracle.
func TestAggregateOracle(t *testing.T) {
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
		reductions := []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}, {Op: Max, Column: "size"}, {Op: CountNulls, Column: "size"}, {Op: CountAbsent, Column: "size"}, {Op: Max, Column: "name"}, {Op: CountAbsent, Column: "absent"}, {Op: CountNulls, Column: "absent"}}
		want := aggregateResults(len(sels), len(reductions))
		for i, sel := range sels {
			var n, sum, nulls int64
			var maxSize *int64
			var maxName *string
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
				n++
				if row.Size == nil {
					nulls++
				} else {
					sum += *row.Size
					if maxSize == nil || *row.Size > *maxSize {
						maxSize = row.Size
					}
				}
				if row.Name != nil && (maxName == nil || *row.Name > *maxName) {
					maxName = row.Name
				}
			}
			values := []Value{Int64Value(n), Int64Value(sum), {}, Int64Value(nulls), Int64Value(0), {}, Int64Value(n), Int64Value(0)}
			if maxSize != nil {
				values[2] = Int64Value(*maxSize)
			}
			if maxName != nil {
				values[5] = ByteArrayValue([]byte(*maxName))
			}
			for j, v := range values {
				want[i][j] = AggregateResult{v, AggregateExact}
			}
		}
		for _, check := range []bool{false, true} {
			for _, workers := range []int{1, 4} {
				for _, budget := range []int64{-1, 1 << 20} {
					var st ReadStats
					got, err := d.Aggregate(ctx, sels, reductions, AggregateOptions{ValueBudgetBytes: budget, Workers: workers, CheckKeys: check}, &st)
					must(t, err)
					if !reflect.DeepEqual(got, want) {
						for i := range got {
							if !reflect.DeepEqual(got[i], want[i]) {
								t.Fatalf("repeated=%v check=%v workers=%d input=%d\ngot=%+v\nwant=%+v", repeated, check, workers, i, got[i], want[i])
							}
						}
					}
				}
			}
		}
	}
}

type aggregateNumbers struct {
	Key      []byte  `parquet:"key"`
	Signed   int64   `parquet:"signed"`
	Unsigned uint64  `parquet:"unsigned"`
	Small    int32   `parquet:"small"`
	USmall   uint32  `parquet:"usmall"`
	Millis   int64   `parquet:"millis,timestamp(millisecond)"`
	Micros   int64   `parquet:"micros,timestamp(microsecond)"`
	Nanos    int64   `parquet:"nanos,timestamp(nanosecond)"`
	Text     string  `parquet:"text"`
	Fixed    [4]byte `parquet:"fixed"`
	Bad      float64 `parquet:"bad"`
}

// Generalizes swath TestObjectSumChecksArithmetic, TestTotalsSizeOverflowFails,
// TestAggregatesSumOverflowLeavesBytesNil and TestSumOverflowNestedStaysExact.
func TestAggregateArithmeticAndTypes(t *testing.T) {
	rows := []aggregateNumbers{
		{Key: []byte("a"), Signed: math.MaxInt64, Unsigned: math.MaxUint64, Small: math.MinInt32, USmall: math.MaxUint32, Millis: -10, Micros: -10000, Nanos: -10000000, Text: "z", Fixed: [4]byte{0, 0, 0, 1}},
		{Key: []byte("b"), Signed: math.MaxInt64, Unsigned: 1, Small: math.MaxInt32, USmall: 1, Millis: -1, Micros: -1000, Nanos: -1000000, Text: "a", Fixed: [4]byte{0, 0, 0, 3}},
		{Key: []byte("c"), Signed: -math.MaxInt64, Text: "c", Fixed: [4]byte{0, 0, 0, 2}},
	}
	d := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, GroupRows: 1}}}), OpenOptions{})
	reductions := []Reduction{{Op: Sum, Column: "signed"}, {Op: Sum, Column: "unsigned"}, {Op: Max, Column: "unsigned"}, {Op: Sum, Column: "small"}, {Op: Max, Column: "usmall"}, {Op: Max, Column: "millis"}, {Op: Max, Column: "micros"}, {Op: Max, Column: "nanos"}, {Op: Max, Column: "text"}, {Op: Sum, Column: "usmall"}, {Op: Max, Column: "fixed"}}
	sels := []Selection{{}, {Span: Span{End: []byte("c")}}, {Span: KeySpan([]byte("a"))}}
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 3} {
			got, err := d.Aggregate(ctx, sels, reductions, AggregateOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: check}, nil)
			must(t, err)
			if got[1][5].Value.Int64() != -1 || got[1][6].Value.Int64() != -1000 || got[1][7].Value.Int64() != -1000000 || got[0][9].Value.Uint64() != 1<<32 || got[0][10].Value.Kind() != KindFixedLenByteArray || !bytes.Equal(got[0][10].Value.Bytes(), []byte{0, 0, 0, 3}) {
				t.Fatalf("type results: %+v", got)
			}
			if got[0][0].Value.Int64() != math.MaxInt64 || got[0][0].Status != AggregateExact || got[1][0].Status != AggregateOverflow || got[0][1].Status != AggregateOverflow || got[2][1].Value.Uint64() != math.MaxUint64 || got[0][2].Value.Uint64() != math.MaxUint64 || got[0][3].Value.Int64() != -1 || got[0][4].Value.Uint32() != math.MaxUint32 || string(got[0][8].Value.Bytes()) != "z" {
				t.Fatalf("check=%v workers=%d: %+v", check, workers, got)
			}
			for _, r := range []Reduction{{Op: Sum, Column: "millis"}, {Op: Sum, Column: "text"}, {Op: Max, Column: "bad"}, {Op: Sum, Column: "bad"}} {
				out, err := d.Aggregate(ctx, []Selection{{}}, []Reduction{r}, AggregateOptions{ValueBudgetBytes: -1, CheckKeys: check}, nil)
				if !errors.Is(err, ErrInvalidRequest) || out != nil {
					t.Fatalf("unsupported %+v: %+v %v", r, out, err)
				}
			}
		}
	}
	// Underflow and cancellation after underflow, across separate shards.
	for _, values := range [][]int64{{math.MinInt64, -1}, {math.MinInt64, -1, 1}} {
		rs := testutil.Rows("a", "b", "c")[:len(values)]
		for i, v := range values {
			rs[i].Size = ptr(v)
		}
		ds := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rs, GroupRows: 1}}}), OpenOptions{})
		got, err := ds.Aggregate(ctx, []Selection{{}}, []Reduction{{Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, Workers: 3}, nil)
		must(t, err)
		want := AggregateOverflow
		if len(values) == 3 {
			want = AggregateExact
		}
		if got[0][0].Status != want || want == AggregateExact && got[0][0].Value.Int64() != math.MinInt64 {
			t.Fatal(got)
		}
	}
}

// swath counts/no-read, budget rejection and bounded index-memory regressions.
func TestAggregateBudgets(t *testing.T) {
	d, p := parallelFixture(t, 600, 30, false)
	spans := p.InputRowSpans()
	for _, budget := range []int64{0, 1, 15, 1 << 20, -1} {
		var st ReadStats
		got, err := d.AggregateRows(ctx, spans, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: budget}, &st)
		must(t, err)
		if got[0][0].Status != AggregateExact || got[0][0].Value.Int64() != 600 {
			t.Fatal(got)
		}
		if budget >= 0 && budget < 16 {
			if got[0][1].Status != AggregateBudgetExceeded || st.SourceReads != 0 {
				t.Fatalf("budget=%d: %+v %+v", budget, got, st)
			}
		} else if got[0][1].Status != AggregateExact || got[0][1].Value.Int64() != 600*599/2 || st.KeyPagesDecoded != 0 {
			t.Fatalf("%+v %+v", got, st)
		}
	}
	var st ReadStats
	_, err := d.AggregateRows(ctx, spans, []Reduction{{Op: CountRows}}, AggregateOptions{}, &st)
	must(t, err)
	if st.SourceReads != 0 || st.KeyPagesDecoded != 0 || st.ValuePagesDecoded != 0 {
		t.Fatal(st)
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
		for range 2 {
			got, err := ds.Aggregate(ctx, []Selection{{}, {Span: KeySpan([]byte("b"))}}, []Reduction{{Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: 1 << 20}, nil)
			must(t, err)
			want := AggregateExact
			if !indexed {
				want = AggregateUnpriced
			}
			if got[0][0].Status != want {
				t.Fatalf("indexed=%v: %+v", indexed, got)
			}
		}
	}
}

func TestAggregateAbsentEmptyAndUnknown(t *testing.T) {
	type keyOnly struct {
		Key []byte `parquet:"key"`
	}
	rows := testutil.Rows("a", "b")
	rows[1].Size = nil
	d := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows}, {Rows: []keyOnly{{[]byte("c")}, {[]byte("d")}}}}}), OpenOptions{})
	got, err := d.Aggregate(ctx, []Selection{{}, {Span: KeySpan([]byte("none"))}}, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}, {Op: CountNulls, Column: "size"}, {Op: CountAbsent, Column: "size"}, {Op: Max, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, Workers: 3}, nil)
	must(t, err)
	if got[0][0].Value.Int64() != 4 || got[0][2].Value.Int64() != 1 || got[0][3].Value.Int64() != 2 || !got[1][4].Value.IsNull() || got[1][4].Status != AggregateExact || got[1][1].Value.Int64() != 0 {
		t.Fatal(got)
	}
	// Plan touches the first part only: the interior unverified part keeps counts unknown.
	coldDir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a")}, {Rows: testutil.Rows("b")}, {Rows: testutil.Rows("c")}}})
	must(t, testutil.EditManifest(coldDir, func(m map[string]any) { testutil.ManifestPart(m, 1)["rows"] = 100 }))
	cold := open(t, coldDir, OpenOptions{})
	result, err := cold.Aggregate(ctx, []Selection{{}}, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1}, nil)
	must(t, err)
	if result[0][0].Status != AggregateUnknown || result[0][1].Status != AggregateUnknown {
		t.Fatal(result)
	}
	strong, err := cold.Aggregate(ctx, []Selection{{}}, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, CheckKeys: true}, nil)
	if strong != nil || RefusalCodeOf(err) == "" {
		t.Fatal(strong, err)
	}
}

func TestAggregateFilter(t *testing.T) {
	d, p := parallelFixture(t, 300, 30, true)
	sels := []Selection{{}, {Span: PrefixSpan([]byte("a/00000"))}, {}}
	reductions := []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}, {Op: Max, Column: "size"}, {Op: CountNulls, Column: "name"}}
	for _, check := range []bool{false, true} {
		for _, workers := range []int{1, 4} {
			var calls, visited atomic.Int64
			filter := &BatchFilter{Columns: []string{"size", "name", "size"}, Apply: func(b *ValueBatch, keep []bool) error {
				calls.Add(1)
				visited.Add(int64(b.Len()))
				if len(b.values) != 3 {
					return errors.New("filter saw undeclared columns")
				}
				for i := range b.Len() {
					if !keep[i] || b.Column(0).Int64(i) != b.Column(2).Int64(i) {
						return errors.New("mask or projection")
					}
					keep[i] = b.Column(0).Int64(i)%2 == 0
				}
				return nil
			}}
			var st ReadStats
			got, err := d.Aggregate(ctx, sels, reductions, AggregateOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: check, Filter: filter}, &st)
			must(t, err)
			if got[0][0].Value.Int64() != 150 || got[0][1].Value.Int64() != 150*149 || got[0][2].Value.Int64() != 298 || !reflect.DeepEqual(got[0], got[2]) || visited.Load() != 300 || calls.Load() == 0 {
				t.Fatalf("%+v visited=%d", got, visited.Load())
			}
			if !check && st.ValuePagesDecoded == 0 {
				t.Fatal(st)
			}
		}
	}
	for _, fail := range []error{Stop, errors.New("predicate error")} {
		out, err := d.AggregateRows(ctx, p.InputRowSpans(), reductions, AggregateOptions{ValueBudgetBytes: -1, Workers: 4, Filter: &BatchFilter{Columns: []string{"size"}, Apply: func(*ValueBatch, []bool) error { return fail }}}, nil)
		if out != nil || err != fail {
			t.Fatalf("%+v %v", out, err)
		}
	}
	got, err := d.AggregateRows(ctx, p.InputRowSpans(), reductions, AggregateOptions{ValueBudgetBytes: 1, Filter: &BatchFilter{Columns: []string{"name"}, Apply: func(*ValueBatch, []bool) error { t.Fatal("unaffordable filter called"); return nil }}}, nil)
	must(t, err)
	for _, r := range got[0] {
		if r.Status != AggregateBudgetExceeded {
			t.Fatal(got)
		}
	}
}

func TestAggregateCancellationAndInvalid(t *testing.T) {
	d, p := parallelFixture(t, 100, 10, false)
	for _, opt := range []AggregateOptions{{ValueBudgetBytes: -2}, {Filter: &BatchFilter{}}} {
		_, err := d.Aggregate(ctx, nil, nil, opt, nil)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
	for _, r := range []Reduction{{Op: 100}, {Op: Sum}, {Op: CountRows, Column: "size"}} {
		_, err := d.Aggregate(ctx, nil, []Reduction{r}, AggregateOptions{}, nil)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
	if _, err := d.AggregateRows(ctx, []RowSpan{{}}, nil, AggregateOptions{}, nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	if out, err := d.Aggregate(c, []Selection{{}}, []Reduction{{Op: CountRows}}, AggregateOptions{}, nil); out != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(out, err)
	}
	c, cancel = context.WithCancel(ctx)
	out, err := d.AggregateRows(c, p.InputRowSpans(), []Reduction{{Op: CountRows}}, AggregateOptions{ValueBudgetBytes: -1, Workers: 3, Filter: &BatchFilter{Apply: func(*ValueBatch, []bool) error { cancel(); return nil }}}, nil)
	if out != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(out, err)
	}
	must(t, d.Close())
	if out, err := d.Aggregate(ctx, nil, nil, AggregateOptions{}, nil); out != nil || !errors.Is(err, ErrClosed) {
		t.Fatal(out, err)
	}
}

// Ported from swath TestAggregatesSumsStayWithinBudget: mixed admission,
// shared sibling pages, exact totals and actual measure bytes below budget.
func TestAggregateBudgetExtentAccounting(t *testing.T) {
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
		src.take()
		got, err := d.Aggregate(ctx, sels, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: budget}, nil)
		must(t, err)
		read := chunkBytes(t, "size", src.take(), extents)
		if read > budget {
			t.Fatalf("budget=%d read=%d", budget, read)
		}
		count := 0
		for i, row := range got {
			if row[0].Value.Int64() != 90 {
				t.Fatal(row)
			}
			if row[1].Status == AggregateExact {
				count++
				base := int64((i % 3) * 90)
				want := 90*base + 90*89/2
				if row[1].Value.Int64() != want {
					t.Fatalf("input=%d got=%+v want=%d", i, row, want)
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
	got, err := ds.Aggregate(ctx, []Selection{{Span: PrefixSpan([]byte("a/"))}, {Span: PrefixSpan([]byte("b/"))}}, []Reduction{{Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: cost}, nil)
	must(t, err)
	if got[0][0].Status != AggregateExact || got[1][0].Status != AggregateExact || got[1][0].Value.Int64() != 1 {
		t.Fatal(got)
	}
}

func TestAggregateIncrementalBudgetAndOuterPlanning(t *testing.T) {
	var parts []testutil.Part
	for p := range 3 {
		var keys []string
		for i := range 200 {
			keys = append(keys, fmt.Sprintf("p%d/%04d", p, i))
		}
		parts = append(parts, testutil.Part{Rows: testutil.Rows(keys...), GroupRows: 50, PageBytes: 64})
	}
	dir := write(t, testutil.Dataset{Parts: parts})
	d := verified(t, dir, OpenOptions{})
	got, err := d.Aggregate(ctx, []Selection{{}}, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: 64}, nil)
	must(t, err)
	if got[0][1].Status != AggregateBudgetExceeded {
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
	// Descendant endpoints add no key reads to the CheckKeys outer union.
	run := func(sels []Selection) ReadStats {
		ds := verified(t, dir, OpenOptions{})
		var st ReadStats
		_, err := ds.Aggregate(ctx, sels, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, CheckKeys: true}, &st)
		must(t, err)
		return st
	}
	sels := []Selection{{}}
	for i := range 100 {
		sels = append(sels, Selection{Span: KeySpan([]byte(fmt.Sprintf("p1/%04d", i)))})
	}
	if base, nested := run([]Selection{{}}), run(sels); base != nested {
		t.Fatalf("outer %+v nested %+v", base, nested)
	}
}

// Ported from swath TestAggregatesSumsPublishNothingOnFailure and
// TestTotalsCancelledDuringScan, using SeekParquet's read-hook fixture.
func TestAggregateSourceFailureAndCancellation(t *testing.T) {
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
				failure := errors.New("aggregate storage failure")
				src.read = func(string, int, int64) error {
					if cancelled {
						cancel()
						return context.Canceled
					}
					return failure
				}
				var st ReadStats
				got, err := ds.Aggregate(c, []Selection{{}}, []Reduction{{Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, Workers: 3, CheckKeys: check}, &st)
				cancel()
				must(t, ds.Close())
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

func TestAggregateNoReductionsReadsNothing(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a", "b")}}})
	d := open(t, dir, OpenOptions{})
	var st ReadStats
	got, err := d.Aggregate(ctx, []Selection{{Span: KeySpan([]byte("b"))}}, nil, AggregateOptions{}, &st)
	must(t, err)
	if len(got) != 1 || len(got[0]) != 0 || st != (ReadStats{}) {
		t.Fatal(got, st)
	}
}

func TestAggregateHotLoopAllocations(t *testing.T) {
	// Decoder and page allocations are covered by the existing scan budgets.
	// Warming one owned maximum buffer must leave the reduction loop allocation free.
	sum := reductionSum{}
	maximum := reductionSum{maxBytes: make([]byte, 0, 32)}
	values := []Value{ByteArrayValue([]byte("alpha")), ByteArrayValue([]byte("omega"))}
	allocations := testing.AllocsPerRun(100, func() {
		sum.lo, sum.hi = 0, 0
		maximum.seen = false
		for i := range 1000 {
			sum.add(Int64Value(int64(i)-500), Reduction{Op: Sum}, reductionType{kind: KindInt64})
			maximum.add(values[i%2], Reduction{Op: Max}, reductionType{kind: KindByteArray})
		}
	})
	if allocations != 0 {
		t.Fatalf("hot loop allocations: %g", allocations)
	}
	if sum.result(Reduction{Op: Sum}, reductionType{kind: KindInt64}).Value.Int64() != -500 || string(maximum.maximum.Bytes()) != "omega" {
		t.Fatal("reduction values")
	}
}

func TestAggregateZeroBudgetAbsentAndFilter(t *testing.T) {
	d, p := parallelFixture(t, 100, 10, false)
	var st ReadStats
	got, err := d.AggregateRows(ctx, p.InputRowSpans(), []Reduction{{Op: Sum, Column: "absent"}, {Op: CountAbsent, Column: "absent"}, {Op: Max, Column: "absent"}}, AggregateOptions{}, &st)
	must(t, err)
	if got[0][0].Status != AggregateExact || got[0][1].Value.Int64() != 100 || !got[0][2].Value.IsNull() || st.SourceReads != 0 {
		t.Fatal(got, st)
	}
	got, err = d.AggregateRows(ctx, p.InputRowSpans(), []Reduction{{Op: CountRows}}, AggregateOptions{Filter: &BatchFilter{Apply: func(b *ValueBatch, keep []bool) error {
		for i := range keep {
			keep[i] = false
		}
		return nil
	}}}, &st)
	must(t, err)
	if got[0][0].Status != AggregateExact || got[0][0].Value.Int64() != 0 || st.SourceReads != 0 {
		t.Fatal(got, st)
	}
}

func TestAggregateRejectsIncompatibleAnnotations(t *testing.T) {
	type signed struct {
		Key   []byte `parquet:"key"`
		Value int64  `parquet:"value"`
	}
	type unsigned struct {
		Key   []byte `parquet:"key"`
		Value uint64 `parquet:"value"`
	}
	type millis struct {
		Key   []byte `parquet:"key"`
		Value int64  `parquet:"value,timestamp(millisecond)"`
	}
	type micros struct {
		Key   []byte `parquet:"key"`
		Value int64  `parquet:"value,timestamp(microsecond)"`
	}
	for _, parts := range [][]testutil.Part{
		{{Rows: []signed{{[]byte("a"), -1}}}, {Rows: []unsigned{{[]byte("b"), 1}}}},
		{{Rows: []millis{{[]byte("a"), 1000}}}, {Rows: []micros{{[]byte("b"), 1500000}}}},
		{{Rows: []signed{{[]byte("a"), 1000}}}, {Rows: []millis{{[]byte("b"), 1500}}}},
	} {
		d := verified(t, write(t, testutil.Dataset{Parts: parts}), OpenOptions{})
		for _, check := range []bool{false, true} {
			out, err := d.Aggregate(ctx, []Selection{{}}, []Reduction{{Op: Max, Column: "value"}}, AggregateOptions{ValueBudgetBytes: -1, CheckKeys: check}, nil)
			if out != nil || !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("annotations: %+v %v", out, err)
			}
		}
	}
}

func TestAggregateDuplicateSelectionsReuseResolution(t *testing.T) {
	var rows []testutil.Row
	for i := range 100 {
		rows = append(rows, testutil.Rows(fmt.Sprintf("p/%04d", i))...)
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, GroupRows: 25, PageBytes: 64}}})
	run := func(selections []Selection) ([][]AggregateResult, ReadStats) {
		d := verified(t, dir, OpenOptions{})
		var st ReadStats
		out, err := d.Aggregate(ctx, selections, []Reduction{{Op: CountRows}, {Op: Max, Column: "name"}}, AggregateOptions{ValueBudgetBytes: -1}, &st)
		must(t, err)
		return out, st
	}
	sel := Selection{Span: Span{Start: []byte("p/0007"), End: []byte("p/0083")}, Cursor: &Cursor{Key: []byte("p/0021")}}
	one, baseline := run([]Selection{sel})
	duplicates, st := run([]Selection{sel, sel, sel})
	if st != baseline {
		t.Fatalf("duplicate resolution: %+v want %+v", st, baseline)
	}
	for _, row := range duplicates {
		if !reflect.DeepEqual(row, one[0]) {
			t.Fatal(duplicates, one)
		}
	}
}
