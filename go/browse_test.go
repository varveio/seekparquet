// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// valueText is the text of one row's projected values.
func valueText(b *strings.Builder, view func(c int) ColumnView, n, i int) {
	for c := range n {
		v := view(c)
		switch {
		case !v.Present():
			b.WriteString(" absent")
		case v.IsNull(i):
			b.WriteString(" null")
		default:
			fmt.Fprintf(b, " %s", v.Value(i))
		}
	}
}

// scannedValues runs ScanValues over plan and returns each yielded row's
// ordinal and values, checking that batches are positive, in dataset
// order and never yield a row twice.
func scannedValues(t *testing.T, plan *Plan, st *ReadStats) ([]int64, []string) {
	t.Helper()
	var ords []int64
	var vals []string
	ncols := len(plan.Columns())
	err := plan.ScanValues(ctx, st, func(b *ValueBatch) error {
		start, ok := b.Start().Ordinal()
		if !ok || b.Len() <= 0 {
			return fmt.Errorf("batch at %d (%v) of %d rows", start, ok, b.Len())
		}
		if n := len(ords); n > 0 && start <= ords[n-1] {
			return fmt.Errorf("batch at %d after row %d", start, ords[n-1])
		}
		for i := range b.Len() {
			var sb strings.Builder
			valueText(&sb, b.Column, ncols, i)
			ords, vals = append(ords, start+int64(i)), append(vals, sb.String())
		}
		return nil
	})
	must(t, err)
	return ords, vals
}

// planScanned runs Plan.Scan over plan and returns each row's key, offset
// and values.
func planScanned(t *testing.T, plan *Plan) ([]string, []string) {
	t.Helper()
	var keys, vals []string
	ncols := len(plan.Columns())
	must(t, plan.Scan(ctx, nil, func(b *Batch) error {
		for i := range b.Len() {
			var sb strings.Builder
			valueText(&sb, b.Column, ncols, i)
			keys, vals = append(keys, fmt.Sprintf("%s/%d", b.Key(i), b.Offset(i))), append(vals, sb.String())
		}
		return nil
	}))
	return keys, vals
}

// randomSpans returns random row spans of d, overlapping, adjacent and
// empty ones among them, and the ordinals of their union.
func randomSpans(r *rand.Rand, d *Dataset) ([]RowSpan, []int64) {
	n := d.r.rows
	var spans []RowSpan
	in := map[int64]bool{}
	for range 1 + r.Intn(6) {
		a := r.Int63n(n + 1)
		b := min(n, a+r.Int63n(max(1, n/4)))
		if r.Intn(8) == 0 {
			b = a
		}
		spans = append(spans, d.r.rowSpan(a, b))
		for o := a; o < b; o++ {
			in[o] = true
		}
	}
	var ords []int64
	for o := range n {
		if in[o] {
			ords = append(ords, o)
		}
	}
	return spans, ords
}

// TestScanValuesDifferential: ScanValues over random row spans yields
// each selected row once, in order, with the values the fixture wrote and
// that Plan.Scan yields over the same plan, which places rows starting
// inside groups of equal keys at their right offsets; over a key plan,
// the values Plan.Scan yields. It decodes no key page; its pages are
// dictionary-encoded and plain, version 1 and 2, with nulls and an absent
// column, located through the OffsetIndex or by walking the headers.
func TestScanValuesDifferential(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		for seed := int64(1); seed <= 3; seed++ {
			dir, rows, nkeys, _ := batchOracleFixture(t, seed, indexed)
			d := open(t, dir, OpenOptions{})
			r := rand.New(rand.NewSource(seed))
			want := make([]string, len(rows))
			wantKeys := make([]string, len(rows))
			for i, row := range rows {
				var sb strings.Builder
				rec := row.rec
				str := func(p *string) {
					if p == nil {
						sb.WriteString(" null")
					} else {
						fmt.Fprintf(&sb, " %s", ByteArrayValue([]byte(*p)))
					}
				}
				fmt.Fprintf(&sb, " %s", ByteArrayValue([]byte(rec.Plain)))
				str(rec.Dict)
				fmt.Fprintf(&sb, " %s %s absent %s", ByteArrayValue(rec.Key), Int64Value(rec.Num), ByteArrayValue([]byte(rec.Plain)))
				str(rec.Opt)
				str(rec.Dict)
				want[i] = sb.String()
				wantKeys[i] = fmt.Sprintf("%s/%d", rec.Key, row.offset)
			}
			for trial := range 12 {
				label := fmt.Sprintf("indexed=%v seed=%d trial=%d", indexed, seed, trial)
				spans, ords := randomSpans(r, d)
				plan, err := d.PlanRows(ctx, spans, batchOracleColumns, nil)
				must(t, err)
				if n, exact := plan.Count(); n != int64(len(ords)) || !exact {
					t.Fatalf("%s: count %d %v, want %d", label, n, exact, len(ords))
				}
				var st ReadStats
				gotOrds, got := scannedValues(t, plan, &st)
				if !slices.Equal(gotOrds, ords) {
					t.Fatalf("%s: rows %v, want %v", label, gotOrds, ords)
				}
				for i, o := range ords {
					if got[i] != want[o] {
						t.Fatalf("%s: row %d: %q, want %q", label, o, got[i], want[o])
					}
				}
				if st.KeyPagesDecoded != 0 || st.KeysDecoded != 0 || len(ords) > 0 && st.ValuePagesDecoded == 0 {
					t.Fatalf("%s: %+v", label, st)
				}
				keys, scanned := planScanned(t, plan)
				if !slices.Equal(scanned, got) {
					t.Fatalf("%s: Plan.Scan values differ", label)
				}
				for i, o := range ords {
					if keys[i] != wantKeys[o] {
						t.Fatalf("%s: Plan.Scan row %d: %s, want %s", label, o, keys[i], wantKeys[o])
					}
				}
				// A plan of key selections: ScanValues yields what Plan.Scan
				// does.
				sels := batchOracleRanges(r, nkeys, rows)
				kplan, err := d.Plan(ctx, sels, batchOracleColumns, nil)
				must(t, err)
				_, kgot := scannedValues(t, kplan, nil)
				_, kwant := planScanned(t, kplan)
				if !slices.Equal(kgot, kwant) {
					t.Fatalf("%s: ScanValues of %d rows, Plan.Scan of %d", label, len(kgot), len(kwant))
				}
			}
		}
	}
}

// TestScanValuesConcurrent: concurrent scans of one plan, run under -race,
// have independent borrowed storage and yield the same values.
func TestScanValuesConcurrent(t *testing.T) {
	dir, _, _, _ := batchOracleFixture(t, 7, true)
	d := open(t, dir, OpenOptions{})
	plan, err := d.PlanRows(ctx, []RowSpan{d.r.rowSpan(0, d.r.rows)}, batchOracleColumns, nil)
	must(t, err)
	_, want := scannedValues(t, plan, nil)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() {
			var got []string
			errs[i] = plan.ScanValues(ctx, nil, func(b *ValueBatch) error {
				for j := range b.Len() {
					var sb strings.Builder
					valueText(&sb, b.Column, len(batchOracleColumns), j)
					got = append(got, sb.String())
				}
				return nil
			})
			if errs[i] == nil && !slices.Equal(got, want) {
				errs[i] = errors.New("values differ")
			}
		})
	}
	wg.Wait()
	must(t, errors.Join(errs...))
}

// TestScanValuesStopAndClose: Stop ends a scan with nil, a callback error
// is returned unchanged, a cancelled context and a closed Dataset end it.
func TestScanValuesStopAndClose(t *testing.T) {
	dir, _, _, _ := batchOracleFixture(t, 2, true)
	d := open(t, dir, OpenOptions{})
	plan, err := d.PlanRows(ctx, []RowSpan{d.r.rowSpan(0, d.r.rows)}, []string{"num"}, nil)
	must(t, err)
	calls := 0
	if err := plan.ScanValues(ctx, nil, func(*ValueBatch) error { calls++; return Stop }); err != nil || calls != 1 {
		t.Fatalf("stop: %v after %d calls", err, calls)
	}
	mine := errors.New("mine")
	if err := plan.ScanValues(ctx, nil, func(*ValueBatch) error { return mine }); err != mine {
		t.Fatalf("callback error: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := plan.ScanValues(cctx, nil, func(*ValueBatch) error { cancel(); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	must(t, d.Close())
	afterClose, err := plan.SelectRows(plan.InputRowSpans())
	must(t, err)
	if err := afterClose.ScanValues(ctx, nil, func(*ValueBatch) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("selected plan after close: %v", err)
	}
	if err := plan.ScanValues(ctx, nil, func(*ValueBatch) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
	if _, err := d.PlanRows(ctx, nil, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("plan rows when closed: %v", err)
	}
}

// TestSelectRowsProjectedBytes: SelectRows prices a union of a plan's rows
// as planning those rows does, without I/O; InputRowSpans gives back the
// inputs; positions outside the plan are refused; with no room in
// OffsetIndexMemory, the selected plan's bytes are not exact.
func TestSelectRowsProjectedBytes(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		dir, _, _, _ := batchOracleFixture(t, 4, indexed)
		d := open(t, dir, OpenOptions{})
		r := rand.New(rand.NewSource(4))
		n := d.r.rows
		lo, hi := n/5, n-n/7
		cols := []string{"dict", "num", "missing", "opt"}
		whole, err := d.PlanRows(ctx, []RowSpan{d.r.rowSpan(lo, hi)}, cols, nil)
		must(t, err)
		same, err := whole.SelectRows(whole.InputRowSpans())
		must(t, err)
		if b, e := same.ProjectedBytes(); !eqBytes(b, e, whole) {
			t.Fatalf("indexed=%v: all of the plan: %d %v, plan %d", indexed, b, e, whole.bytes)
		}
		for trial := range 30 {
			var spans []RowSpan
			for range 1 + r.Intn(4) {
				a := lo + r.Int63n(hi-lo)
				spans = append(spans, d.r.rowSpan(a, min(hi, a+r.Int63n(n/6))))
			}
			var st ReadStats
			sel, err := whole.SelectRows(spans)
			must(t, err)
			fresh, err := d.PlanRows(ctx, spans, cols, &st)
			must(t, err)
			gb, ge := sel.ProjectedBytes()
			if !eqBytes(gb, ge, fresh) {
				t.Fatalf("indexed=%v trial %d: selected %d %v, planned %d %v", indexed, trial, gb, ge, fresh.bytes, !fresh.partial)
			}
			gn, gx := sel.Count()
			fn, fx := fresh.Count()
			if gn != fn || gx != fx || !slices.Equal(sel.InputRowSpans(), spans) {
				t.Fatalf("trial %d: counts %d %v, %d %v", trial, gn, gx, fn, fx)
			}
			_, a := scannedValues(t, sel, nil)
			_, b := scannedValues(t, fresh, nil)
			if !slices.Equal(a, b) {
				t.Fatalf("trial %d: selected values differ", trial)
			}
		}
		if _, err := whole.SelectRows([]RowSpan{d.r.rowSpan(lo-1, lo+1)}); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("outside: %v", err)
		}
		if !indexed {
			continue
		}
		small := open(t, dir, OpenOptions{Limits: Limits{OffsetIndexMemory: 1}})
		p, err := small.PlanRows(ctx, []RowSpan{small.r.rowSpan(lo, hi)}, cols, nil)
		must(t, err)
		if _, exact := p.ProjectedBytes(); !exact {
			t.Fatalf("the plan's own bytes are exact")
		}
		s, err := p.SelectRows(p.InputRowSpans())
		must(t, err)
		if _, exact := s.ProjectedBytes(); exact {
			t.Fatalf("locations not kept, yet selected bytes exact")
		}
		if used := small.r.locsMemory.Load(); used != 0 {
			t.Fatalf("%d bytes of locations held past OffsetIndexMemory", used)
		}
	}
}

func eqBytes(n int64, exact bool, p *Plan) bool { return n == p.bytes && exact == !p.partial }

func TestSelectRowsDuplicateProjection(t *testing.T) {
	dir, _, _, _ := batchOracleFixture(t, 4, true)
	d := open(t, dir, OpenOptions{})
	spans := []RowSpan{d.r.rowSpan(0, d.r.rows)}
	for _, byKey := range []bool{false, true} {
		makePlan := func(columns []string) *Plan {
			var p *Plan
			var err error
			if byKey {
				p, err = d.Plan(ctx, []Selection{{}}, columns, nil)
			} else {
				p, err = d.PlanRows(ctx, spans, columns, nil)
			}
			must(t, err)
			return p
		}
		one := makePlan([]string{"dict", "num"})
		dup := makePlan([]string{"dict", "num", "dict", "num"})
		if n, exact := dup.ProjectedBytes(); !eqBytes(n, exact, one) {
			t.Fatalf("byKey=%v: duplicate columns priced %d %v, want %d %v", byKey, n, exact, one.bytes, !one.partial)
		}
		subset := []RowSpan{
			d.r.rowSpan(d.r.rows/3, d.r.rows/2),
			d.r.rowSpan(d.r.rows/3+1, d.r.rows/2+1),
		}
		a, err := one.SelectRows(subset)
		must(t, err)
		b, err := dup.SelectRows(subset)
		must(t, err)
		if n, exact := b.ProjectedBytes(); !eqBytes(n, exact, a) {
			t.Fatalf("byKey=%v: duplicate subset priced %d %v, want %d %v", byKey, n, exact, a.bytes, !a.partial)
		}
		_, values := scannedValues(t, b, nil)
		for _, row := range values {
			fields := strings.Fields(row)
			if len(fields) != 4 || fields[0] != fields[2] || fields[1] != fields[3] {
				t.Fatalf("duplicate projection changed values: %q", row)
			}
		}
	}
}

// TestPlanMetadataFirst: Dataset.Plan resolves the whole dataset, and ends
// at part boundaries, from the manifest without decoding a key page, and
// decoded unique first keys place an end at a row group; InputRowSpans gives
// each selection's rows in input order, an empty one with equal ends.
func TestPlanMetadataFirst(t *testing.T) {
	keys := fixtureKeys(300)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 25, PageBytes: 256}, split(keys, 3)...)), OpenOptions{})
	var st ReadStats
	plan, err := d.Plan(ctx, []Selection{{}}, nil, &st)
	must(t, err)
	if n, _ := plan.Count(); n != 300 || st.KeyPagesDecoded != 0 || st.PartsOpened != 1 {
		t.Fatalf("whole dataset: %d rows, %+v", n, st)
	}
	parts := d.Metadata().Parts
	sels := []Selection{
		{Span: Span{Start: parts[1].MinKey, End: parts[2].MinKey}},
		{Span: Span{Start: []byte("\xff\xff\xff"), End: []byte("\xff\xff\xff\x00")}},
		{Span: Span{Start: parts[1].MinKey, End: append(bytes.Clone(parts[1].MaxKey), 0)}},
	}
	plan, err = d.Plan(ctx, sels, nil, &st)
	must(t, err)
	if st.KeyPagesDecoded != 0 {
		t.Fatalf("part boundaries decoded %d key pages", st.KeyPagesDecoded)
	}
	spans := plan.InputRowSpans()
	ord := func(p RowPosition) int64 {
		n, ok := p.Ordinal()
		must(t, map[bool]error{true: nil, false: errors.New("unknown")}[ok])
		return n
	}
	if len(spans) != 3 || ord(spans[0].Start) != 100 || ord(spans[0].End) != 200 || ord(spans[1].Start) != ord(spans[1].End) ||
		ord(spans[2].Start) != 100 || ord(spans[2].End) != 200 {
		t.Fatalf("row spans %+v", spans)
	}
	if n, _ := plan.Count(); n != 100 {
		t.Fatalf("count %d", n)
	}
	// A row group's first key, decoded once, places a later plan's start.
	first := []byte(keys[125])
	_, err = d.Rows(ctx, Query{Selection: Selection{Span: KeySpan(first)}, Limit: 1}, nil)
	must(t, err)
	if k := d.r.parts[1].groups[1].first.Load(); k == nil || !bytes.Equal(*k, first) {
		t.Fatal("the fixture's row group does not begin at the key")
	}
	plan, err = d.Plan(ctx, []Selection{{Span: Span{Start: first, End: parts[2].MinKey}}}, nil, &st)
	must(t, err)
	if n, exact := plan.Count(); n != 75 || !exact || st.KeyPagesDecoded != 0 {
		t.Fatalf("from a first key: %d %v, %+v", n, exact, st)
	}
}

// TestListRowsRandomOracle: over random listings (prefixes, limits from 1,
// End bounds, cursors of earlier pages, arbitrary row and prefix cursors),
// with unique and repeated keys, across parts and with a list index, every
// boundary ListEntry.Rows records is that of the candidate rows the entry
// represents, and every entry but a full page's last has both, unless End
// or an arbitrary prefix cursor may clip it.
func TestListRowsRandomOracle(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	checked := 0
	defer func() {
		if checked < 500 {
			t.Errorf("only %d common prefixes with both boundaries", checked)
		}
	}()
	for _, repeated := range []bool{false, true} {
		base := fixtureKeys(400)
		var keys []string
		var partKeys [][]string
		for _, pk := range split(base, 3) {
			var out []string
			for _, k := range pk {
				n := 1
				if repeated {
					n = 1 + r.Intn(3)
				}
				for range n {
					out = append(out, k)
				}
			}
			partKeys, keys = append(partKeys, out), append(keys, out...)
		}
		spec := parts(testutil.Part{GroupRows: 20, PageBytes: 128}, partKeys...)
		spec.RepeatedKeys = repeated
		dir := write(t, spec)
		for _, indexed := range []bool{false, true} {
			d := open(t, dir, OpenOptions{})
			if indexed {
				_, err := d.BuildListIndex(ctx, ListIndexOptions{})
				must(t, err)
			}
			prefixes := []string{"", "d0", "d01/", "long/", "é/", "root", "\xff"}
			for trial := range 300 {
				q := ListQuery{Prefix: []byte(prefixes[r.Intn(len(prefixes))]), Delimiter: '/', Limit: 1 + r.Intn(8)}
				if r.Intn(3) == 0 {
					q.End = []byte(base[r.Intn(len(base))])
				}
				arbitrary := false
				switch r.Intn(4) {
				case 1:
					k := keys[r.Intn(len(keys))]
					q.Cursor = RowCursor([]byte(k), int64(r.Intn(3)))
				case 2:
					k := keys[r.Intn(len(keys))]
					q.Cursor = PrefixCursor([]byte(k[:r.Intn(len(k)+1)]))
					arbitrary = true
				}
				for page := 0; page < 4; page++ {
					ents, err := d.List(ctx, q, nil)
					must(t, err)
					for i, e := range ents {
						label := fmt.Sprintf("repeated=%v indexed=%v trial %d page %d entry %d %q of %+v", repeated, indexed, trial, page, i, e.Bytes(), q)
						lo, hi := listCandidates(keys, q, e)
						s, okS := e.Rows.Start.Ordinal()
						x, okE := e.Rows.End.Ordinal()
						if e.IsPrefix() && okS && okE {
							checked++
						}
						if okS && s != lo || okE && x != hi {
							t.Fatalf("%s: rows [%d %v, %d %v), want [%d, %d)", label, s, okS, x, okE, lo, hi)
						}
						full := len(ents) == q.Limit && i == len(ents)-1
						if !full && q.End == nil && !arbitrary && (!okS || !okE) {
							t.Fatalf("%s: boundaries %v %v", label, okS, okE)
						}
					}
					if len(ents) < q.Limit {
						break
					}
					q.Cursor, arbitrary = ents[len(ents)-1].Cursor(), false
				}
			}
		}
	}
}

// listCandidates returns the ordinals [lo, hi) of q's candidate rows that
// e represents, keys being the dataset's in order.
func listCandidates(keys []string, q ListQuery, e ListEntry) (lo, hi int64) {
	lo, hi = -1, -1
	offset := int64(0)
	for i, k := range keys {
		if i > 0 && keys[i-1] == k {
			offset++
		} else {
			offset = 0
		}
		key := []byte(k)
		if !bytes.HasPrefix(key, q.Prefix) || q.End != nil && bytes.Compare(key, q.End) >= 0 || !afterCursor(q.Cursor, key, offset) {
			continue
		}
		var match bool
		if e.IsPrefix() {
			match = bytes.Equal(commonPrefix(key, q.Prefix, q.Delimiter), e.CommonPrefix)
		} else {
			match = bytes.Equal(key, e.Row.Key) && offset == e.Row.Offset
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

// Unindexed chunks still use windows, but an unchecked chunk must never
// cause a read larger than the page bound, even if its extent is enormous.
func TestScanValuesUnindexedWindows(t *testing.T) {
	type record struct {
		Key  string `parquet:"key"`
		Size int64  `parquet:"size,plain"`
	}
	rows := make([]record, 6000)
	for i := range rows {
		rows[i] = record{fmt.Sprintf("key-%06d", i), int64(i)}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256}}})
	dropValueOffsets(t, dir)
	for _, tc := range []struct {
		name      string
		pageBytes int64
		oversized bool
	}{
		{"bounded", 1024, false},
		{"oversized", 1024, true},
		{"large-page-limit", 128 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &recordingWaveSource{Source: dirOf(t, dir)}
			d := openWith(t, ctx, src, OpenOptions{Limits: Limits{PageBytes: tc.pageBytes, MetadataMemory: 8192}}, false)
			p, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
			must(t, err)
			if tc.oversized {
				// Keep the extent within the declared file size so scheduling
				// cannot rely on the file bound to reject the oversized chunk.
				d.r.parts[0].size = 2 << 30
				d.r.parts[0].footer.Metadata().RowGroups[0].Columns[1].MetaData.TotalCompressedSize = 1 << 30
			}
			src.batches = nil
			var sum int64
			err = p.ScanValues(ctx, nil, func(b *ValueBatch) error {
				for i := range b.Len() {
					sum += b.Column(0).Int64(i)
				}
				return nil
			})
			if tc.pageBytes > readWaveBytes {
				// A bounded prefix of the forged extent reaches beyond the
				// real file. Its source error must be surfaced.
				if err == nil {
					t.Fatal("expected a source error for the forged extent")
				}
			} else {
				must(t, err)
				if want := int64(len(rows) * (len(rows) - 1) / 2); sum != want {
					t.Fatalf("sum %d, want %d", sum, want)
				}
			}
			if len(src.batches) == 0 {
				t.Fatal("unindexed chunk bypassed windows")
			}
			limit := min(readWindowBytes, d.r.limits.PageBytes)
			for _, batch := range src.batches {
				for _, rg := range batch {
					if rg[1] > limit {
						t.Fatalf("window reads %d bytes, page bound %d", rg[1], limit)
					}
				}
			}
		})
	}
}

func TestBrowsePositionEquality(t *testing.T) {
	known := RowPosition{ds: 1, ordinal: 3}
	otherHandle := RowPosition{ds: 2, ordinal: 3}
	for _, tc := range []struct {
		a, b  RowPosition
		equal bool
	}{
		{RowPosition{}, RowPosition{}, true},
		{known, RowPosition{}, false},
		{RowPosition{}, known, false},
		{known, otherHandle, true},
		{known, RowPosition{ds: 1, ordinal: 4}, false},
	} {
		if samePosition(tc.a, tc.b) != tc.equal {
			t.Fatalf("position comparison of %+v and %+v, want %v", tc.a, tc.b, tc.equal)
		}
		for _, end := range []bool{false, true} {
			a, b := RowSpan{Start: tc.a}, RowSpan{Start: tc.b}
			if end {
				a, b = RowSpan{End: tc.a}, RowSpan{End: tc.b}
			}
			if sameEntries([]ListEntry{{Rows: a}}, []ListEntry{{Rows: b}}) != tc.equal {
				t.Fatalf("entry comparison of %+v and %+v, want %v", a, b, tc.equal)
			}
		}
	}
}

func TestListShortcutPartialGroupPosition(t *testing.T) {
	keys := []string{"a/", "a/x", "a/y", "a/z", "a/zz", "b/z"}
	d := open(t, write(t, parts(testutil.Part{GroupRows: 3, PageBytes: 16}, keys)), OpenOptions{})
	// Seed the first-key witness used by the shortcut, then start inside
	// that group, whose successor group still shares the common prefix.
	_, err := d.Rows(ctx, Query{Limit: 1}, nil)
	must(t, err)
	var st ReadStats
	entries, err := d.List(ctx, ListQuery{Delimiter: '/', Cursor: &Cursor{Prefix: []byte("a/x")}, Limit: 1}, &st)
	must(t, err)
	if len(entries) != 1 || string(entries[0].CommonPrefix) != "a/" {
		t.Fatal(entries)
	}
	if n, known := entries[0].Rows.Start.Ordinal(); !known || n != 2 {
		t.Fatalf("start %d, known %v; want 2, true", n, known)
	}
	if st.GroupsSkipped != 0 {
		t.Fatalf("read group counted as skipped: %+v", st)
	}
}
