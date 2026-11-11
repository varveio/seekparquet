// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

type batchOracleRecord struct {
	Key   []byte  `parquet:"key"`
	Dict  *string `parquet:"dict,optional,dict"`
	Plain string  `parquet:"plain,delta"`
	Num   int64   `parquet:"num"`
	Opt   *string `parquet:"opt,optional"`
}

var batchOracleColumns = []string{"plain", "dict", "key", "num", "missing", "plain", "opt", "dict"}

type batchOracleRow struct {
	rec    batchOracleRecord
	offset int64
	ord    int
}

func batchOracleFixture(t *testing.T, seed int64, indexed bool) (string, []batchOracleRow, int, []int) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	var rows []batchOracleRow
	var groupStarts []int
	for k := 0; len(rows) < 3000; k++ {
		n := 1 + r.Intn(5)
		if r.Intn(6) == 0 {
			n = 30 + r.Intn(400)
		}
		groupStarts = append(groupStarts, len(rows))
		for o := 0; o < n; o++ {
			i := len(rows)
			rec := batchOracleRecord{Key: fmt.Appendf(nil, "k%05d", k), Plain: fmt.Sprintf("p-%d-%s", i, string(bytes.Repeat([]byte{'x'}, r.Intn(30)))), Num: int64(i * 3)}
			if r.Intn(5) != 0 {
				v := fmt.Sprintf("d-%d", r.Intn(8))
				if i > 1500 {
					v = fmt.Sprintf("unique-%d", i)
				}
				rec.Dict = &v
			}
			if r.Intn(3) == 0 {
				v := fmt.Sprintf("o-%d", i)
				rec.Opt = &v
			}
			rows = append(rows, batchOracleRow{rec: rec, offset: int64(o), ord: i})
		}
	}
	// Three parts at key-group boundaries.
	cut1 := groupStarts[len(groupStarts)/3]
	cut2 := groupStarts[2*len(groupStarts)/3]
	recs := make([]batchOracleRecord, len(rows))
	for i := range rows {
		recs[i] = rows[i].rec
	}
	spec := testutil.Dataset{RepeatedKeys: true}
	for _, b := range [][2]int{{0, cut1}, {cut1, cut2}, {cut2, len(rows)}} {
		spec.Parts = append(spec.Parts, testutil.Part{Rows: recs[b[0]:b[1]], GroupRows: 250 + r.Intn(200), PageBytes: 200 + r.Intn(400), DictionaryMaxBytes: 600, Compression: parquet.Compression(&zstd.Codec{}), DataPageVersion: 1 + r.Intn(2)})
	}
	dir := write(t, spec)
	cuts := batchBoundaries(t, dir, len(spec.Parts))
	if !indexed {
		for p := range 3 {
			rewrite(t, dir, p, func(meta *format.FileMetaData) {
				for g := range meta.RowGroups {
					for c := 1; c < len(meta.RowGroups[g].Columns); c++ {
						col := &meta.RowGroups[g].Columns[c]
						col.OffsetIndexOffset, col.OffsetIndexLength = 0, 0
					}
				}
			})
		}
	}
	return dir, rows, len(groupStarts), cuts
}

func batchOracleKey(k int) []byte { return fmt.Appendf(nil, "k%05d", k) }

func batchOracleRanges(r *rand.Rand, nkeys int, rows []batchOracleRow) []Selection {
	var out []Selection
	lo := 0
	for i := 0; i < 1+r.Intn(4) && lo < nkeys-2; i++ {
		a := lo + r.Intn(max(1, (nkeys-lo)/3))
		b := a + 1 + r.Intn(max(1, (nkeys-a)/2))
		rg := Selection{Span: Span{Start: batchOracleKey(a), End: batchOracleKey(b), StartExclusive: r.Intn(4) == 0}}
		if r.Intn(5) == 0 {
			rg.Start = nil
			if len(out) > 0 {
				rg.Start = batchOracleKey(a)
			}
		}
		if r.Intn(6) == 0 && b >= nkeys-2 {
			rg.End = nil
		}
		if r.Intn(2) == 0 {
			// Cursor inside the range on a random row.
			var cand []batchOracleRow
			for _, row := range rows {
				if batchOracleSelects(rg, row.rec.Key) {
					cand = append(cand, row)
				}
			}
			if len(cand) > 0 {
				c := cand[r.Intn(len(cand))]
				off := c.offset
				if r.Intn(4) == 0 {
					off += 1000 // past the group
				}
				rg.Cursor = &Cursor{Key: bytes.Clone(c.rec.Key), Offset: off}
			}
		}
		out = append(out, rg)
		lo = b + 1
		if rg.End == nil {
			break
		}
	}
	return out
}

func batchOracleSelects(rg Selection, key []byte) bool {
	if rg.Start != nil {
		c := bytes.Compare(key, rg.Start)
		if c < 0 || c == 0 && rg.StartExclusive {
			return false
		}
	}
	return rg.End == nil || bytes.Compare(key, rg.End) < 0
}

func batchOracleExpected(ranges []Selection, rows []batchOracleRow) []batchOracleRow {
	var want []batchOracleRow
	for _, row := range rows {
		for _, rg := range ranges {
			if !batchOracleSelects(rg, row.rec.Key) {
				continue
			}
			if c := rg.Cursor; c != nil {
				cmp := bytes.Compare(row.rec.Key, c.Key)
				if cmp < 0 || cmp == 0 && row.offset <= c.Offset {
					continue
				}
			}
			want = append(want, row)
			break
		}
	}
	return want
}

func batchOracleString(p *string) string {
	if p == nil {
		return "<null>"
	}
	return *p
}

func checkBatchOracleRows(t *testing.T, label string, got []Row, want []batchOracleRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rows want %d", label, len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		bad := func(what string) { t.Fatalf("%s: row %d (ord %d) %s: got %+v want %+v", label, i, w.ord, what, g, w) }
		if !bytes.Equal(g.Key, w.rec.Key) || g.Offset != w.offset {
			bad("key/offset")
		}
		if !slices.Equal(g.Columns, batchOracleColumns) || len(g.Values) != len(batchOracleColumns) {
			bad("columns")
		}
		v := g.Values
		if string(v[0].Bytes()) != w.rec.Plain || string(v[5].Bytes()) != w.rec.Plain {
			bad("plain")
		}
		for _, c := range []int{1, 7} {
			if v[c].Absent() || (w.rec.Dict == nil) != v[c].IsNull() || w.rec.Dict != nil && string(v[c].Bytes()) != *w.rec.Dict {
				bad("dict " + batchOracleString(w.rec.Dict))
			}
		}
		if !bytes.Equal(v[2].Bytes(), w.rec.Key) || v[3].Int64() != w.rec.Num || !v[4].Absent() {
			bad("key/num/missing")
		}
		if (w.rec.Opt == nil) != v[6].IsNull() || w.rec.Opt != nil && string(v[6].Bytes()) != *w.rec.Opt {
			bad("opt")
		}
	}
}

// Keep a small deterministic seed set in the default suite. Expected rows come
// from the generated records and range/cursor comparisons, not another scan.
func TestScanBatchesRandomizedOracle(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		for _, indexed := range []bool{true, false} {
			t.Run(fmt.Sprintf("seed=%d/indexed=%t", seed, indexed), func(t *testing.T) {
				dir, rows, nkeys, cuts := batchOracleFixture(t, seed, indexed)
				d := open(t, dir, OpenOptions{})
				r := rand.New(rand.NewSource(seed * 7))
				for trial := 0; trial < 8; trial++ {
					ranges := batchOracleRanges(r, nkeys, rows)
					if trial == 0 {
						ranges = []Selection{{}}
					}
					want := batchOracleExpected(ranges, rows)
					plan, err := d.Plan(ctx, ranges, batchOracleColumns, nil)
					must(t, err)
					var got []Row
					must(t, scanRows(ctx, plan, nil, func(row Row) error { got = append(got, row); return nil }))
					checkBatchOracleRows(t, fmt.Sprintf("Scan trial %d %+v", trial, ranges), got, want)
					shards, err := plan.Split(ctx, 1+r.Intn(6), nil)
					must(t, err)
					got = got[:0]
					for _, sh := range shards {
						must(t, scanRows(ctx, sh, nil, func(row Row) error { got = append(got, row); return nil }))
					}
					checkBatchOracleRows(t, fmt.Sprintf("Split trial %d", trial), got, want)
					checkBatchOracleBatches(t, plan, shards, want, cuts)
				}
			})
		}
	}
}

func checkBatchOracleBatches(t *testing.T, plan *Plan, shards []*Plan, want []batchOracleRow, cuts []int) {
	t.Helper()
	for _, ps := range [][]*Plan{{plan}, shards} {
		var got []Row
		for _, p := range ps {
			must(t, p.Scan(ctx, nil, func(b *Batch) error {
				start := len(got)
				if b.Len() == 0 || start+b.Len() > len(want) {
					t.Fatalf("unexpected batch length %d after %d rows (want %d total)", b.Len(), start, len(want))
				}
				first, last := want[start].ord, want[start+b.Len()-1].ord
				if last-first+1 != b.Len() {
					t.Fatalf("batch crossed a gap: ords %d..%d len %d", first, last, b.Len())
				}
				for _, cut := range cuts {
					if first < cut && cut <= last {
						t.Fatalf("batch [%d,%d] crossed boundary %d", first, last, cut)
					}
				}
				for i := 0; i < b.Len(); i++ {
					w := want[start+i]
					if !bytes.Equal(b.Key(i), w.rec.Key) || b.Offset(i) != w.offset {
						t.Fatalf("borrowed key/offset at %d", w.ord)
					}
					c := b.Cursor(i)
					if !bytes.Equal(c.Key, w.rec.Key) || c.Offset != w.offset {
						t.Fatal("cursor")
					}
					for col := range batchOracleColumns {
						vals, ok := rawColumn(b, col)
						if ok != (col != 4) || ok && len(vals) != b.Len() {
							t.Fatalf("column %d", col)
						}
					}
					got = append(got, b.Row(i))
				}
				return nil
			}))
		}
		checkBatchOracleRows(t, "ScanBatches", got, want)
	}
}
