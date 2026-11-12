// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// A whole unindexed chunk that fits PageBytes should cost no more waves
// than Plan.Scan, even when it exceeds the per-window size.
func TestScanValuesLargeUnindexedWindows(t *testing.T) {
	type record struct {
		Key  string `parquet:"key"`
		Blob string `parquet:"blob,plain"`
	}
	rows := make([]record, 6000)
	for i := range rows {
		rows[i] = record{fmt.Sprintf("key-%06d", i), string(make([]byte, 1024))}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 64 << 10}}})
	dropValueOffsets(t, dir)
	var waves [2]int
	for mode := range waves {
		src := &recordingWaveSource{Source: dirOf(t, dir)}
		d := openWith(t, ctx, src, OpenOptions{}, false)
		p, err := d.Plan(ctx, []Selection{{}}, []string{"blob"}, nil)
		must(t, err)
		meta := &d.r.parts[0].footer.Metadata().RowGroups[0].Columns[1].MetaData
		if meta.TotalCompressedSize <= readWindowBytes || meta.TotalCompressedSize > d.r.limits.PageBytes {
			t.Fatalf("fixture chunk size %d outside (window, PageBytes]", meta.TotalCompressedSize)
		}
		src.batches = nil
		n := 0
		if mode == 0 {
			err = p.ScanValues(ctx, nil, func(b *ValueBatch) error {
				for i := range b.Len() {
					if string(b.Column(0).Bytes(i)) != rows[n+i].Blob {
						t.Fatal("wrong value")
					}
				}
				n += b.Len()
				return nil
			})
		} else {
			err = p.Scan(ctx, nil, func(b *Batch) error { n += b.Len(); return nil })
		}
		must(t, err)
		if n != len(rows) {
			t.Fatalf("rows %d, want %d", n, len(rows))
		}
		waves[mode] = len(src.batches)
	}
	t.Logf("values %d waves, scan %d waves", waves[0], waves[1])
	if waves[0] > waves[1] {
		t.Fatalf("values %d waves exceed scan %d", waves[0], waves[1])
	}
}

func TestScanValuesDictionaryPageLimitBytes(t *testing.T) {
	type record struct {
		Key  string `parquet:"key"`
		Dict string `parquet:"dict,dict"`
	}
	rows := make([]record, 100)
	for i := range rows {
		rows[i] = record{fmt.Sprintf("k%04d", i), fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 128))}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256}}})
	var counts [2]int64
	for mode := range counts {
		d := openWith(t, ctx, &recordingWaveSource{Source: dirOf(t, dir)}, OpenOptions{Limits: Limits{PageBytes: 1000, OffsetIndexMemory: 1}}, false)
		p, err := d.Plan(ctx, []Selection{{}}, []string{"dict"}, nil)
		must(t, err)
		var st ReadStats
		if mode == 0 {
			err = p.ScanValues(ctx, &st, func(*ValueBatch) error { t.Fatal("decoded oversized dictionary"); return nil })
		} else {
			err = p.Scan(ctx, &st, func(*Batch) error { t.Fatal("decoded oversized dictionary"); return nil })
		}
		var limit *LimitError
		if !errors.As(err, &limit) || limit.Limit != LimitPageBytes {
			t.Fatalf("want PageBytes limit, got %v", err)
		}
		counts[mode] = st.SourceBytes
	}
	t.Logf("values %d bytes, scan %d bytes", counts[0], counts[1])
	if counts[0] > counts[1] {
		t.Fatalf("values transferred %d bytes before dictionary limit, scan %d", counts[0], counts[1])
	}
}

// Port of the review's differential repro: compare remote windows with
// local demand for random sparse spans, dictionaries, duplicate projection
// slots, missing columns, and indexed and unindexed chunks.
func TestScanValuesWindowedDifferential(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		for seed := int64(1); seed <= 3; seed++ {
			dir, _, _, _ := batchOracleFixture(t, seed, indexed)
			local := open(t, dir, OpenOptions{})
			for _, pageBytes := range []int64{0, 4096} {
				t.Run(fmt.Sprintf("indexed-%v/seed-%d/page-%d", indexed, seed, pageBytes), func(t *testing.T) {
					src := &recordingWaveSource{Source: dirOf(t, dir)}
					d := openWith(t, ctx, src, OpenOptions{Limits: Limits{PageBytes: pageBytes}}, false)
					rng := rand.New(rand.NewSource(seed))
					for range 12 {
						spans, ords := randomSpans(rng, d)
						lspans := make([]RowSpan, len(spans))
						for i, s := range spans {
							lspans[i] = local.r.rowSpan(s.Start.ordinal, s.End.ordinal)
						}
						lp, err := local.PlanRows(ctx, lspans, batchOracleColumns, nil)
						must(t, err)
						wantOrds, want := scannedValues(t, lp, nil)
						p, err := d.PlanRows(ctx, spans, batchOracleColumns, nil)
						must(t, err)
						gotOrds, got := scannedValues(t, p, nil)
						if !slices.Equal(gotOrds, ords) || !slices.Equal(gotOrds, wantOrds) || !slices.Equal(got, want) {
							t.Fatal("windowed values differ")
						}
					}
					p, err := d.PlanRows(ctx, []RowSpan{d.r.rowSpan(0, d.r.rows)}, batchOracleColumns, nil)
					must(t, err)
					must(t, p.ScanValues(ctx, nil, func(*ValueBatch) error { return Stop }))
					cctx, cancel := context.WithCancel(ctx)
					defer cancel()
					if err := p.ScanValues(cctx, nil, func(*ValueBatch) error { cancel(); return nil }); !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					must(t, d.Close())
					if err := p.ScanValues(ctx, nil, func(*ValueBatch) error { return nil }); !errors.Is(err, ErrClosed) {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestListShortcutCountsOnlyCommittedSkip(t *testing.T) {
	keys := []string{"a/", "a/x", "a/y", "a/z", "a/zz", "b/z"}
	d := open(t, write(t, parts(testutil.Part{GroupRows: 3, PageBytes: 16}, keys)), OpenOptions{})
	must(t, d.Verify(ctx, nil))
	op, err := d.r.begin(ctx)
	must(t, err)
	var st ReadStats
	defer op.end(&st)
	s := d.r.newScan(op, nil, &st)
	whole, g, err := s.shortcut([]byte("a/y"), nil, '/')
	must(t, err)
	if string(whole) != "a/" || g != (groupRef{}) {
		t.Fatalf("shortcut %q in %v", whole, g)
	}
	if st.GroupsSkipped != 0 {
		t.Fatalf("uncommitted shortcut counted: %+v", st)
	}
	l := listing{r: d.r}
	must(t, s.seekList([]byte("a/y"), ListQuery{Delimiter: '/', Limit: 1}, &l))
	if st.GroupsSkipped != 0 {
		t.Fatalf("read group counted as skipped: %+v", st)
	}
	// seekList falls back to positioning the scan; the caller emits that row.
	if s.ordinal() != 2 {
		t.Fatalf("first candidate row %d", s.ordinal())
	}
	s = d.r.newScan(op, nil, &st)
	l = listing{r: d.r}
	must(t, s.seekList(nil, ListQuery{Delimiter: '/', Limit: 1}, &l))
	if st.GroupsSkipped != 1 {
		t.Fatalf("committed shortcut uncounted: %+v", st)
	}
}
