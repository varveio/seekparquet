// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Additional rows only: existing ioBudgets and scenarios remain unchanged.
// Mirrors swath internal/readbudget's counts-off/on and budgeted browse sums
// on SeekParquet's own fixture. Each operation starts after the same listing.
func TestReadBudgetsGeneric(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	for _, group := range []struct {
		name  string
		spec  ioFixtureSpec
		names []string
		table []ioBudget
	}{
		{"small", ioSmall, []string{"counts", "sum-one", "page", "parallel"}, []ioBudget{
			{"counts", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
			{"sum-one", ioMetrics{1, 1, 6153, 0, 1}, ioTarget{1, 1}, atTarget},
			{"page", ioMetrics{4, 2, 19328, 0, 2}, none, current},
			{"parallel", ioMetrics{4, 1, 43524, 0, 4}, none, current},
		}},
		// Mirror swath's largeGeo budget cut with this writer's fixture:
		// 2,200 rows per prefix makes exactly nine fit within 64 KiB.
		{"nine-sums", ioFixtureSpec{rows: 400_000, parts: 4, groups: 4, perFolder: 2200}, []string{"sum-budget", "sum-budget-counts", "filter-budget"}, []ioBudget{
			{"sum-budget", ioMetrics{2, 1, 63454, 0, 2}, none, current},
			{"sum-budget-counts", ioMetrics{2, 1, 63454, 0, 2}, none, current},
			{"filter-budget", ioMetrics{2, 1, 63454, 0, 2}, none, current},
		}},
	} {
		t.Run(group.name, func(t *testing.T) {
			fx := ioFixtureOf(t, group.spec)
			budgeted := group.name == "nine-sums"
			var scenarios []ioScenario
			for _, name := range group.names {
				scenarios = append(scenarios, ioScenario{name, func(e *ioEnv) {
					ctx := context.Background()
					d := e.open()
					limit := 151
					if budgeted {
						limit = 1000
					}
					entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: limit}, nil)
					must(e.t, err)
					var spans []seekparquet.RowSpan
					if name == "sum-one" {
						entries, err = d.List(ctx, seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: 101}, nil)
						must(e.t, err)
					}
					for _, ent := range entries {
						if ent.CommonPrefix != nil {
							if _, known := ent.Rows.End.Ordinal(); known {
								spans = append(spans, ent.Rows)
							}
						}
					}
					if name != "sum-one" && len(spans) < 9 {
						e.t.Fatal("fixture needs nine complete prefixes")
					}
					if name != "sum-one" && !budgeted {
						spans = spans[:9]
					}
					e.step(name, func() string {
						if name == "page" {
							p, err := d.Page(ctx, seekparquet.PageQuery{ListQuery: seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: 10}, Reductions: []seekparquet.Reduction{{Op: seekparquet.CountRows}, {Op: seekparquet.Sum, Column: "size"}}, Aggregates: seekparquet.AggregateOptions{ValueBudgetBytes: 64 << 10}}, nil)
							if err != nil {
								return errResult(err)
							}
							return fmt.Sprintf("%d entries next=%v", len(p.Entries), p.Next != nil)
						}
						if name == "parallel" {
							p, err := d.PlanRows(ctx, spans, []string{"size"}, nil)
							if err != nil {
								return errResult(err)
							}
							// Sum is commutative; callbacks are intentionally unordered.
							var n atomic.Int64
							err = p.ScanParallel(ctx, 2, nil, func(b *seekparquet.Batch) error { n.Add(int64(b.Len())); return nil })
							return withErr(fmt.Sprint(n.Load()), err)
						}
						reductions := []seekparquet.Reduction{{Op: seekparquet.Sum, Column: "size"}}
						opt := seekparquet.AggregateOptions{ValueBudgetBytes: 64 << 10}
						if name == "counts" {
							reductions = []seekparquet.Reduction{{Op: seekparquet.CountRows}}
							opt.ValueBudgetBytes = 0
						}
						if name == "sum-one" {
							spans = spans[len(spans)-1:]
						}
						if name == "sum-budget-counts" {
							reductions = append(reductions, seekparquet.Reduction{Op: seekparquet.CountRows})
						}
						if name == "filter-budget" {
							opt.Filter = &seekparquet.BatchFilter{Columns: []string{"size"}, Apply: func(b *seekparquet.ValueBatch, keep []bool) error {
								for i := range keep {
									keep[i] = b.Column(0).Int64(i)%2 == 0
								}
								return nil
							}}
						}
						out, err := d.AggregateRows(ctx, spans, reductions, opt, nil)
						if err != nil {
							return errResult(err)
						}
						var text strings.Builder
						summed := 0
						for _, row := range out {
							if row[0].Status == seekparquet.AggregateExact {
								summed++
							}
							for j, r := range row {
								want := seekparquet.AggregateExact
								if budgeted && j == 0 && r.Status == seekparquet.AggregateBudgetExceeded {
									want = seekparquet.AggregateBudgetExceeded
								}
								if r.Status != want {
									e.t.Fatalf("%s: unexpected result %+v", name, r)
								}
								if budgeted && j > 0 && r.Value.Int64() != int64(group.spec.perFolder) {
									e.t.Fatalf("%s: count %+v", name, r)
								}
								fmt.Fprintf(&text, "%d:%d ", r.Status, r.Value.Int64())
							}
						}
						if budgeted && (summed != 9 || summed >= len(spans)) {
							e.t.Fatalf("%s: %d of %d prefixes summed under 64 KiB; want exactly nine and not all", name, summed, len(spans))
						}
						return text.String()
					})
				}})
			}
			reports := runIOScenarios(t, fx, scenarios, ioStores)
			for _, name := range []string{"plain", "capable"} {
				checkBudgets(t, name, group.table, reports[name])
			}
		})
	}
}
