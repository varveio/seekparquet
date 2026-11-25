// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	seekparquet "github.com/varveio/seekparquet/go"
)

func ExampleDataset_Aggregate() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	if err := d.Verify(ctx, nil); err != nil {
		log.Fatal(err)
	}
	results, err := d.Aggregate(ctx, []seekparquet.Selection{
		{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))},
		{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
	}, []seekparquet.Reduction{
		{Op: seekparquet.CountRows},
		{Op: seekparquet.Sum, Column: "size"},
		{Op: seekparquet.CountNulls, Column: "size"},
	}, seekparquet.AggregateOptions{ValueBudgetBytes: 4 << 20}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		if r[0].Status == seekparquet.AggregateExact && r[1].Status == seekparquet.AggregateExact {
			fmt.Printf("rows=%d size=%d nulls=%d\n", r[0].Value.Int64(), r[1].Value.Int64(), r[2].Value.Int64())
		}
	}
	// Output:
	// rows=30 size=31543127898 nulls=0
	// rows=40 size=43279389918 nulls=0
}

func ExampleBatchFilter() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	if err := d.Verify(ctx, nil); err != nil {
		log.Fatal(err)
	}
	filter := &seekparquet.BatchFilter{
		Columns: []string{"size"},
		Apply: func(b *seekparquet.ValueBatch, keep []bool) error {
			sizes := b.Column(0)
			for i := range keep {
				keep[i] = sizes.Present() && !sizes.IsNull(i) && sizes.Int64(i) >= 0
			}
			return nil
		},
	}
	results, err := d.Aggregate(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}},
		[]seekparquet.Reduction{{Op: seekparquet.CountRows}},
		seekparquet.AggregateOptions{ValueBudgetBytes: -1, Filter: filter}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("selected rows:", results[0][0].Value.Int64())
	// Output:
	// selected rows: 30
}

func ExamplePlan_ScanParallel() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	plan, err := d.Plan(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}}, []string{"size"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	var rows atomic.Int64
	err = plan.ScanParallel(ctx, 2, nil, func(b *seekparquet.Batch) error {
		rows.Add(int64(b.Len()))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("rows:", rows.Load())
	// Output:
	// rows: 30
}

func ExampleDataset_Page() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	if err := d.Verify(ctx, nil); err != nil {
		log.Fatal(err)
	}
	q := seekparquet.PageQuery{
		ListQuery:  seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 2},
		Reductions: []seekparquet.Reduction{{Op: seekparquet.CountRows}},
	}
	for {
		page, err := d.Page(ctx, q, nil)
		if err != nil {
			log.Fatal(err)
		}
		for _, entry := range page.Entries {
			fmt.Printf("%s: %d rows\n", entry.Bytes(), entry.Aggregates[0].Value.Int64())
		}
		if page.Next == nil {
			break
		}
		q.Cursor = page.Next
	}
	// Output:
	// logs/2026/01/: 30 rows
	// logs/2026/02/: 40 rows
	// logs/2026/03/: 30 rows
}

func ExampleFoldRows() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	plan, err := d.Plan(ctx, []seekparquet.Selection{
		{Span: seekparquet.PrefixSpan([]byte("logs/2026/"))},
		{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
	}, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Sum size by the value of another column. Each worker folds into maps
	// of its own, so Add takes no lock. Merge copies: from may be merged
	// into both overlapping results.
	sumBy := seekparquet.Fold[map[string]int64]{
		Columns: []string{"size", "storage_class"},
		New:     func() map[string]int64 { return map[string]int64{} },
		Add: func(acc *map[string]int64, rows *seekparquet.ValueBatch) error {
			sizes, labels := rows.Column(0), rows.Column(1)
			if sizes.Kind() != seekparquet.KindInt64 || labels.Present() && labels.Kind() != seekparquet.KindByteArray {
				return fmt.Errorf("unexpected column types")
			}
			for i := range rows.Len() {
				label := "(null)"
				if labels.Present() && !labels.IsNull(i) {
					label = string(labels.Bytes(i))
				}
				(*acc)[label] += sizes.Int64(i)
			}
			return nil
		},
		Merge: func(into, from *map[string]int64) {
			for label, size := range *from {
				(*into)[label] += size
			}
		},
	}
	results, err := seekparquet.FoldRows(ctx, d, plan.InputRowSpans(), sumBy,
		seekparquet.FoldOptions{ValueBudgetBytes: -1, Workers: 2, CheckKeys: true}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		fmt.Println(r.Status == seekparquet.AggregateExact, r.Value)
	}
	// Output:
	// true map[(null):25240398209 GLACIER:31427457570 STANDARD:46326142539]
	// true map[(null):9347359785 GLACIER:13075496879 STANDARD:20856533254]
}
