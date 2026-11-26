// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"slices"
	"sync/atomic"

	seekparquet "github.com/varveio/seekparquet/go"
)

func ExampleFold() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// A fold sees physical values, whatever their annotation. Check that
	// every part stores size as a plain or signed INT64 before summing it.
	for part := range d.Metadata().Parts {
		columns, err := d.Columns(ctx, part, nil)
		if err != nil {
			log.Fatal(err)
		}
		i := slices.IndexFunc(columns, func(c seekparquet.Column) bool { return c.Name == "size" })
		if i < 0 || columns[i].Type != seekparquet.KindInt64 {
			log.Fatalf("part %d: size is not an INT64 column", part)
		}
		if l := columns[i].Logical; l.Kind != seekparquet.LogicalNone && (l.Kind != seekparquet.LogicalInt || !l.Signed) {
			log.Fatalf("part %d: size is annotated %s", part, l)
		}
	}

	plan, err := d.Plan(ctx, []seekparquet.Selection{
		{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))},
		{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
	}, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Count rows, and sum and count the nulls of size. The sum is exact
	// however FoldRows groups the rows, so check its range on the result.
	type totals struct {
		rows, nulls int64
		size        *big.Int
	}
	fold := seekparquet.Fold[totals]{
		Columns: []string{"size"},
		New:     func() totals { return totals{size: new(big.Int)} },
		Add: func(acc *totals, rows *seekparquet.ValueBatch) error {
			sizes := rows.Column(0)
			acc.rows += int64(rows.Len())
			var n big.Int
			for i := range rows.Len() {
				if sizes.IsNull(i) {
					acc.nulls++
				} else {
					acc.size.Add(acc.size, n.SetInt64(sizes.Int64(i)))
				}
			}
			return nil
		},
		Merge: func(into, from *totals) {
			into.rows += from.rows
			into.nulls += from.nulls
			into.size.Add(into.size, from.size)
		},
	}
	results, err := seekparquet.FoldRows(ctx, d, plan.InputRowSpans(), fold,
		seekparquet.FoldOptions{ValueBudgetBytes: 4 << 20, CheckKeys: true}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		switch {
		case r.Status != seekparquet.FoldExact:
			fmt.Println("not exact")
		case !r.Value.size.IsInt64():
			fmt.Println("size overflows int64")
		default:
			fmt.Printf("rows=%d size=%d nulls=%d\n", r.Value.rows, r.Value.size, r.Value.nulls)
		}
	}
	// Output:
	// rows=30 size=31543127898 nulls=0
	// rows=40 size=43279389918 nulls=0
}

func ExampleFoldOptions_checkKeys() {
	ctx := context.Background()
	// A fixture whose keys decrease inside one page.
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/invalid/key-order-within-page", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	plan, err := d.Plan(ctx, []seekparquet.Selection{{}}, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// The count comes from checked row counts: it decodes no key.
	n, exact := plan.Count()
	fmt.Printf("count: %d, exact=%t\n", n, exact)

	// A fold with no columns and CheckKeys counts the rows and checks
	// every key it selects.
	count := seekparquet.Fold[int64]{
		Add:   func(acc *int64, rows *seekparquet.ValueBatch) error { *acc += int64(rows.Len()); return nil },
		Merge: func(into, from *int64) { *into += *from },
	}
	_, err = seekparquet.FoldRows(ctx, d, plan.InputRowSpans(), count, seekparquet.FoldOptions{CheckKeys: true}, nil)
	fmt.Println("checked count:", seekparquet.RefusalCodeOf(err))
	// Output:
	// count: 12, exact=true
	// checked count: key_order
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

func ExampleDataset_List_pages() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	if err := d.Verify(ctx, nil); err != nil {
		log.Fatal(err)
	}
	// Pages of two entries. Each listing asks for one entry more, to learn
	// whether another page follows and to establish the rows of the last
	// common prefix it keeps.
	const pageSize = 2
	q := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: pageSize + 1}
	for {
		entries, err := d.List(ctx, q, nil)
		if err != nil {
			log.Fatal(err)
		}
		more := len(entries) > pageSize
		if more {
			entries = entries[:pageSize]
		}
		for _, entry := range entries {
			_, startKnown := entry.Rows.Start.Ordinal()
			_, endKnown := entry.Rows.End.Ordinal()
			if !startKnown || !endKnown {
				log.Fatal("use a key-range plan for an unknown boundary")
			}
			// Counting known rows reads nothing.
			p, err := d.PlanRows(ctx, []seekparquet.RowSpan{entry.Rows}, nil, nil)
			if err != nil {
				log.Fatal(err)
			}
			n, _ := p.Count()
			fmt.Printf("%s: %d rows\n", entry.Bytes(), n)
		}
		if !more {
			break
		}
		q.Cursor = entries[len(entries)-1].Cursor()
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
		seekparquet.FoldOptions{Workers: 2, CheckKeys: true}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		fmt.Println(r.Status == seekparquet.FoldExact, r.Value)
	}
	// Output:
	// true map[(null):25240398209 GLACIER:31427457570 STANDARD:46326142539]
	// true map[(null):9347359785 GLACIER:13075496879 STANDARD:20856533254]
}
