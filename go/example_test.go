// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"log"
	"os"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// exampleDataset writes a small dataset of object-store-like keys into a
// temporary directory and returns it with a function that removes it.
func exampleDataset() (string, func()) {
	dir, err := os.MkdirTemp("", "seekparquet-example-")
	if err != nil {
		log.Fatal(err)
	}
	err = testutil.Write(dir, testutil.Dataset{Parts: []testutil.Part{
		{Rows: testutil.Rows("logs/2026/01/a.gz", "logs/2026/01/b.gz", "logs/2026/02/a.gz", "logs/readme")},
		{Rows: testutil.Rows("photos/cat.jpg", "photos/dog.jpg")},
	}})
	if err != nil {
		log.Fatal(err)
	}
	return dir, func() { _ = os.RemoveAll(dir) }
}

// Read a key range: start inclusive, end exclusive.
func ExampleDataset_Range() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Range(ctx, seekparquet.Range{
		Start: []byte("logs/2026/01/b.gz"),
		End:   []byte("photos/dog.jpg"),
		Limit: 10,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(string(row.Key))
	}
	// Output:
	// logs/2026/01/b.gz
	// logs/2026/02/a.gz
	// logs/readme
	// photos/cat.jpg
}

// Read every key that begins with a prefix, a page of two rows at a time:
// each request continues after the last row of the page before.
func ExamplePrefixRange() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rng := seekparquet.PrefixRange([]byte("logs/2026/"), 2)
	for page := 1; ; page++ {
		rows, err := d.Range(ctx, rng)
		if err != nil {
			log.Fatal(err)
		}
		for _, row := range rows {
			fmt.Printf("page %d: %s\n", page, row.Key)
		}
		if len(rows) < rng.Limit {
			break
		}
		rng.Cursor = rows[len(rows)-1].Cursor()
	}
	// Output:
	// page 1: logs/2026/01/a.gz
	// page 1: logs/2026/01/b.gz
	// page 2: logs/2026/02/a.gz
}

// List a prefix like a directory, a page of one entry at a time.
func ExampleDataset_List() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	req := seekparquet.List{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1}
	for page := 1; ; page++ {
		entries, err := d.List(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			if e.IsPrefix() {
				fmt.Printf("page %d: prefix %s\n", page, e.CommonPrefix)
			} else {
				fmt.Printf("page %d: row %s\n", page, e.Row.Key)
			}
		}
		req.Cursor = entries[len(entries)-1].Cursor()
	}
	// Output:
	// page 1: prefix logs/2026/
	// page 2: row logs/readme
}

// Project columns: values, nulls, and columns a part does not have.
func ExampleRange_projection() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Range(ctx, seekparquet.Range{Prefix: []byte("logs/"), Limit: 3, Columns: []string{"size", "name", "owner"}})
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Print(string(row.Key))
		for i, v := range row.Values {
			switch {
			case v.Absent:
				fmt.Printf(" %s=absent", row.Columns[i])
			case v.IsNull():
				fmt.Printf(" %s=null", row.Columns[i])
			default:
				fmt.Printf(" %s=%v", row.Columns[i], v)
			}
		}
		fmt.Println()
	}
	// Output:
	// logs/2026/01/a.gz size=0 name=name-0 owner=absent
	// logs/2026/01/b.gz size=1 name=name-1 owner=absent
	// logs/2026/02/a.gz size=2 name=null owner=absent
}
