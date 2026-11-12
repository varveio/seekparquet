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
func ExampleDataset_Rows() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Start: []byte("logs/2026/01/b.gz"), End: []byte("photos/dog.jpg")}}, Limit: 10}, nil)
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
func ExamplePrefixSpan() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rng := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/2026/"))}, Limit: 2}
	for page := 1; ; page++ {
		rows, err := d.Rows(ctx, rng, nil)
		if err != nil {
			log.Fatal(err)
		}
		for _, row := range rows {
			fmt.Printf("page %d: %s\n", page, row.Key)
		}
		if int64(len(rows)) < rng.Limit {
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
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	req := seekparquet.ListQuery{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1}
	for page := 1; ; page++ {
		entries, err := d.List(ctx, req, nil)
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
func ExampleQuery_projection() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Prefix: []byte("logs/")}}, Limit: 3, Columns: []string{"size", "name", "owner"}}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Print(string(row.Key))
		for i, v := range row.Values {
			switch {
			case v.Absent():
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

// Read every row stored under one key.
func ExampleDataset_Get() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Get(ctx, []byte("photos/dog.jpg"), nil, "size")
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(string(row.Key), row.Values[0].Int64())
	}
	// Output:
	// photos/dog.jpg 1
}

// Stream every row of a prefix as borrowed batches, reading a column in
// place, and stop early with Stop.
func ExampleDataset_Scan() {
	dir, cleanup := exampleDataset()
	defer cleanup()
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/"))}, Columns: []string{"name"}}
	var st seekparquet.ReadStats
	err = d.Scan(ctx, q, &st, func(b *seekparquet.Batch) error {
		names := b.Column(0)
		for i := range b.Len() {
			if names.IsNull(i) {
				fmt.Printf("%s: no name\n", b.Key(i))
				return seekparquet.Stop
			}
			fmt.Printf("%s: %s\n", b.Key(i), names.Bytes(i))
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("key pages decoded:", st.KeyPagesDecoded)
	// Output:
	// logs/2026/01/a.gz: name-0
	// logs/2026/01/b.gz: name-1
	// logs/2026/02/a.gz: no name
	// key pages decoded: 1
}

// Carry a cursor between pages as a text token.
func ExampleParseCursor() {
	token, err := seekparquet.RowCursor([]byte("logs/readme"), 0).Encode()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(token)
	c, err := seekparquet.ParseCursor(token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %d\n", c.Key, c.Offset)
	// Output:
	// spc1.r.bG9ncy9yZWFkbWU.0
	// logs/readme 0
}
