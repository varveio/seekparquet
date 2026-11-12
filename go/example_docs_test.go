// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	seekparquet "github.com/varveio/seekparquet/go"
)

// The examples in this file are embedded in the documentation (README.md,
// docs/guides) by go/internal/tools/docgen: change them there and here
// together by running go -C go run ./internal/tools/docgen -w. They read
// the conformance suite's checked-in fixtures.

// Read a prefix, then list it like a directory, a page at a time.
func Example_readme() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// The first three keys under a prefix.
	q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}, Limit: 3}
	rows, err := d.Rows(ctx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Printf("row %s\n", row.Key)
	}

	// Common prefixes under logs/2026/, two per page, resuming from a cursor.
	list := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 2}
	for page := 1; ; page++ {
		entries, err := d.List(ctx, list, nil)
		if err != nil {
			log.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			fmt.Printf("page %d: %s\n", page, e.Bytes())
		}
		list.Cursor = entries[len(entries)-1].Cursor()
	}
	// Output:
	// row logs/2026/01/f000.gz
	// row logs/2026/01/f001.gz
	// row logs/2026/01/f002.gz
	// page 1: logs/2026/01/
	// page 1: logs/2026/02/
	// page 2: logs/2026/03/
}

// Validate a local dataset at the full level, which runs the structural
// and profile levels first.
func ExampleValidateDir() {
	ctx := context.Background()
	report, err := seekparquet.ValidateDir(ctx, "../conformance/datasets/multipart", seekparquet.ValidateOptions{Level: seekparquet.LevelFull})
	if err != nil {
		log.Fatal(err) // the directory cannot be opened, or ctx ended
	}
	for _, level := range report.Levels {
		fmt.Println(level.Level, level.Result)
	}
	fmt.Println("result:", report.Result())
	// Output:
	// structural complete
	// profile complete
	// full complete
	// result: complete
}

// Tell a refusal of the data from the other kinds of error.
func Example_errors() {
	ctx := context.Background()
	// A fixture whose keys decrease inside one page: opening succeeds,
	// because the fault is in a page, and the read that decodes it refuses.
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/invalid/key-order-within-page", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	_, err = d.Rows(ctx, seekparquet.Query{Limit: 1000}, nil)

	var refusal *seekparquet.RefusalError
	var unsupported *seekparquet.UnsupportedError
	var limit *seekparquet.LimitError
	switch {
	case errors.As(err, &refusal):
		// A fault in the data, or a version pair this reader does not
		// support (unsupported_version). The code is stable.
		fmt.Println("refused:", refusal.Code, "in", refusal.Part)
	case errors.As(err, &unsupported):
		// A feature this reader does not implement. It says nothing
		// about the validity of the rest of the dataset.
		fmt.Println("unsupported:", unsupported.Code)
	case errors.As(err, &limit):
		// Your Limits, or a capacity of the reader; not a fault of the data.
		fmt.Println("limit:", limit.Limit)
	case errors.Is(err, seekparquet.ErrInvalidRequest):
		fmt.Println("invalid request:", err)
	case err != nil:
		// Storage, cancellation, or a closed Dataset.
		fmt.Println("environment:", err)
	}
	fmt.Println("code:", seekparquet.RefusalCodeOf(err))
	// Output:
	// refused: key_order in data/part-00000.parquet
	// code: key_order
}
