// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command quickstart reads the checked-in multipart conformance fixture.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	seekparquet "github.com/varveio/seekparquet/go"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: quickstart DATASET_DIRECTORY")
		os.Exit(2)
	}
	if err := run(context.Background(), os.Args[1], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dir string, out io.Writer) error {
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		return err
	}
	defer d.Close()

	// Preview the first two keys under January, without reading value columns.
	rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}, Limit: 2}, nil)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(out, "preview: %s\n", row.Key); err != nil {
			return err
		}
	}

	// List the months, one common prefix per page. Keep the same query and
	// dataset when resuming; ListEntry.Cursor handles both rows and prefixes.
	req := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 1}
	for page := 1; ; page++ {
		entries, err := d.List(ctx, req, nil)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		for _, entry := range entries {
			kind, key := "prefix", entry.CommonPrefix
			if !entry.IsPrefix() {
				kind, key = "row", entry.Row.Key
			}
			if _, err := fmt.Fprintf(out, "page %d: %s %s\n", page, kind, key); err != nil {
				return err
			}
		}
		req.Cursor = entries[len(entries)-1].Cursor()
	}
}
