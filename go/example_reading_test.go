// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil/fakestore"
)

func ExampleOpenDir() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	fmt.Println("key:", d.Metadata().Key.Column)
	fmt.Println("parts:", len(d.Metadata().Parts))
	// Output:
	// key: key
	// parts: 3
}

func ExampleSpan_exclusive() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{
		Start: []byte("logs/2026/01/f000.gz"), StartExclusive: true,
		End: []byte("logs/2026/01/f003.gz"),
	}}, Limit: 10}
	rows, err := d.Rows(ctx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(string(row.Key))
	}
	// Output:
	// logs/2026/01/f001.gz
	// logs/2026/01/f002.gz
}

func ExampleDataset_Get_largeGroup() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/one-large-group", seekparquet.OpenOptions{Limits: seekparquet.Limits{GetRows: 8}})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	_, err = d.Get(ctx, []byte("only"), nil, "size")
	var limit *seekparquet.LimitError
	if !errors.As(err, &limit) || limit.Limit != seekparquet.LimitGetRows {
		log.Fatal(err)
	}
	fmt.Println("lookup exceeds:", limit.Limit)
	q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan([]byte("only"))}, Limit: 10, Columns: []string{"size"}}
	for {
		rows, err := d.Rows(ctx, q, nil)
		if err != nil {
			log.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		fmt.Printf("%d rows, offsets %d..%d\n", len(rows), rows[0].Offset, rows[len(rows)-1].Offset)
		q.Cursor = rows[len(rows)-1].Cursor()
	}
	// Output:
	// lookup exceeds: GetRows
	// 10 rows, offsets 0..9
	// 10 rows, offsets 10..19
	// 4 rows, offsets 20..23
}

func ExampleListEntry_exact() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/delimiter", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: []byte("a/"), Delimiter: '/', Limit: 10, Columns: []string{"size"}}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsPrefix() && entry.Exact != nil {
			fmt.Printf("prefix %s, exact row %s\n", entry.CommonPrefix, entry.Exact.Key)
		}
	}
	// Output:
	// prefix a//, exact row a//
	// prefix a/b/, exact row a/b/
}

func Example_paginate() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	q := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 2}
	for {
		entries, err := d.List(ctx, q, nil)
		if err != nil {
			log.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			fmt.Println(string(entry.Bytes()))
		}
		// Carry the token through your service response and next request.
		token, err := entries[len(entries)-1].Cursor().Encode()
		if err != nil {
			log.Fatal(err)
		}
		q.Cursor, err = seekparquet.ParseCursor(token)
		if err != nil {
			log.Fatal(err)
		}
	}
	// Output:
	// logs/2026/01/
	// logs/2026/02/
	// logs/2026/03/
}

func ExampleValue_absent() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/projection", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan([]byte("row-04"))}, Limit: 1, Columns: []string{"extra", "i32", "req"}}, nil)
	if err != nil {
		log.Fatal(err)
	}
	for i, value := range rows[0].Values {
		switch {
		case value.Absent():
			fmt.Println(rows[0].Columns[i], "absent")
		case value.IsNull():
			fmt.Println(rows[0].Columns[i], "null")
		default:
			fmt.Println(rows[0].Columns[i], value.Kind())
		}
	}
	// Output:
	// extra absent
	// i32 null
	// req INT64
}

func ExampleColumnView() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	err = d.Scan(ctx, seekparquet.Query{Limit: 3, Columns: []string{"size"}}, nil, func(b *seekparquet.Batch) error {
		sizes := b.Column(0)
		if !sizes.Present() || sizes.Kind() != seekparquet.KindInt64 {
			return fmt.Errorf("size must be INT64")
		}
		for i := range b.Len() {
			if !sizes.IsNull(i) {
				fmt.Println(string(b.Key(i)), sizes.Int64(i))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// README 2052263231
	// app/v1/obj-000 572045545
	// app/v1/obj-001 2107371739
}

func ExampleDataset_Plan() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	p, err := d.Plan(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}}, []string{"size"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	rows, rowsExact := p.Count()
	bytes, bytesExact := p.ProjectedBytes()
	// These are application admission thresholds, not reader defaults.
	if !rowsExact || !bytesExact || rows > 100 || bytes > 1<<20 {
		log.Fatal("read exceeds admission policy or needs a separate estimate")
	}
	fmt.Println("rows:", rows, "exact:", rowsExact)
	fmt.Println("projected bytes exact:", bytesExact)
	var scanned int
	err = p.Scan(ctx, nil, func(b *seekparquet.Batch) error { scanned += b.Len(); return nil })
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("scanned:", scanned)
	// Output:
	// rows: 30 exact: true
	// projected bytes exact: true
	// scanned: 30
}

func ExamplePlan_Split() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	p, err := d.Plan(ctx, []seekparquet.Selection{{}}, []string{"size"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	shards, err := p.Split(ctx, 4, nil)
	if err != nil {
		log.Fatal(err)
	}
	counts := make([]int, len(shards))
	failures := make([]error, len(shards))
	stats := make([]seekparquet.ReadStats, len(shards))
	var wg sync.WaitGroup
	for i, shard := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			failures[i] = shard.Scan(ctx, &stats[i], func(b *seekparquet.Batch) error { counts[i] += b.Len(); return nil })
		}()
	}
	wg.Wait()
	var total int
	for i, err := range failures {
		if err != nil {
			log.Fatal(err)
		}
		total += counts[i]
	}
	fmt.Println("scanned:", total)
	// Output:
	// scanned: 134
}

func ExampleDataset_Columns() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/projection", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	columns, err := d.Columns(ctx, 0)
	if err != nil {
		log.Fatal(err)
	}
	for _, column := range columns {
		if column.Key {
			fmt.Println(column.String())
		}
	}
	// Output:
	// key BYTE_ARRAY REQUIRED STRING key
}

func ExampleReadStats() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	var stats seekparquet.ReadStats
	rows, err := d.Rows(ctx, seekparquet.Query{Limit: 1, Columns: []string{"size"}}, &stats)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("rows:", len(rows))
	fmt.Println("logical part reads:", stats.SourceReads > 0)
	fmt.Println("key pages decoded:", stats.KeyPagesDecoded > 0)
	fmt.Println("value pages decoded:", stats.ValuePagesDecoded > 0)
	// Output:
	// rows: 1
	// logical part reads: true
	// key pages decoded: true
	// value pages decoded: true
}

func ExampleReadStats_listing() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/delimiter", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	var stats seekparquet.ReadStats
	entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: []byte("a/"), Delimiter: '/', Limit: 3, Columns: []string{"size"}}, &stats)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("entries:", len(entries))
	fmt.Println("logical part reads:", stats.SourceReads > 0)
	fmt.Println("key pages decoded:", stats.KeyPagesDecoded > 0)
	fmt.Println("value pages decoded:", stats.ValuePagesDecoded > 0)
	// Output:
	// entries: 3
	// logical part reads: true
	// key pages decoded: true
	// value pages decoded: true
}

func ExampleCursor_Encode() {
	row, err := seekparquet.RowCursor([]byte("logs/a"), 2).Encode()
	if err != nil {
		log.Fatal(err)
	}
	prefix, err := seekparquet.PrefixCursor([]byte("logs/")).Encode()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(row)
	fmt.Println(prefix)
	_, err = seekparquet.ParseCursor("spc1.r.bad=.0")
	fmt.Println("invalid request:", errors.Is(err, seekparquet.ErrInvalidRequest))
	// Output:
	// spc1.r.bG9ncy9h.2
	// spc1.p.bG9ncy8
	// invalid request: true
}

func ExampleLimits_untrusted() {
	// Illustrative admission profiles. Match them to your producer and workload.
	profiles := []struct {
		name   string
		limits seekparquet.Limits
	}{
		{"local trusted", seekparquet.Limits{}},
		{"remote untrusted", seekparquet.Limits{ManifestBytes: 1 << 20, Parts: 100, FooterBytes: 1 << 20, PageBytes: 4 << 20, PageValues: 1 << 16, PageMemory: 16 << 20, IndexMemory: 16 << 20}},
		{"memory tight", seekparquet.Limits{PageBytes: 1 << 20, PageValues: 1 << 14, PageMemory: 4 << 20, IndexMemory: 4 << 20, OffsetIndexMemory: 1, PageIndexCacheMemory: 1}},
	}
	for _, profile := range profiles {
		d, err := seekparquet.OpenDir(context.Background(), "../conformance/datasets/multipart", seekparquet.OpenOptions{Limits: profile.limits})
		if err != nil {
			log.Fatal(err)
		}
		_, err = d.Rows(context.Background(), seekparquet.Query{Limit: 1}, nil)
		closeErr := d.Close()
		if err != nil {
			log.Fatal(err)
		}
		if closeErr != nil {
			log.Fatal(closeErr)
		}
		fmt.Println(profile.name, "read succeeds")
	}
	// Output:
	// local trusted read succeeds
	// remote untrusted read succeeds
	// memory tight read succeeds
}

func ExampleDataset_BuildListIndex() {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	built, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{MaxBytes: 1 << 20, Workers: 1, MaxChangesPerPage: 8})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("has index:", built.HasIndex)
	fmt.Println("within cache budget:", built.Bytes <= built.Options.MaxBytes)
	entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 4}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("entries:", len(entries))
	// Output:
	// has index: true
	// within cache budget: true
	// entries: 3
}

func TestGuideMemorySource(t *testing.T) {
	ctx := context.Background()
	objects := make(map[string]fakestore.Object)
	err := filepath.WalkDir("../conformance/datasets/multipart", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name, err := filepath.Rel("../conformance/datasets/multipart", path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		objects[filepath.ToSlash(name)] = fakestore.Object{Data: data}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	store := fakestore.New(objects, fakestore.Options{})
	defer store.Close()
	// fakeSource is the existing test-only Source implementation; see the guide.
	d, err := seekparquet.OpenDataset(ctx, fakeSource{store}, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Rows(ctx, seekparquet.Query{Limit: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if got := string(rows[0].Key); got != "README" {
		t.Fatalf("first key = %q, want README", got)
	}
}
