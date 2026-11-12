// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Windows has no directory sync in Go: there, local publishes only with
// AllowUnsynced, which the examples do not set.

//go:build !windows

package local_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// object is one row: the key column and one value column.
type object struct {
	Key  []byte `parquet:"key"`
	Size int64  `parquet:"size"`
}

// Write a new dataset from rows already sorted by key, then validate it.
func ExampleWriteDir() {
	parent, err := os.MkdirTemp("", "seekparquet-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(parent)
	dir := filepath.Join(parent, "objects") // WriteDir creates it

	schema := parquet.SchemaOf(new(object))
	objects := []object{ // in unsigned byte order of the key
		{Key: []byte("logs/a.gz"), Size: 10},
		{Key: []byte("logs/b.gz"), Size: 20},
		{Key: []byte("photos/cat.jpg"), Size: 30},
	}
	var rows []parquet.Row
	for i := range objects {
		rows = append(rows, schema.Deconstruct(nil, &objects[i]))
	}
	reader := parquet.RowReaderFunc(func(buf []parquet.Row) (int, error) {
		n := copy(buf, rows)
		rows = rows[n:]
		if len(rows) == 0 {
			return n, io.EOF
		}
		return n, nil
	})

	err = local.WriteDir(context.Background(), dir, schema, reader, local.DatasetOptions{
		Part: write.PartOptions{
			Key:      seekparquet.Key{Column: "key", Unique: true},
			SchemaID: "example/objects/v1",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	report, err := seekparquet.ValidateDir(context.Background(), dir, seekparquet.ValidateOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("validation:", report.Result())
	// Output:
	// validation: complete
}
