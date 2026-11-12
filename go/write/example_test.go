// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Windows has no directory sync in Go: there, local publishes only with
// AllowUnsynced, which the example does not set.

//go:build !windows

package write_test

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// Drive the steps WriteDir takes yourself: write a part, publish it
// durably, then commit the manifest. A producer does this to choose its
// own part boundaries, names and recovery.
func ExampleNewPart() {
	dir, err := os.MkdirTemp("", "seekparquet-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	root, err := os.OpenRoot(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer root.Close()
	if err := root.Mkdir("data", 0o755); err != nil {
		log.Fatal(err)
	}
	// Make the root's entry in its parent, and data's entry in the root,
	// durable before publishing a part (the local implementation of COMMIT-1).
	for _, path := range []string{filepath.Dir(dir), dir} {
		d, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		if err := errors.Join(d.Sync(), d.Close()); err != nil {
			log.Fatal(err)
		}
	}
	ops := local.Ops{Root: root}

	type object struct {
		Key  []byte `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	schema := parquet.SchemaOf(new(object))
	key := seekparquet.Key{Column: "key", Unique: true}

	// 1. Write one part to a temporary name. The writer checks every key
	// as it arrives and fails with write.ErrProfile rather than write a
	// part a reader would refuse.
	f, err := root.Create("data/part-00000.parquet.tmp")
	if err != nil {
		log.Fatal(err)
	}
	w, err := write.NewPart(f, schema, write.PartOptions{Key: key, SchemaID: "example/objects/v1"})
	if err != nil {
		log.Fatal(err)
	}
	for i, k := range []string{"logs/a.gz", "logs/b.gz", "photos/cat.jpg"} {
		row := object{Key: []byte(k), Size: int64(10 * (i + 1))}
		if _, err := w.WriteRows([]parquet.Row{schema.Deconstruct(nil, &row)}); err != nil {
			log.Fatal(err)
		}
	}
	part, err := w.Seal() // rows, size, exact first and last keys, SHA-256
	if err != nil {
		log.Fatal(err)
	}

	// 2. Publish it under its final name: sync, rename, sync the directory.
	part.Path = "data/part-00000.parquet"
	if err := local.PublishPart(ops, "data/part-00000.parquet.tmp", part.Path); err != nil {
		log.Fatal(err)
	}

	// 3. Commit the manifest atomically. Until then the root holds no dataset.
	m := seekparquet.Manifest{
		Key:      key,
		Version:  seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision},
		SchemaID: "example/objects/v1",
		Parts:    []seekparquet.PartInfo{part},
	}
	if _, err := local.Commit(ops, m); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %d rows, %s to %s\n", part.Path, part.Rows, part.MinKey, part.MaxKey)
	// Output:
	// data/part-00000.parquet: 3 rows, logs/a.gz to photos/cat.jpg
}
