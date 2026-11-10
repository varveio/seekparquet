// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package samples defines datasets written through package local for
// tests: the local package's own, and the conformance job's, which checks
// them with the reference adapter and DuckDB (internal/cmd/samples).
package samples

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// Sample is one dataset: its schema, its rows in key order, and how
// WriteDir writes it.
type Sample struct {
	Name    string
	Schema  *parquet.Schema
	Rows    []parquet.Row
	Options local.DatasetOptions
}

// Reader returns a RowReader over the sample's rows.
func (s Sample) Reader() parquet.RowReader { return &rowSlice{rows: s.Rows} }

// Keys returns the key of every row.
func (s Sample) Keys() [][]byte {
	leaf, _ := s.Schema.Lookup(s.Options.Part.Key.Column)
	keys := make([][]byte, len(s.Rows))
	for i, row := range s.Rows {
		for _, v := range row {
			if v.Column() == leaf.ColumnIndex {
				keys[i] = v.ByteArray()
			}
		}
	}
	return keys
}

// binaryRow has an unannotated key and a value column of each physical
// type a reader must project (COL-3).
type binaryRow struct {
	Key     []byte   `parquet:"key"`
	Size    *int64   `parquet:"size,optional"`
	Name    *string  `parquet:"name,optional"`
	Flag    bool     `parquet:"flag"`
	Small   int32    `parquet:"small"`
	Ratio   float32  `parquet:"ratio"`
	Score   float64  `parquet:"score"`
	ID      [16]byte `parquet:"id"`
	Payload []byte   `parquet:"payload,optional"`
}

// stringRow has a key annotated STRING.
type stringRow struct {
	Key  string `parquet:"key"`
	Size int64  `parquet:"size"`
}

func value(i int, key []byte, payload int) binaryRow {
	r := binaryRow{Key: key, Flag: i%3 == 0, Small: int32(i) - 1000, Ratio: float32(i) / 7, Score: float64(i) * 1.5}
	r.ID[0], r.ID[15] = byte(i), byte(i>>8)
	if i%4 != 3 {
		size := int64(i) * 10
		r.Size = &size
	}
	if i%5 != 4 {
		name := fmt.Sprintf("name-%d", i)
		r.Name = &name
	}
	if payload > 0 {
		r.Payload = bytes.Repeat([]byte{byte(i), 0xff}, payload/2)
	}
	return r
}

func rowsOf[T any](schema *parquet.Schema, values []T) []parquet.Row {
	rows := make([]parquet.Row, len(values))
	for i := range values {
		rows[i] = schema.Deconstruct(nil, &values[i])
	}
	return rows
}

func options(name string, unique bool, partBytes int64) local.DatasetOptions {
	return local.DatasetOptions{
		Part:       write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: unique}, SchemaID: "samples/" + name},
		PartBytes:  partBytes,
		Extensions: json.RawMessage(fmt.Sprintf(`{"samples":{"name":%q}}`, name)),
	}
}

// All returns the samples: unique keys over several parts and row groups;
// repeated keys whose groups span pages, row groups and part targets, one
// of 5,000 rows; long binary keys, some beginning with 1,100 0xFF bytes,
// with values over 1,024 bytes, SNAPPY; STRING keys, GZIP; folder objects
// beside the keys under them, some repeated; and an empty dataset.
func All() []Sample {
	binary := parquet.SchemaOf(new(binaryRow))
	var out []Sample

	var unique []binaryRow
	for i := range 6000 {
		unique = append(unique, value(i, fmt.Appendf(nil, "logs/%04d/%02d/object-%d.gz", i/100, i%17, i), 0))
	}
	slices.SortFunc(unique, func(a, b binaryRow) int { return bytes.Compare(a.Key, b.Key) })
	s := Sample{Name: "unique", Schema: binary, Rows: rowsOf(binary, unique), Options: options("unique", true, 64<<10)}
	s.Options.Part.RowGroupRows = 2000
	out = append(out, s)

	var grouped []binaryRow
	for i := range 400 {
		reps := 1 + i%3
		switch {
		case i == 200:
			reps = 5000
		case i%9 == 4:
			reps = 300
		}
		for j := range reps {
			grouped = append(grouped, value(j, fmt.Appendf(nil, "group/%04d", i), 0))
		}
	}
	s = Sample{Name: "grouped", Schema: binary, Rows: rowsOf(binary, grouped), Options: options("grouped", false, 32<<10)}
	s.Options.Part.PageRows, s.Options.Part.RowGroupRows = 256, 1000
	out = append(out, s)

	var long []binaryRow
	for i := range 3000 {
		lead := strings.Repeat("a", 1100)
		if i >= 1500 {
			lead = strings.Repeat("\xff", 1100+i%3)
		}
		key := lead + string(rune('A'+i/100%26)) + strings.Repeat("z", i%7) + string(rune('0'+i%10)) + strings.Repeat("\x00", i%100)
		long = append(long, value(i, []byte(key), 1100+i%900))
	}
	slices.SortFunc(long, func(a, b binaryRow) int { return bytes.Compare(a.Key, b.Key) })
	long = slices.CompactFunc(long, func(a, b binaryRow) bool { return bytes.Equal(a.Key, b.Key) })
	s = Sample{Name: "long", Schema: binary, Rows: rowsOf(binary, long), Options: options("long", true, 1<<20)}
	s.Options.Part.Codec = &parquet.Snappy
	out = append(out, s)

	str := parquet.SchemaOf(new(stringRow))
	var strs []stringRow
	for i := range 3000 {
		strs = append(strs, stringRow{Key: fmt.Sprintf("café/%05d/日", i), Size: int64(i)})
	}
	s = Sample{Name: "string-key", Schema: str, Rows: rowsOf(str, strs), Options: options("string-key", true, 16<<10)}
	s.Options.Part.Codec = &parquet.Gzip
	out = append(out, s)

	// Folder objects: keys equal to their own delimiter rollups ("d0000/",
	// "d0000/s/"), some repeated, beside the keys under them, so that a
	// listing with delimiter '/' carries exact rows (LIST-8), at their
	// offsets in their groups.
	var folders []binaryRow
	for i := range 1200 {
		dir := fmt.Sprintf("d%04d/", i)
		for j := range 1 + i%3 {
			folders = append(folders, value(j, []byte(dir), 0))
		}
		folders = append(folders, value(i, []byte(dir+"s/"), 0), value(i, []byte(dir+"s/y"), 0), value(i, []byte(dir+"x"), 0))
	}
	slices.SortStableFunc(folders, func(a, b binaryRow) int { return bytes.Compare(a.Key, b.Key) })
	s = Sample{Name: "folders", Schema: binary, Rows: rowsOf(binary, folders), Options: options("folders", false, 16<<10)}
	s.Options.Part.PageRows, s.Options.Part.RowGroupRows = 64, 200
	out = append(out, s)

	out = append(out, Sample{Name: "empty", Schema: binary, Options: options("empty", true, 0)})
	return out
}

// rowSlice reads rows from a slice, copying each into the caller's buffer.
type rowSlice struct {
	rows []parquet.Row
	next int
}

func (r *rowSlice) ReadRows(buf []parquet.Row) (int, error) {
	n := 0
	for ; n < len(buf) && r.next < len(r.rows); n, r.next = n+1, r.next+1 {
		buf[n] = append(buf[n][:0], r.rows[r.next]...)
	}
	if r.next == len(r.rows) {
		return n, io.EOF
	}
	return n, nil
}
