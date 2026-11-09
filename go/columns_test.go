// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestColumnsAnnotated: the annotated conformance fixture's columns, as its
// generator declares them, from Columns before and after Verify and from
// InspectFooter, which agree. Columns checks the part it answers for.
func TestColumnsAnnotated(t *testing.T) {
	ms := &format.LogicalType{Value: &format.TimestampType{Unit: format.TimeUnit{Value: &format.MilliSeconds{}}}}
	us := &format.LogicalType{Value: &format.TimestampType{IsAdjustedToUTC: true, Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}}
	decimal := func(precision, scale int32) *format.LogicalType {
		return &format.LogicalType{Value: &format.DecimalType{Precision: precision, Scale: scale}}
	}
	want := []Column{
		{Name: "key", Type: format.ByteArray, Repetition: format.Required, Key: true},
		{Name: "d", Type: format.Int32, Logical: &format.LogicalType{Value: &format.DateType{}}, Repetition: format.Optional},
		{Name: "ts_ms", Type: format.Int64, Logical: ms, Repetition: format.Optional},
		{Name: "ts_utc", Type: format.Int64, Logical: us, Repetition: format.Optional},
		{Name: "dec9", Type: format.Int32, Logical: decimal(9, 2), Repetition: format.Optional},
		{Name: "dec18", Type: format.Int64, Logical: decimal(18, 4), Repetition: format.Optional},
		{Name: "dec30", Type: format.FixedLenByteArray, Length: 13, Logical: decimal(30, 5), Repetition: format.Optional},
		{Name: "s", Type: format.ByteArray, Logical: &format.LogicalType{Value: &format.StringType{}}, Repetition: format.Optional},
	}
	dir := "../conformance/datasets/annotated"
	d := open(t, dir, Options{})
	got, err := d.Columns(ctx, 0)
	must(t, err)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("before Verify:\n got %+v\nwant %+v", got, want)
	}
	if !d.r.parts[0].ready.Load() {
		t.Fatal("Columns answered without checking the part")
	}
	got[0].Name, got[1].Logical.Value = "changed", &format.StringType{}
	must(t, d.Verify(ctx))
	if got, err = d.Columns(ctx, 0); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("after Verify: %v\n got %+v\nwant %+v", err, got, want)
	}
	part := filepath.Join(dir, filepath.FromSlash(d.Metadata().Parts[0].Path))
	f, err := os.Open(part)
	must(t, err)
	defer f.Close()
	ft, err := InspectFooter(f, d.Metadata().Parts[0].Bytes)
	must(t, err)
	if !reflect.DeepEqual(ft.Schema, want) {
		t.Fatalf("InspectFooter:\n got %+v\nwant %+v", ft.Schema, want)
	}
	for _, i := range []int{-1, 1} {
		if _, err := d.Columns(ctx, i); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("part %d: %v, want ErrInvalidRequest", i, err)
		}
	}
	must(t, d.Close())
	if _, err := d.Columns(ctx, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v, want ErrClosed", err)
	}
}

// TestColumnsInconsistent: a second part whose column differs in physical
// type is refused by COL-5, by Columns as by a read, whichever part is
// checked first; the part checked first still answers.
func TestColumnsInconsistent(t *testing.T) {
	type narrow struct {
		Key  []byte `parquet:"key"`
		Size int32  `parquet:"size"`
	}
	type wide struct {
		Key  []byte `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: []narrow{{[]byte("a"), 1}}},
		{Rows: []wide{{[]byte("b"), 2}}},
	}})
	types := []format.Type{format.Int32, format.Int64}
	for first := range 2 {
		d := open(t, dir, Options{})
		cols, err := d.Columns(ctx, first)
		must(t, err)
		if len(cols) != 2 || cols[1].Name != "size" || cols[1].Type != types[first] {
			t.Fatalf("part %d: %+v", first, cols)
		}
		if _, err := d.Columns(ctx, 1-first); CodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("part %d after %d: %v, want inconsistent_column", 1-first, first, err)
		}
		if err := d.Verify(ctx); CodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("Verify: %v, want inconsistent_column", err)
		}
		if again, err := d.Columns(ctx, first); err != nil || !reflect.DeepEqual(again, cols) {
			t.Fatalf("part %d again: %+v, %v", first, again, err)
		}
	}
}

// TestColumnsConcurrent races Columns with reads on unchecked parts.
func TestColumnsConcurrent(t *testing.T) {
	d := open(t, write(t, lazyParts()), Options{})
	var wg sync.WaitGroup
	for i := range 16 {
		part := i % len(d.r.parts)
		wg.Go(func() {
			cols, err := d.Columns(ctx, part)
			if err != nil || len(cols) == 0 || !cols[0].Key {
				t.Errorf("part %d: %+v, %v", part, cols, err)
			}
		})
		wg.Go(func() {
			if rows, err := d.Range(ctx, PrefixRange([]byte{"abcd"[part]}, 100)); err != nil || len(rows) != 25 {
				t.Errorf("part %d: %d rows, %v", part, len(rows), err)
			}
		})
	}
	wg.Wait()
}
