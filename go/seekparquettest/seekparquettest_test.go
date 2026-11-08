// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquettest_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/seekparquettest"
)

// TestWriterMeetsTheProfile reads back what the writer wrote: the manifest,
// the stamps, sorting_columns and column_orders, a key page index whose
// boundary order is ASCENDING, and true page maxima where parquet-go would
// shorten an all-0xFF maximum below the keys.
func TestWriterMeetsTheProfile(t *testing.T) {
	ff := strings.Repeat("\xff", 30)
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("%s%04d", ff, i))
	}
	dir := t.TempDir()
	if err := seekparquettest.Write(dir, seekparquettest.Dataset{Parts: []seekparquettest.Part{{Rows: seekparquettest.Rows(keys...), GroupRows: 120}}}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Format        string `json:"format"`
		FormatVersion int    `json:"format_version"`
		DraftRevision int    `json:"draft_revision"`
		SchemaID      string `json:"schema_id"`
		Parts         []struct {
			Path  string `json:"path"`
			Rows  int    `json:"rows"`
			Bytes int64  `json:"bytes"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || m.Format != "seekparquet" || m.FormatVersion != 0 || m.DraftRevision != 1 || m.SchemaID != seekparquettest.SchemaID || len(m.Parts) != 1 || m.Parts[0].Rows != 200 {
		t.Fatalf("manifest %s: %v", raw, err)
	}
	path := filepath.Join(dir, filepath.FromSlash(m.Parts[0].Path))
	meta, err := seekparquettest.ReadFooter(path)
	if err != nil {
		t.Fatal(err)
	}
	stamps := map[string]string{}
	for _, kv := range meta.KeyValueMetadata {
		stamps[kv.Key] = kv.Value
	}
	if stamps[seekparquettest.StampFormatVersion] != "0" || stamps[seekparquettest.StampDraftRevision] != "1" || stamps[seekparquettest.StampSchemaID] != seekparquettest.SchemaID {
		t.Fatalf("stamps %v", stamps)
	}
	if len(meta.ColumnOrders) == 0 {
		t.Fatal("no column_orders")
	}
	if _, ok := meta.ColumnOrders[0].Value.(*format.TypeDefinedOrder); !ok {
		t.Fatalf("key column order %T", meta.ColumnOrders[0].Value)
	}
	file, _ := os.ReadFile(path)
	for g, rg := range meta.RowGroups {
		if len(rg.SortingColumns) == 0 || rg.SortingColumns[0].ColumnIdx != 0 || rg.SortingColumns[0].Descending {
			t.Fatalf("row group %d sorting columns %+v", g, rg.SortingColumns)
		}
		chunk := rg.Columns[0]
		var index format.ColumnIndex
		if err := thrift.Unmarshal(new(thrift.CompactProtocol), file[chunk.ColumnIndexOffset:chunk.ColumnIndexOffset+int64(chunk.ColumnIndexLength)], &index); err != nil {
			t.Fatal(err)
		}
		if index.BoundaryOrder != format.Ascending || len(index.MaxValues) < 2 {
			t.Fatalf("row group %d: %d pages, order %v", g, len(index.MaxValues), index.BoundaryOrder)
		}
		for i, max := range index.MaxValues {
			if !bytes.HasPrefix(max, []byte(ff)) || len(max) != len(ff)+4 {
				t.Fatalf("row group %d page %d maximum %q is not the full key", g, i, max)
			}
			if len(index.MinValues[i]) != 16 {
				t.Fatalf("row group %d page %d minimum is not shortened", g, i)
			}
		}
	}
}
