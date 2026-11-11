// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// declarePartRows changes only metadata, not the encoded rows. Default
// open must handle even implausibly large counts before it decodes pages.
func declarePartRows(t *testing.T, dir string, part int, rows int64) {
	t.Helper()
	rewrite(t, dir, part, func(m *format.FileMetaData) {
		m.NumRows = rows
		m.RowGroups[0].NumRows = rows
		for c := range m.RowGroups[0].Columns {
			m.RowGroups[0].Columns[c].MetaData.NumValues = rows
		}
	})
	editManifest(t, dir, func(m map[string]any) {
		testutil.ManifestPart(m, part)["rows"] = rows
	})
}

func TestDatasetRowTotalLimit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second int64
		tooLarge      bool
	}{
		{"maximum total", math.MaxInt64 - 1, 1, false},
		{"maximum plus one", math.MaxInt64, 1, true},
		{"two large parts", 1 << 62, 1 << 62, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{}, []string{"a"}, []string{"b"}))
			declarePartRows(t, dir, 0, tc.first)
			declarePartRows(t, dir, 1, tc.second)
			src := &countingSource{Source: dirOf(t, dir)}
			d, err := OpenDataset(ctx, src, OpenOptions{})
			if d != nil {
				defer d.Close()
			}
			if tc.tooLarge {
				if !errors.Is(err, ErrLimitExceeded) || RefusalCodeOf(err) != "" {
					t.Fatalf("OpenDataset: %v, want non-refusal ErrLimitExceeded", err)
				}
				if src.opened.Load() != src.closed.Load() {
					t.Fatalf("failed open leaked files: opened %d, closed %d", src.opened.Load(), src.closed.Load())
				}
				return
			}
			must(t, err)
			if src.opened.Load() != 1 {
				t.Fatalf("open opened %d files, want the manifest only", src.opened.Load())
			}
			must(t, d.r.checkParts(ctx, new(ReadStats)))
			if second := d.r.parts[1].groups[0].start; d.r.rows != math.MaxInt64 || second != tc.first {
				t.Fatalf("total %d, second group starts at %d", d.r.rows, second)
			}
		})
	}
}

// The first part is never decoded. Its large declared count puts a real
// repeated-key tail at the largest supported dataset ordinals.
func TestResumeAtMaximumDatasetRowTotal(t *testing.T) {
	doc := parts(testutil.Part{}, []string{"a"}, []string{"b", "b", "c"})
	doc.RepeatedKeys = true
	dir := write(t, doc)
	declarePartRows(t, dir, 0, math.MaxInt64-3)
	d := open(t, dir, OpenOptions{})
	for _, offset := range []int64{0, 1, math.MaxInt64} {
		rows, err := d.Rows(ctx, Query{Selection: Selection{Cursor: &Cursor{Key: []byte("b"), Offset: offset}}, Limit: 3}, nil)
		must(t, err)
		if offset == 0 {
			if len(rows) != 2 || string(rows[0].Key) != "b" || rows[0].Offset != 1 || string(rows[1].Key) != "c" {
				t.Fatalf("resume after b/0: %+v", rows)
			}
		} else if len(rows) != 1 || string(rows[0].Key) != "c" {
			t.Fatalf("resume after b/%d: %+v", offset, rows)
		}
	}
}

// Before the row-total check, this opened with a wrapped total and
// resuming after b/0 returned c/0 without error, skipping 99 real rows.
// Early pages are real; only the last page's declared row count is false.
func TestOverflowCannotSkipRepeatedKeyTail(t *testing.T) {
	keys := make([]string, 400)
	for i := range keys {
		keys[i] = string(rune('b' + i/100))
	}
	keys = append(keys, "f")
	doc := parts(testutil.Part{PageBytes: 32}, []string{"a"}, keys)
	doc.RepeatedKeys = true
	dir := write(t, doc)
	declarePartRows(t, dir, 0, 1<<62)
	declarePartRows(t, dir, 1, 1<<62)
	if err := openErr(dir, OpenOptions{}); !errors.Is(err, ErrLimitExceeded) || RefusalCodeOf(err) != "" {
		t.Fatalf("overflowing repeated-key dataset: %v, want non-refusal ErrLimitExceeded", err)
	}
}
