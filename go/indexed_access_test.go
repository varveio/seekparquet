// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// Count requested bytes (including repeated reads), not OS or network I/O.
type accessSource struct {
	Source
	reads, bytes atomic.Int64
}
type accessFile struct {
	SourceFile
	owner *accessSource
}

func (s *accessSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return accessFile{f, s}, nil
}
func (f accessFile) ReadAt(p []byte, off int64) (int, error) {
	f.owner.reads.Add(1)
	f.owner.bytes.Add(int64(len(p)))
	return f.SourceFile.ReadAt(p, off)
}
func (s *accessSource) reset() { s.reads.Store(0); s.bytes.Store(0) }

// Sixteen independently routed parts, each with sixteen groups. A 128-byte
// shared stem also makes the 8-byte page bounds broad within each directory.
func TestIndexedAccessColdWarmLongPrefix(t *testing.T) {
	stem := strings.Repeat("x", 128) + "/"
	var chunks [][]string
	for p := range 16 {
		var keys []string
		for i := range 256 {
			keys = append(keys, fmt.Sprintf("%sp%02d/d%02d/f%03d", stem, p, i/16, i))
		}
		chunks = append(chunks, keys)
	}
	src := &accessSource{Source: dirOf(t, write(t, parts(testutil.Part{GroupRows: 16, PageBytes: 4096, IndexSizeLimit: 8}, chunks...)))}
	// Open is deliberately excluded: CHECK-1 opens every part.
	target := chunks[14][240]
	for _, n := range []int{1, 5} {
		d, err := OpenDataset(ctx, src, Options{})
		must(t, err)
		defer d.Close()
		for _, warm := range []bool{false, true} {
			src.reset()
			got, err := d.Range(ctx, Range{Start: []byte(target), Limit: n, Columns: []string{"size"}})
			must(t, err)
			if len(got) != n {
				t.Fatalf("got %d rows, want %d", len(got), n)
			}
			for i, row := range got {
				if string(row.Key) != chunks[14][240+i] || row.Values[0].Int64() != int64(240+i) {
					t.Fatalf("row %d: %+v", i, row)
				}
			}
			// At most all 16 group first pages/indexes plus the selected key/value
			// pages. The byte ceiling allows 4 KiB per group plus 16 KiB metadata.
			if src.reads.Load() > 56 || src.bytes.Load() > 80<<10 {
				t.Fatalf("reads=%d bytes=%d", src.reads.Load(), src.bytes.Load())
			}
			if warm && src.reads.Load() > 3 {
				t.Fatalf("warm request reads=%d, want selected key/value work only", src.reads.Load())
			}
			accessMetric(t, map[string]any{"operation": "range", "rows": n, "warm": warm, "reads": src.reads.Load(), "requested_bytes": src.bytes.Load()})
		}
		must(t, d.Close())
	}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	defer d.Close()
	for _, warm := range []bool{false, true} {
		src.reset()
		got, err := d.List(ctx, List{Prefix: []byte(fmt.Sprintf("%sp13/", stem)), Delimiter: '/', Limit: 100})
		must(t, err)
		if len(got) != 16 {
			t.Fatalf("got %d directories", len(got))
		}
		for i, e := range got {
			if string(e.CommonPrefix) != fmt.Sprintf("%sp13/d%02d/", stem, i) {
				t.Fatalf("entry %d: %+v", i, e)
			}
		}
		// One directory per group: at most one group-index load and two key
		// page reads per emitted prefix, plus routing probes.
		if src.reads.Load() > 96 || src.bytes.Load() > 160<<10 {
			t.Fatalf("listing reads=%d bytes=%d", src.reads.Load(), src.bytes.Load())
		}
		accessMetric(t, map[string]any{"operation": "list", "warm": warm, "reads": src.reads.Load(), "requested_bytes": src.bytes.Load()})
	}
}

// The list API must use the physical-row cursor even when every emitted row
// has the same key. A group recount would grow with each later request.
func TestIndexedAccessRepeatedListingResume(t *testing.T) {
	keys := []string{"a"}
	for range 5000 {
		keys = append(keys, "k")
	}
	keys = append(keys, "z")
	doc := parts(testutil.Part{GroupRows: 1000, PageBytes: 64}, keys)
	doc.RepeatedKeys = true
	d := open(t, write(t, doc), Options{})
	req := List{Prefix: []byte("k"), Delimiter: '/', Limit: 10, Columns: []string{"size"}}
	seen := 0
	var total, maxPages int64
	for {
		var st stats
		entries, err := d.r.readList(ctx, req, &st)
		must(t, err)
		for _, entry := range entries {
			if entry.Row == nil || string(entry.Row.Key) != "k" || entry.Row.Offset != int64(seen) || entry.Row.Values[0].Int64() != int64(seen+1) {
				t.Fatalf("row %d: %+v", seen, entry)
			}
			seen++
		}
		total += st.pages
		maxPages = max(maxPages, st.pages)
		// The group's first page, cursor page and selected key/value pages,
		// plus boundary probes: independent of the cursor's depth in the group.
		if st.pages > 12 {
			t.Fatalf("after %d rows decoded %d pages", seen, st.pages)
		}
		if len(entries) < req.Limit {
			break
		}
		req.Cursor = entries[len(entries)-1].Cursor()
	}
	if seen != 5000 {
		t.Fatalf("got %d repeated rows", seen)
	}
	accessMetric(t, map[string]any{"operation": "repeated_list", "rows": seen, "decoded_pages": total, "max_decoded_pages_per_request": maxPages})
}

// Grow the routing problem while preserving a fixed-size candidate group.
// Every key page has legal but deliberately broad/nonmonotone bounds, so
// seeking may scan the candidate group but must not scan preceding groups.
func TestIndexedAccessNonMonotoneScaling(t *testing.T) {
	for _, groups := range []int{10, 100, 1000} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			var keys []string
			for i := range groups * 16 {
				keys = append(keys, fmt.Sprintf("k%08d", i))
			}
			dir := write(t, parts(testutil.Part{GroupRows: 16, PageBytes: 64}, keys))
			broadNonMonotoneIndexes(t, dir)
			src := &accessSource{Source: dirOf(t, dir)}
			d, err := OpenDataset(ctx, src, Options{})
			must(t, err)
			defer d.Close()
			target := len(keys) - 8
			for _, warm := range []bool{false, true} {
				src.reset()
				var st stats
				got, err := d.r.readRange(ctx, Range{Start: []byte(keys[target]), Limit: 5}, &st)
				must(t, err)
				if len(got) != 5 {
					t.Fatalf("got %d rows", len(got))
				}
				for i, row := range got {
					if string(row.Key) != keys[target+i] {
						t.Fatalf("row %d: %q", i, row.Key)
					}
				}
				// Binary first-key routing, then at most all 16 rows/pages of the
				// candidate group. Index pair reads add at most two per routing probe.
				logarithm := 0
				for n := groups; n > 0; n >>= 1 {
					logarithm++
				}
				if st.pages > int64(logarithm+20) || src.reads.Load() > int64(6*logarithm+40) || src.bytes.Load() > int64(5000*logarithm+20000) {
					t.Fatalf("warm=%v pages=%d reads=%d bytes=%d", warm, st.pages, src.reads.Load(), src.bytes.Load())
				}
				accessMetric(t, map[string]any{"operation": "nonmonotone_range", "groups": groups, "warm": warm, "decoded_pages": st.pages, "reads": src.reads.Load(), "requested_bytes": src.bytes.Load()})
			}
		})
	}
}

func broadNonMonotoneIndexes(t *testing.T, dir string) {
	t.Helper()
	path := partFile(dir, 0)
	raw, err := os.ReadFile(path)
	must(t, err)
	meta, err := testutil.ReadFooter(path)
	must(t, err)
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	body := append([]byte(nil), raw[:len(raw)-8-footerLen]...)
	for g := range meta.RowGroups {
		col := &meta.RowGroups[g].Columns[0]
		var index format.ColumnIndex
		must(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw[col.ColumnIndexOffset:col.ColumnIndexOffset+int64(col.ColumnIndexLength)], &index))
		if len(index.MinValues) < 2 {
			t.Fatal("need multiple key pages per group")
		}
		for page := range index.MinValues {
			index.MinValues[page] = []byte("a")
			if page%2 == 0 {
				index.MaxValues[page] = []byte("z")
			}
		}
		index.BoundaryOrder = format.Unordered
		encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &index)
		must(t, err)
		col.ColumnIndexOffset = int64(len(body))
		col.ColumnIndexLength = int32(len(encoded))
		body = append(body, encoded...)
	}
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), meta)
	must(t, err)
	body = append(body, footer...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(footer)))
	body = append(body, "PAR1"...)
	must(t, os.WriteFile(path, body, 0o644))
	must(t, testutil.SyncSizes(dir))
}

// Machine-readable observations for the common regression runner. Budgets remain
// assertions beside the fixture geometry; this is not a format-level protocol.
func accessMetric(t *testing.T, metric map[string]any) {
	t.Helper()
	raw, err := json.Marshal(metric)
	must(t, err)
	t.Logf("ACCESS_METRIC %s", raw)
}
