// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/encoding/rle"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

type reuseWriterOptions []parquet.WriterOption

func (options reuseWriterOptions) ConfigureWriter(c *parquet.WriterConfig) {
	for _, option := range options {
		option.ConfigureWriter(c)
	}
}

func TestBufferReusePhysicalTransitions(t *testing.T) {
	type record struct {
		Key   []byte  `parquet:"key"`
		RLE   bool    `parquet:"rle"`
		Plain bool    `parquet:"plain,plain"`
		Fixed [8]byte `parquet:"fixed"`
		Bytes []byte  `parquet:"bytes,plain"`
	}
	records := make([]record, 2048)
	for i := range records {
		records[i] = record{Key: fmt.Appendf(nil, "k%05d", i), RLE: i%3 == 0, Plain: i/32%2 == 0, Fixed: [8]byte{byte(i), byte(i >> 8), 0xff}, Bytes: fmt.Appendf(nil, "value-%05d", i)}
	}
	columns := []string{"rle", "plain", "fixed", "bytes"}
	for _, codec := range []compress.Codec{&uncompressed.Codec{}, &zstd.Codec{}} {
		for _, version := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/v%d", codec, version), func(t *testing.T) {
				options := reuseWriterOptions{parquet.Compression(codec), parquet.DefaultEncodingFor(parquet.Boolean, &rle.Encoding{BitWidth: 1})}
				d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 32, DataPageVersion: version, Compression: options}}}), OpenOptions{})
				must(t, d.r.checkParts(ctx, new(ReadStats)))
				for i, name := range columns {
					cp, err := d.r.parts[0].openColumn(ctx, 0, name, new(ReadStats), nil)
					must(t, err)
					if len(cp.locs) < 2 {
						t.Fatalf("%s needs page transitions", name)
					}
					if cp.c.col.optional {
						t.Fatalf("%s must be required", name)
					}
					for _, loc := range cp.locs {
						h, _, _, err := cp.c.header(loc.offset)
						must(t, err)

						enc := h.DataPageHeader.V.Encoding
						if h.DataPageHeaderV2.Valid {
							enc = h.DataPageHeaderV2.V.Encoding
						}
						want := format.Plain
						if i == 0 {
							want = format.RLE
						}
						if enc != want {
							t.Fatalf("%s encoding %s, want %s", name, enc, want)
						}
					}
				}
				check := func(rows []Row) {
					t.Helper()
					if len(rows) != len(records) {
						t.Fatalf("%d rows", len(rows))
					}
					for i, row := range rows {
						w := records[i]
						if !bytes.Equal(row.Key, w.Key) || row.Values[0].Boolean() != w.RLE || row.Values[1].Boolean() != w.Plain || !bytes.Equal(row.Values[2].Bytes(), w.Fixed[:]) || !bytes.Equal(row.Values[3].Bytes(), w.Bytes) {
							t.Fatalf("row %d: %v", i, row.Values)
						}
					}
				}
				rows, err := d.Rows(ctx, Query{Limit: int64(len(records)), Columns: columns}, nil)
				must(t, err)
				entries, err := d.List(ctx, ListQuery{Limit: len(records), Columns: columns}, nil)
				must(t, err)
				listed := make([]Row, len(entries))
				for i, e := range entries {
					listed[i] = *e.Row
				}
				plan, err := d.Plan(ctx, []Selection{{}}, columns, nil)
				must(t, err)
				var scanned []Row
				must(t, scanRows(ctx, plan, nil, func(row Row) error { scanned = append(scanned, row); return nil }))
				check(rows)
				check(listed)
				check(scanned)
			})
		}
	}
}

func TestBufferReuseCodecRecovery(t *testing.T) {
	large := bytes.Repeat([]byte("0123456789abcdef"), 4096)
	small := bytes.Repeat([]byte("zyxw"), 300)
	for _, codec := range []compress.Codec{&gzip.Codec{}, &brotli.Codec{}, &zstd.Codec{}} {
		t.Run(codec.String(), func(t *testing.T) {
			var scratch pageScratch
			a, err := codec.Encode(nil, large)
			must(t, err)
			b, err := codec.Encode(nil, small)
			must(t, err)
			good := func(src, want []byte) {
				t.Helper()
				out, err := scratch.decompress(codec.CompressionCodec(), src, len(want))
				must(t, err)
				if !bytes.Equal(out, want) {
					t.Fatal("wrong output after reset")
				}
			}
			good(a, large)
			for _, bad := range [][]byte{a[:len(a)/2], bytes.Repeat([]byte{0xff}, 64)} {
				if _, err := scratch.decompress(codec.CompressionCodec(), bad, len(large)); err == nil {
					t.Fatal("corrupt stream accepted")
				}
				good(b, small)
			}
			if _, err := scratch.decompress(codec.CompressionCodec(), a, 100); err == nil {
				t.Fatal("surplus accepted")
			}
			good(b, small)
			if codec.CompressionCodec() == format.Gzip {
				if _, err := scratch.decompress(format.Gzip, append(bytes.Clone(a), b...), len(large)); err == nil {
					t.Fatal("concatenated surplus accepted")
				}
				good(b, small)
			}
			good(a, large)
		})
	}
}

// observeReads observes caller buffers after a real source read. Tests arm it
// only around a synchronous request, avoiding hooks in the production decoder.
type observeReads struct {
	Source
	after func([]byte, int64)
}
type observedFile struct {
	SourceFile
	owner *observeReads
}

func (s *observeReads) Open(ctx context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	if path == "manifest.json" {
		return f, nil
	}
	return observedFile{f, s}, nil
}
func (f observedFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.SourceFile.ReadAt(p, off)
	if f.owner.after != nil {
		f.owner.after(p, off)
	}
	return n, err
}

func TestBufferReuseMidScanAndRangeCancellation(t *testing.T) {
	records := transitionRecords(2048, true)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 128, DictionaryMaxBytes: 1024, Compression: parquet.Compression(&zstd.Codec{})}}})
	src := &observeReads{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	defer d.Close()
	plan, err := d.Plan(ctx, []Selection{{}}, transitionColumns, nil)
	must(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	var kept []Row
	err = scanRows(cancelled, plan, nil, func(row Row) error {
		kept = append(kept, row)
		if len(kept) == 700 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(kept) < 700 || len(kept) >= len(records) {
		t.Fatalf("mid-scan cancellation: %d rows, %v", len(kept), err)
	}
	checkTransitionRows(t, kept, records[:len(kept)])
	var fresh []Row
	must(t, scanRows(ctx, plan, nil, func(row Row) error { fresh = append(fresh, row); return nil }))
	checkTransitionRows(t, fresh, records)
	// Cancel during value projection in the second group, after scratch from
	// the first group has already been consumed and reused by this Range.
	first, err := d.r.parts[0].openColumn(ctx, 0, "dict", new(ReadStats), nil)
	must(t, err)
	second, err := d.r.parts[0].openColumn(ctx, 1, "dict", new(ReadStats), nil)
	must(t, err)
	seenFirst, triggered := false, false
	cancelled, cancelRange := context.WithCancel(ctx)
	defer cancelRange()
	src.after = func(_ []byte, off int64) {
		if off == first.locs[0].offset {
			seenFirst = true
		}
		if off == second.locs[1].offset {
			triggered = true
			cancelRange()
		}
	}
	_, err = d.Rows(cancelled, Query{Limit: int64(len(records)), Columns: transitionColumns}, nil)
	src.after = nil
	if !errors.Is(err, context.Canceled) || !triggered || !seenFirst {
		t.Fatalf("mid-range cancellation: first=%t trigger=%t error=%v", seenFirst, triggered, err)
	}
	fresh, err = d.Rows(ctx, Query{Limit: int64(len(records)), Columns: transitionColumns}, nil)
	must(t, err)
	checkTransitionRows(t, fresh, records)
	checkTransitionRows(t, kept, records[:len(kept)])
}

func TestBufferReuseRangeHighWaterCapacity(t *testing.T) {
	type record struct {
		Key   []byte `parquet:"key"`
		Value []byte `parquet:"value,plain"`
	}
	records := make([]record, 513)
	for i := range records {
		records[i] = record{fmt.Appendf(nil, "k%05d", i), bytes.Repeat([]byte{byte(i)}, 32)}
	}
	records[0].Value = bytes.Repeat([]byte{0x7f}, 128<<10)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 64, PageBytes: 128}}})
	src := &observeReads{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	defer d.Close()
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	offsets := map[int64]int64{}
	for g := range d.r.parts[0].groups {
		cp, err := d.r.parts[0].openColumn(ctx, g, "value", new(ReadStats), nil)
		must(t, err)
		for _, loc := range cp.locs {
			offsets[loc.offset] = loc.size
		}
	}
	var base *byte
	high, small := 0, 0
	src.after = func(buf []byte, off int64) {
		size, ok := offsets[off]
		if !ok {
			return
		}
		if int64(len(buf)) != size {
			t.Fatalf("expected indexed page extent, got %d/%d", len(buf), size)
		}
		if base == nil {
			base = &buf[0]
			high = cap(buf)
			if len(buf) < 128<<10 {
				t.Fatal("first page is not large")
			}
			return
		}
		if len(buf) >= high/2 {
			t.Fatal("subsequent page is not small")
		}
		if cap(buf) != high || &buf[0] != base {
			t.Fatalf("lost high-water allocation: %d -> %d", high, cap(buf))
		}
		small++
	}
	rows, err := d.Rows(ctx, Query{Limit: int64(len(records)), Columns: []string{"value"}}, nil)
	must(t, err)
	src.after = nil
	if small < 100 || len(rows) != len(records) {
		t.Fatalf("small pages=%d rows=%d", small, len(rows))
	}
	for i, row := range rows {
		if !bytes.Equal(row.Values[0].Bytes(), records[i].Value) {
			t.Fatal("corrupted value", i)
		}
	}
	t.Logf("Query reused a %d-byte raw allocation for %d smaller pages across %d groups", high, small, len(d.r.parts[0].groups))
}

// Whole Scan, including keys, headers, row ownership and all projected pages.
// The denominator is checked against the generated benchmark fixture's geometry.
// MemStats avoids changing GOMAXPROCS or the shared Zstd decoder's initialization.
func TestBufferReuseScanAllocationsPerPage(t *testing.T) {
	const n = 32768
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: reuseRecords(n), GroupRows: 4096, PageBytes: 4096, Compression: parquet.Compression(&zstd.Codec{})}}}), OpenOptions{})
	plan, err := d.Plan(ctx, []Selection{{}}, []string{"size", "etag", "class"}, nil)
	must(t, err)
	var st ReadStats
	count := 0
	emit := func(Row) error { count++; return nil }
	must(t, scanRows(ctx, plan, &st, emit))
	if st.KeyPagesDecoded != 384 || st.ValuePagesDecoded != 440 || count != n {
		t.Fatalf("fixture geometry: %+v, %d rows", st, count)
	}
	const runs = 3
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		count = 0
		must(t, scanRows(ctx, plan, &st, emit))
		if count != n {
			t.Fatal(count)
		}
	}
	runtime.ReadMemStats(&after)
	pages := float64(runs * (384 + 440))
	allocations := float64(after.Mallocs-before.Mallocs) / pages
	bytes := float64(after.TotalAlloc-before.TotalAlloc) / pages
	t.Logf("whole owning Scan: %.2f allocations/page, %.0f bytes/page", allocations, bytes)
	// Allow race-build pool churn. The slot gate below also rejects partial
	// buffer regressions that could fit within this whole-request budget.
	if allocations > 180 || bytes > 23<<10 {
		t.Fatalf("page allocation budget exceeded: %.2f allocations, %.0f bytes", allocations, bytes)
	}
}

// Gate each warmed projection slot separately: the whole owning scan's row
// copies otherwise hide a regression in just one page buffer. Uncompressed
// PLAIN avoids codec pools and their race-build eviction; headers still count.
func TestBufferReuseProjectionSlotAllocations(t *testing.T) {
	type record struct {
		Key      []byte  `parquet:"key"`
		Required string  `parquet:"required,plain"`
		Optional *string `parquet:"optional,optional,plain"`
	}
	rows := make([]record, 1024)
	for i := range rows {
		value := fmt.Sprintf("value-%08d", i)
		rows[i] = record{Key: fmt.Appendf(nil, "k%05d", i), Required: value}
		if i%7 != 0 {
			rows[i].Optional = &value
		}
	}
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256}}}), OpenOptions{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	for _, name := range []string{"required", "optional"} {
		var scratch pageScratch
		cp, err := d.r.parts[0].openColumn(ctx, 0, name, new(ReadStats), &scratch)
		must(t, err)
		if len(cp.locs) < 20 {
			t.Fatal("fixture needs many pages")
		}
		allocations := testing.AllocsPerRun(10, func() {
			cp.values = nil
			for _, loc := range cp.locs {
				if err := cp.load(loc.first); err != nil {
					panic(err)
				}
			}
		}) / float64(len(cp.locs))
		t.Logf("%s projection slot: %.3f allocations/page (%d pages)", name, allocations, len(cp.locs))
		budget := 6.5 // measured 6: includes the page header and decoded page wrapper
		if name == "optional" {
			budget = 7.5 // definition-level decoding adds one
		}
		if allocations > budget {
			t.Fatalf("%s slot lost buffer reuse: %.3f allocations/page > %.1f", name, allocations, budget)
		}
	}
}

// This end-to-end borrowed budget includes fresh per-call scratch (key pages
// included), headers, dictionaries and indexes. It guards against
// reintroducing row copies or per-page key storage; it is not the proposed
// <0.01 allocations/row acceptance target. Measured (Go 1.27.4,
// linux/amd64): 0.16 allocations and 24 bytes per row, 0.19 and 101 under
// -race; 0.24 and 113 before key pages were reused.
func TestScanBatchesAllocationsPerRow(t *testing.T) {
	const n = 32768
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: reuseRecords(n), GroupRows: 4096, PageBytes: 4096, Compression: parquet.Compression(&zstd.Codec{})}}}), OpenOptions{})
	plan, err := d.Plan(ctx, []Selection{{}}, []string{"size", "etag", "class"}, nil)
	must(t, err)
	var st ReadStats
	count := 0
	emit := func(b *Batch) error { count += b.Len(); return nil }
	must(t, plan.Scan(ctx, &st, emit))
	if count != n || st.KeyPagesDecoded != 384 || st.ValuePagesDecoded != 440 {
		t.Fatalf("fixture geometry: %d rows, %+v", count, st)
	}
	const runs = 3
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		count = 0
		must(t, plan.Scan(ctx, &st, emit))
		if count != n {
			t.Fatal(count)
		}
	}
	runtime.ReadMemStats(&after)
	allocations := float64(after.Mallocs-before.Mallocs) / float64(runs*n)
	bytes := float64(after.TotalAlloc-before.TotalAlloc) / float64(runs*n)
	t.Logf("whole borrowed ScanBatches: %.4f allocations/row, %.2f bytes/row", allocations, bytes)
	// Race builds discard pool entries; keep headroom for that measured churn.
	if allocations > 0.25 || bytes > 150 {
		t.Fatalf("borrowed allocation budget exceeded: %.4f allocations/row, %.2f bytes/row", allocations, bytes)
	}
}
