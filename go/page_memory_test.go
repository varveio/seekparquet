// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestPageMemoryDefault: the default PageMemory is the most memory a page
// within PageValues and PageBytes can be estimated to hold, so the default
// refuses no page those limits admit: 832 MiB at their defaults, and it
// follows them when they are set.
func TestPageMemoryDefault(t *testing.T) {
	if size := unsafe.Sizeof(parquet.Value{}); size != valueBytes {
		t.Fatalf("a parquet.Value is %d bytes, the estimate counts %d", size, valueBytes)
	}
	if got := (Limits{}).withDefaults().PageMemory; got != 832<<20 {
		t.Fatalf("default PageMemory %d, want %d", got, 832<<20)
	}
	if got := (Limits{PageValues: 1000, PageBytes: 1 << 20}).withDefaults().PageMemory; got != 48_000+1<<20 {
		t.Fatalf("PageMemory from lowered limits: %d", got)
	}
	if got := (Limits{PageMemory: 12345}).withDefaults().PageMemory; got != 12345 {
		t.Fatalf("an explicit PageMemory became %d", got)
	}
	if got := worstPageMemory(1<<62, 1<<20); got != maxInt64 {
		t.Fatalf("worstPageMemory overflowed to %d", got)
	}
}

// booleanRunPage returns a DATA_PAGE of n Booleans, all true, encoded as a
// single RLE run: a few bytes declaring a million values, within PageBytes
// and PageValues. Optional, every row is defined, by one run of levels.
func booleanRunPage(n int, optional bool) (*format.PageHeader, []byte) {
	run := binary.AppendUvarint(nil, uint64(n)<<1)
	var payload []byte
	if optional {
		levels := append(run[:len(run):len(run)], 1)
		payload = append(binary.LittleEndian.AppendUint32(nil, uint32(len(levels))), levels...)
	}
	values := append(run[:len(run):len(run)], 1)
	payload = append(payload, binary.LittleEndian.AppendUint32(nil, uint32(len(values)))...)
	payload = append(payload, values...)
	h := &format.PageHeader{Type: format.DataPage, UncompressedPageSize: int32(len(payload)), CompressedPageSize: int32(len(payload))}
	h.DataPageHeader = thrift.New(format.DataPageHeader{NumValues: int32(n), Encoding: format.RLE,
		DefinitionLevelEncoding: format.RLE, RepetitionLevelEncoding: format.RLE})
	return h, payload
}

// TestPageMemoryBoundsDecoding decodes a page of a million Booleans, a few
// bytes long: within PageValues and PageBytes, it decodes to 24 MB of
// parquet.Value (48 MB optional). Under a PageMemory of 16 MiB it fails
// with ErrLimitExceeded, naming PageMemory, before anything of that size is
// allocated; under the default it decodes. An optional column counts twice
// the Values of a required one.
func TestPageMemoryBoundsDecoding(t *testing.T) {
	const n = 1_000_000
	for _, optional := range []bool{false, true} {
		t.Run(fmt.Sprintf("optional=%v", optional), func(t *testing.T) {
			h, payload := booleanRunPage(n, optional)
			c := testChunk(parquet.BooleanType)
			c.col.optional = optional
			c.p.r.limits.PageMemory = 16 << 20
			var err error
			used := allocated(func() { _, err = c.decode(h, payload) })
			if !errors.Is(err, ErrLimitExceeded) || !strings.Contains(err.Error(), "PageMemory") {
				t.Fatalf("got %v, want a PageMemory limit", err)
			}
			if used > 64<<10 {
				t.Fatalf("the refused page allocated %d bytes", used)
			}
			c = testChunk(parquet.BooleanType)
			c.col.optional = optional
			var values []parquet.Value
			used = allocated(func() { values, err = c.decode(h, payload) })
			must(t, err)
			if len(values) != n || !values[0].Boolean() || !values[n-1].Boolean() {
				t.Fatalf("%d values", len(values))
			}
			if used < n*valueBytes {
				t.Fatalf("the page decoded in %d bytes: the test does not reach the bound", used)
			}
		})
	}
	h, payload := booleanRunPage(n, false)
	required := testChunk(parquet.BooleanType).pageMemory(h, false)
	optional := testChunk(parquet.BooleanType).pageMemory(h, true)
	decoded := int64(n) // a byte per Boolean, at most
	if required != n*valueBytes+decoded || optional != 2*n*valueBytes+decoded || int64(len(payload)) > 64 {
		t.Fatalf("estimates %d and %d for a %d-byte page", required, optional, len(payload))
	}
}

// memoryRow has a key, a dictionary-encoded string with many distinct
// values, and required and optional numbers, in small pages.
type memoryRow struct {
	Key  []byte `parquet:"key"`
	Name string `parquet:"name,dict"`
	Req  int64  `parquet:"req"`
	Opt  *int64 `parquet:"opt,optional"`
}

// TestPageMemoryOnEveryPage reads a dataset whose dictionary, key pages
// and value pages each fit within PageValues and PageBytes, under
// PageMemory limits that admit some kinds of page and not others: each
// kind is bounded (a dictionary page, key pages, required and optional
// value pages), the read fails with the limit and not a refusal, and the
// same pages read under a sufficient limit.
func TestPageMemoryOnEveryPage(t *testing.T) {
	rows := make([]memoryRow, 3000)
	for i := range rows {
		v := int64(i)
		rows[i] = memoryRow{Key: []byte(fmt.Sprintf("k%05d", i)), Name: fmt.Sprintf("name-%05d", i), Req: v, Opt: &v}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 1024}}})
	// The largest estimate of each kind of page, from the part's headers.
	memory := pageMemoryByKind(t, dir)
	t.Logf("largest estimates: %v", memory)
	// Each case isolates one kind of page: the dictionary holds the most,
	// key pages (read by every case) less than either value column's pages.
	if memory["dictionary"] <= max(memory["key"], memory["nameData"], memory["opt"], memory["req"]) ||
		memory["key"] >= min(memory["opt"], memory["req"]) {
		t.Fatalf("the fixture's pages do not isolate each kind: %v", memory)
	}
	read := func(limit int64, columns ...string) error {
		d := open(t, dir, Options{Limits: Limits{PageMemory: limit}})
		got, err := d.Range(ctx, Range{Limit: 5000, Columns: columns})
		if err == nil && (len(got) != len(rows) || len(columns) > 0 && got[2999].Values[0].IsNull()) {
			return fmt.Errorf("%d rows", len(got))
		}
		return err
	}
	isLimit := func(err error) bool {
		return errors.Is(err, ErrLimitExceeded) && strings.Contains(err.Error(), "PageMemory")
	}
	cases := []struct {
		name    string
		limit   int64
		columns []string
		refused bool
	}{
		{"dictionary over", memory["dictionary"] - 1, []string{"name"}, true},
		{"dictionary within", memory["dictionary"], []string{"name"}, false},
		{"key pages over", memory["key"] - 1, nil, true},
		{"key pages within", memory["key"], nil, false},
		{"optional over", memory["opt"] - 1, []string{"opt"}, true},
		{"optional within", max(memory["opt"], memory["key"]), []string{"opt"}, false},
		{"required over", memory["req"] - 1, []string{"req"}, true},
		{"required within", max(memory["req"], memory["key"]), []string{"req"}, false},
		{"default", 0, []string{"name", "req", "opt"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := read(c.limit, c.columns...)
			if c.refused != isLimit(err) || !c.refused && err != nil {
				t.Fatalf("PageMemory %d, columns %v: %v", c.limit, c.columns, err)
			}
		})
	}
	// The validator decodes under the same bound: its full level is
	// incomplete, not failed, when a page is over it.
	report, err := ValidateDir(ctx, dir, ValidateOptions{Limits: Limits{PageMemory: memory["dictionary"] - 1}})
	must(t, err)
	if full, _ := report.Level(LevelFull); full.Result != Incomplete || !errors.Is(full.Err, ErrLimitExceeded) {
		t.Fatalf("validation: %+v", full)
	}
}

// pageMemoryByKind returns the largest page memory estimate of the part's
// dictionary pages, key pages, and required and optional value pages.
func pageMemoryByKind(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	d := open(t, dir, Options{})
	p, err := d.r.part(ctx, 0, new(ReadStats))
	must(t, err)
	out := map[string]int64{}
	for physical := range p.footer.Metadata().RowGroups {
		for _, name := range []string{"key", "name", "req", "opt"} {
			col := p.leaves[name]
			c, err := p.openChunk(ctx, physical, col, new(ReadStats), new(int64))
			must(t, err)
			for off := c.start; off < c.end; {
				h, n, _, err := c.header(off)
				must(t, err)
				kind := name
				if h.Type == format.DictionaryPage {
					kind = "dictionary"
				} else if name == "name" {
					kind = "nameData"
				}
				out[kind] = max(out[kind], c.pageMemory(h, col.optional && h.Type != format.DictionaryPage))
				off += n + int64(h.CompressedPageSize)
			}
		}
	}
	return out
}
