// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// extentSource records every read of a part file, so that a test can
// attribute the bytes a request asked for to the column chunks they fall in.
type extentSource struct {
	Source
	mu    sync.Mutex
	reads []extentRead
}

type extentRead struct {
	path     string
	off, len int64
}

type extentFile struct {
	SourceFile
	path  string
	owner *extentSource
}

func (s *extentSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return extentFile{f, path, s}, nil
}

func (f extentFile) ReadAt(p []byte, off int64) (int, error) {
	f.owner.mu.Lock()
	f.owner.reads = append(f.owner.reads, extentRead{f.path, off, int64(len(p))})
	f.owner.mu.Unlock()
	return f.SourceFile.ReadAt(p, off)
}

func (s *extentSource) take() []extentRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	reads := s.reads
	s.reads = nil
	return reads
}

// oracleRow is one row of a dataset as parquet-go decodes it, whole and in
// file order, independently of this package's page reader.
type oracleRow struct {
	key    []byte
	offset int64
	value  parquet.Value // the summed column's value; null when absent
}

// chunkExtent is one column chunk's bytes [start, end) in a part file.
type chunkExtent struct {
	path       string
	start, end int64
}

// oracle decodes every row of the dataset at dir through parquet-go, with
// the values of column, and returns the byte extents of column's chunks.
func oracle(t *testing.T, dir string, meta Metadata, column string) ([]oracleRow, []chunkExtent) {
	t.Helper()
	var rows []oracleRow
	var extents []chunkExtent
	for _, part := range meta.Parts {
		raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(part.Path)))
		must(t, err)
		f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
		must(t, err)
		keyLeaf, ok := f.Schema().Lookup(meta.Key.Column)
		if !ok {
			t.Fatalf("%s: no key column", part.Path)
		}
		valueLeaf, hasValue := f.Schema().Lookup(column)
		for g, rg := range f.RowGroups() {
			keys := columnValues(t, rg.ColumnChunks()[keyLeaf.ColumnIndex])
			var values []parquet.Value
			if hasValue {
				values = columnValues(t, rg.ColumnChunks()[valueLeaf.ColumnIndex])
				m := f.Metadata().RowGroups[g].Columns[valueLeaf.ColumnIndex].MetaData
				start := m.DataPageOffset
				if m.DictionaryPageOffset != 0 {
					start = m.DictionaryPageOffset
				}
				extents = append(extents, chunkExtent{part.Path, start, start + m.TotalCompressedSize})
			}
			for i, k := range keys {
				row := oracleRow{key: bytes.Clone(k.ByteArray())}
				if n := len(rows); n > 0 && bytes.Equal(rows[n-1].key, row.key) {
					row.offset = rows[n-1].offset + 1
				}
				if hasValue {
					row.value = values[i]
				}
				rows = append(rows, row)
			}
		}
	}
	return rows, extents
}

func columnValues(t *testing.T, chunk parquet.ColumnChunk) []parquet.Value {
	t.Helper()
	pages := chunk.Pages()
	defer pages.Close()
	var out []parquet.Value
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			return out
		}
		must(t, err)
		values := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		for _, v := range values[:n] {
			out = append(out, v.Clone())
		}
	}
}

// selects reports whether rng's bounds select key, by the definition.
func selects(rng Span, key []byte) bool {
	if !bytes.HasPrefix(key, rng.Prefix) {
		return false
	}
	if rng.Start != nil {
		if c := bytes.Compare(key, rng.Start); c < 0 || c == 0 && rng.StartExclusive {
			return false
		}
	}
	return rng.End == nil || bytes.Compare(key, rng.End) < 0
}

// planRanges returns ranges over keys: every pair of a sample of bounds,
// inclusive and exclusive, bounds between and above keys, an empty End,
// prefixes of several lengths and lookups of single keys.
func planRanges(keys [][]byte) []Selection {
	var distinct [][]byte
	for _, k := range keys {
		if len(distinct) == 0 || !bytes.Equal(distinct[len(distinct)-1], k) {
			distinct = append(distinct, k)
		}
	}
	bounds := [][]byte{nil}
	step := max(1, len(distinct)/9)
	for i := 0; i < len(distinct); i += step {
		k := distinct[i]
		bounds = append(bounds, k, append(bytes.Clone(k), 0), k[:(len(k)+1)/2])
	}
	if n := len(distinct); n > 0 {
		bounds = append(bounds, distinct[n-1], append(bytes.Clone(distinct[n-1]), 0xff))
	}
	var out []Selection
	for _, a := range bounds {
		for _, b := range bounds {
			out = append(out, Selection{Span: Span{Start: a, End: b}}, Selection{Span: Span{Start: a, StartExclusive: a != nil, End: b}})
		}
	}
	out = append(out, Selection{Span: Span{End: []byte{}}})
	for i := 0; i < len(distinct); i += step {
		k := distinct[i]
		for _, n := range []int{1, (len(k) + 1) / 2, len(k)} {
			out = append(out, Selection{Span: PrefixSpan(k[:n])}, Selection{Span: Span{Prefix: k[:n], Start: k, End: append(bytes.Clone(k), 1)}})
		}
		out = append(out, Selection{Span: KeySpan(k)})
	}
	return out
}

// checkPlan plans ranges over d and checks the plan against rows, the
// oracle: its count, every row Scan yields with its offset and value, the
// sum of the column's values, and that the bytes Scan asked for in the
// column's chunks are exactly ProjectedBytes, each within one chunk.
func checkPlan(t *testing.T, d *Dataset, src *extentSource, rows []oracleRow, extents []chunkExtent, column string, ranges []Selection) {
	t.Helper()
	name := fmt.Sprintf("%s over %s", column, describe(ranges))
	var want []oracleRow
	var wantSum int64
	for _, row := range rows {
		if slices.ContainsFunc(ranges, func(rng Selection) bool { return selects(rng.Span, row.key) }) {
			want = append(want, row)
			if !row.value.IsNull() {
				wantSum += row.value.Int64()
			}
		}
	}
	columns := []string{column}
	src.take()
	var planned ReadStats
	plan, err := d.Plan(ctx, ranges, columns, &planned)
	if err != nil {
		t.Fatalf("%s: plan: %v", name, err)
	}
	if n, exact := plan.Count(); n != int64(len(want)) || !exact {
		t.Fatalf("%s: count %d (exact %v), want %d", name, n, exact, len(want))
	}
	if st := planned; st.ValuePagesDecoded != 0 || chunkBytes(t, name, src.take(), extents) != 0 {
		t.Fatalf("%s: planning read value pages", name)
	}
	var got []Row
	var st ReadStats
	must(t, scanRows(ctx, plan, &st, func(row Row) error {
		got = append(got, row)
		return nil
	}))
	reads := src.take()
	if len(got) != len(want) {
		t.Fatalf("%s: scanned %d rows, want %d", name, len(got), len(want))
	}
	var sum int64
	for i, row := range got {
		w := want[i]
		v := row.Values[0]
		none := v.IsNull() || v.Absent() // the oracle's value is null when absent
		if !bytes.Equal(row.Key, w.key) || row.Offset != w.offset || none != w.value.IsNull() {
			t.Fatalf("%s: row %d is %q/%d (null %v), want %q/%d (null %v)", name, i, row.Key, row.Offset, none, w.key, w.offset, w.value.IsNull())
		}
		if !none {
			sum += v.Int64()
		}
	}
	if sum != wantSum {
		t.Fatalf("%s: sum %d, want %d", name, sum, wantSum)
	}
	served := chunkBytes(t, name, reads, extents)
	if n, exact := plan.ProjectedBytes(); exact && n != served {
		t.Fatalf("%s: projected %d bytes, Scan asked for %d", name, n, served)
	} else if !exact && n != 0 {
		t.Fatalf("%s: projected %d bytes of a chunk without an OffsetIndex", name, n)
	}
}

// TestPlanConformanceOracle plans ranges over every valid conformance
// dataset and checks counts, rows, sums and projected bytes against a full
// decode by parquet-go.
func TestPlanConformanceOracle(t *testing.T) {
	dirs, err := filepath.Glob("../conformance/datasets/*/STRUCTURE.txt")
	must(t, err)
	for _, structure := range dirs {
		raw, err := os.ReadFile(structure)
		must(t, err)
		if !strings.Contains(string(raw), "\nvalid: yes\n") {
			continue
		}
		dir := filepath.Dir(structure)
		t.Run(filepath.Base(dir), func(t *testing.T) {
			src := &extentSource{Source: dirOf(t, dir)}
			d, err := OpenDataset(ctx, src, OpenOptions{})
			must(t, err)
			defer d.Close()
			column := "size"
			if strings.Contains(string(raw), "column i64: INT64") {
				column = "i64"
			}
			rows, extents := oracle(t, dir, d.Metadata(), column)
			keys := make([][]byte, len(rows))
			for i, row := range rows {
				keys[i] = row.key
			}
			ranges := planRanges(keys)
			for _, rng := range ranges {
				checkPlan(t, d, src, rows, extents, column, []Selection{rng})
			}
			random := rand.New(rand.NewPCG(1, uint64(len(rows))))
			for range 40 {
				n := 2 + random.IntN(3)
				var set []Selection
				for range n {
					set = append(set, ranges[random.IntN(len(ranges))])
				}
				checkPlan(t, d, src, rows, extents, column, set)
			}
		})
	}
}

// chunkBytes returns the bytes of the reads that begin in one of extents,
// requiring each to end in it too.
func chunkBytes(t *testing.T, name string, reads []extentRead, extents []chunkExtent) int64 {
	t.Helper()
	var n int64
	for _, read := range reads {
		for _, e := range extents {
			if read.path == e.path && read.off >= e.start && read.off < e.end {
				if read.off+read.len > e.end {
					t.Fatalf("%s: a read of %d bytes at %d leaves the chunk [%d, %d)", name, read.len, read.off, e.start, e.end)
				}
				n += read.len
			}
		}
	}
	return n
}

// openOracle opens the dataset at dir through an extentSource and decodes
// it with parquet-go.
func openOracle(t *testing.T, dir, column string) (*Dataset, *extentSource, []oracleRow, []chunkExtent) {
	t.Helper()
	src := &extentSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	rows, extents := oracle(t, dir, d.Metadata(), column)
	return d, src, rows, extents
}

// TestPlanSharedDictionary plans two lookups in one dictionary-encoded row
// group (the encodings dataset's part 0, row group 0, i64 pages of three
// rows): together they cost the dictionary once, and two lookups in one
// page cost that page once.
func TestPlanSharedDictionary(t *testing.T) {
	dir := "../conformance/datasets/encodings"
	d, src, rows, extents := openOracle(t, dir, "i64")
	footer, err := testutil.ReadFooter(filepath.Join(dir, "data/part-00000.parquet"))
	must(t, err)
	meta := footer.RowGroups[0].Columns[2].MetaData
	if footer.Schema[3].Name != "i64" || meta.DictionaryPageOffset == 0 {
		t.Fatal("the fixture's i64 chunk has no dictionary")
	}
	dict := meta.DataPageOffset - meta.DictionaryPageOffset
	bytesOf := func(keys ...string) int64 {
		var ranges []Selection
		for _, k := range keys {
			ranges = append(ranges, Selection{Span: KeySpan([]byte(k))})
		}
		checkPlan(t, d, src, rows, extents, "i64", ranges)
		plan, err := d.Plan(ctx, ranges, []string{"i64"}, nil)
		must(t, err)
		n, exact := plan.ProjectedBytes()
		if !exact {
			t.Fatal("not exact")
		}
		return n
	}
	a, b, both := bytesOf("enc/000"), bytesOf("enc/004"), bytesOf("enc/000", "enc/004")
	if a+b-both != dict || both <= dict {
		t.Fatalf("pages 0 and 1 cost %d and %d, together %d: the dictionary of %d is not shared", a, b, both, dict)
	}
	if same := bytesOf("enc/000", "enc/001", "enc/002"); same != a {
		t.Fatalf("three rows of page 0 cost %d, one %d", same, a)
	}
}

// TestPlanBoundaries checks plans whose ends fall at every key of datasets
// of three parts with several row groups and pages, so that an end falls
// inside a page, at a page's first key, at a row group's and at a part's,
// with exact and with truncated key page bounds; and empty ranges.
func TestPlanBoundaries(t *testing.T) {
	keys := fixtureKeys(240)
	for _, geometry := range []testutil.Part{
		{GroupRows: 40, PageBytes: 256},
		{GroupRows: 40, PageBytes: 256, IndexSizeLimit: 2},
	} {
		t.Run(fmt.Sprintf("bounds%d", geometry.IndexSizeLimit), func(t *testing.T) {
			dir := write(t, parts(geometry, split(keys, 3)...))
			d, src, rows, extents := openOracle(t, dir, "size")
			must(t, d.Verify(ctx))
			pages := 0
			for _, p := range d.r.parts {
				for _, grp := range p.groups {
					index, err := p.keyIndex(ctx, grp.physical, new(ReadStats))
					must(t, err)
					pages += len(index.pages)
				}
			}
			if groups := len(keys) / 40; pages < 2*groups || pages > len(keys)/2 {
				t.Fatalf("%d key pages in %d row groups: the fixture has no pages of several rows inside its groups", pages, groups)
			}
			last := []byte(keys[len(keys)-1])
			for i, k := range keys {
				key := []byte(k)
				for _, rng := range []Selection{{Span: Span{End: key}}, {Span: Span{Start: key}}, {Span: Span{Start: key, StartExclusive: true}}, Selection{Span: KeySpan(key)}, {Span: Span{Start: key, End: []byte(keys[min(i+7, len(keys)-1)])}}} {
					checkPlan(t, d, src, rows, extents, "size", []Selection{rng})
				}
			}
			for _, rng := range []Selection{{Span: Span{Start: []byte(keys[9]), End: []byte(keys[9])}}, {Span: Span{Start: []byte(keys[90]), End: []byte(keys[10])}}, Selection{Span: PrefixSpan([]byte("nothing"))}, {Span: Span{Start: last, StartExclusive: true}}, {Span: Span{Start: append(bytes.Clone(last), 0)}}} {
				plan, err := d.Plan(ctx, []Selection{rng}, []string{"size"}, nil)
				must(t, err)
				if n, exact := plan.Count(); n != 0 || !exact {
					t.Fatalf("%s: count %d (exact %v)", describe(rng), n, exact)
				}
				if n, _ := plan.ProjectedBytes(); n != 0 {
					t.Fatalf("%s: %d bytes", describe(rng), n)
				}
				checkPlan(t, d, src, rows, extents, "size", []Selection{rng})
			}
		})
	}
}

// TestPlanCountsUseOnlyCheckedParts: a count across a part no read has
// checked relies on its manifest row count and is not exact; planning
// without columns opens only the parts at the ends, and not a part the
// manifest bounds place after the range; with columns it checks the parts
// between, and the count is exact.
func TestPlanCountsUseOnlyCheckedParts(t *testing.T) {
	keys := fixtureKeys(300)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 25, PageBytes: 256}, split(keys, 3)...)), OpenOptions{})
	plan := func(rng Query, columns ...string) (int64, bool, ReadStats) {
		t.Helper()
		var st ReadStats
		p, err := d.Plan(ctx, []Selection{rng.Selection}, columns, &st)
		must(t, err)
		n, exact := p.Count()
		return n, exact, st
	}
	span := Query{Selection: Selection{Span: Span{Start: []byte(keys[50]), End: []byte(keys[250])}}}
	if n, exact, st := plan(span); n != 200 || exact || st.PartsOpened != 2 || st.ValuePagesDecoded != 0 {
		t.Fatalf("across an unchecked part: %d (exact %v), %+v", n, exact, st)
	}
	if n, exact, st := plan(Query{Selection: Selection{Span: Span{Start: []byte(keys[10]), End: []byte(keys[200])}}}); n != 190 || exact || st.PartsOpened != 0 {
		t.Fatalf("up to part 2's first key: %d (exact %v), %+v", n, exact, st)
	}
	if n, exact, _ := plan(Query{Selection: Selection{Span: Span{Start: []byte(keys[210]), End: []byte(keys[290])}}}); n != 80 || !exact {
		t.Fatalf("within a part: %d (exact %v)", n, exact)
	}
	if n, exact, st := plan(span, "size"); n != 200 || !exact || st.PartsOpened != 1 || st.ValuePagesDecoded != 0 {
		t.Fatalf("with a column: %d (exact %v), %+v", n, exact, st)
	}
	if n, exact, st := plan(span); n != 200 || !exact || st.PartsOpened != 0 {
		t.Fatalf("once checked: %d (exact %v), %+v", n, exact, st)
	}
}

// TestPlanRequests: what a plan refuses, a cursor, a scan ended by its
// callback, and a plan of a closed dataset.
func TestPlanRequests(t *testing.T) {
	keys := fixtureKeys(60)
	d, err := OpenDir(ctx, write(t, parts(testutil.Part{GroupRows: 20}, split(keys, 2)...)), OpenOptions{})
	must(t, err)
	for _, rng := range []Selection{{Cursor: &Cursor{Prefix: []byte("a")}}, {Cursor: &Cursor{Key: []byte("a"), Offset: -1}}, {Cursor: &Cursor{}}} {
		if _, err := d.Plan(ctx, []Selection{rng}, nil, nil); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: %v", describe(rng), err)
		}
	}
	all, err := d.Rows(ctx, Query{Limit: 100}, nil)
	must(t, err)
	plan, err := d.Plan(ctx, []Selection{{Cursor: all[29].Cursor()}}, []string{"name"}, nil)
	must(t, err)
	var got []string
	stop := errors.New("stop")
	err = scanRows(ctx, plan, nil, func(row Row) error {
		got = append(got, string(row.Key))
		if v, _ := row.Value("name"); len(got) == 5 && !v.IsNull() && !strings.HasPrefix(string(v.Bytes()), "name-") {
			t.Errorf("value %v", v)
		}
		if len(got) == 20 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || !slices.Equal(got, keysOf(all[30:50])) {
		t.Fatalf("after a cursor: %q, %v", got, err)
	}
	if n, exact := plan.Count(); n != 30 || !exact {
		t.Fatalf("count after a cursor: %d %v", n, exact)
	}
	must(t, d.Close())
	if err := scanRows(ctx, plan, nil, func(Row) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("scan after close: %v", err)
	}
	if _, err := d.Plan(ctx, nil, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("plan after close: %v", err)
	}
	typed := open(t, write(t, typedDataset()), OpenOptions{})
	var unsupported *UnsupportedError
	if _, err := typed.Plan(ctx, []Selection{{Span: Span{Start: []byte("t")}}}, []string{"tags"}, nil); !errors.As(err, &unsupported) {
		t.Fatalf("nested projection: %v", err)
	}
}

// TestPlanConcurrent plans and scans two plans on one dataset from several
// goroutines at once, each scan agreeing with a sequential one.
func TestPlanConcurrent(t *testing.T) {
	keys := fixtureKeys(400)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 30, PageBytes: 256}, split(keys, 4)...)), OpenOptions{})
	ranges := [][]Selection{{Selection{Span: PrefixSpan([]byte("d0"))}, {Span: Span{Start: []byte(keys[300]), End: []byte(keys[350])}}}, {{Span: Span{Start: []byte(keys[20]), End: []byte(keys[380])}}, Selection{Span: KeySpan([]byte(keys[5]))}}}
	scan := func(plan *Plan) (string, error) {
		var b strings.Builder
		err := scanRows(ctx, plan, nil, func(row Row) error {
			fmt.Fprintf(&b, "%q=%v;", row.Key, row.Values[0].Int64())
			return nil
		})
		return b.String(), err
	}
	var wg sync.WaitGroup
	results := make([][]string, len(ranges))
	errs := make(chan error, 16)
	for i, set := range ranges {
		results[i] = make([]string, 4)
		for j := range 4 {
			wg.Go(func() {
				plan, err := d.Plan(ctx, set, []string{"size"}, nil)
				if err != nil {
					errs <- err
					return
				}
				if results[i][j], err = scan(plan); err != nil {
					errs <- err
				}
			})
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for i, set := range ranges {
		plan, err := d.Plan(ctx, set, []string{"size"}, nil)
		must(t, err)
		want, err := scan(plan)
		must(t, err)
		for _, got := range results[i] {
			if got != want {
				t.Fatalf("plan %d: a concurrent scan differs", i)
			}
		}
	}
}

// fallbackRow has a dictionary-encoded value column.
type fallbackRow struct {
	Key  []byte `parquet:"key"`
	Size int64  `parquet:"size,dict"`
}

// TestRangeReadsDictionaryOnlyForItsPages: a chunk whose dictionary
// outgrew its limit continues in PLAIN pages; a Rows projecting only a
// PLAIN page reads no byte of the dictionary, and succeeds under a
// Limits.PageBytes the dictionary page exceeds, while a plan's Scan, which
// counts the dictionary, reads it and so meets the limit.
func TestRangeReadsDictionaryOnlyForItsPages(t *testing.T) {
	rows := make([]fallbackRow, 700)
	for i := range rows {
		rows[i] = fallbackRow{Key: fmt.Appendf(nil, "k%04d", i), Size: int64(i) * 1000}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256, DictionaryMaxBytes: 2048}}})
	last := rows[len(rows)-1].Key
	footer, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	meta := footer.RowGroups[0].Columns[1].MetaData
	if meta.DictionaryPageOffset == 0 {
		t.Fatal("the size chunk has no dictionary")
	}
	dict := chunkExtent{testutil.PartPath(0), meta.DictionaryPageOffset, meta.DataPageOffset}
	whole := chunkExtent{testutil.PartPath(0), meta.DictionaryPageOffset, meta.DictionaryPageOffset + meta.TotalCompressedSize}

	limits := Limits{PageBytes: 1024}
	src := &extentSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, OpenOptions{Limits: limits})
	must(t, err)
	defer d.Close()
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	cp, err := d.r.parts[0].openColumn(ctx, 0, "size", new(ReadStats), nil)
	must(t, err)
	if cp.dictionaryBytes() <= limits.PageBytes {
		t.Fatalf("the dictionary takes %d bytes, within Limits.PageBytes", cp.dictionaryBytes())
	}
	for i, want := range map[int]bool{0: true, len(cp.locs) - 1: false} {
		h, _, _, err := cp.c.header(cp.locs[i].offset)
		must(t, err)
		enc := h.DataPageHeader.V.Encoding
		if h.DataPageHeaderV2.Valid {
			enc = h.DataPageHeaderV2.V.Encoding
		}
		if (enc == parquet.RLEDictionary.Encoding() || enc == parquet.PlainDictionary.Encoding()) != want {
			t.Fatalf("page %d is encoded %s", i, enc)
		}
	}

	src.take()
	got, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan(last)}, Limit: 1, Columns: []string{"size"}}, nil)
	must(t, err)
	if len(got) != 1 || got[0].Values[0].Int64() != rows[len(rows)-1].Size {
		t.Fatalf("range: %+v", got)
	}
	reads := src.take()
	if n := chunkBytes(t, "range", reads, []chunkExtent{dict}); n != 0 {
		t.Fatalf("a Query of a PLAIN page read %d bytes of the dictionary", n)
	}
	if n := chunkBytes(t, "range", reads, []chunkExtent{whole}); n != cp.locs[len(cp.locs)-1].size {
		t.Fatalf("a Query of one page read %d bytes of the chunk, the page is %d", n, cp.locs[len(cp.locs)-1].size)
	}
	plan, err := d.Plan(ctx, []Selection{Selection{Span: KeySpan(last)}}, []string{"size"}, nil)
	must(t, err)
	if n, _ := plan.ProjectedBytes(); n != cp.dictionaryBytes()+cp.locs[len(cp.locs)-1].size {
		t.Fatalf("projected %d bytes", n)
	}
	if err := scanRows(ctx, plan, nil, func(Row) error { return nil }); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("a planned scan of a dictionary over the limit: %v", err)
	}
}

// TestPlanCursorInEqualKeys plans a range continuing after a cursor deep
// inside a group of equal keys that spans several pages, with ranges
// overlapping it and each other, and checks the rows and offsets Scan
// yields, and the count, against the rows a full Rows returns.
func TestPlanCursorInEqualKeys(t *testing.T) {
	stem := strings.Repeat("x", 40)
	var keys []string
	keys = append(keys, "a"+stem, "a"+stem)
	for range 60 {
		keys = append(keys, "b"+stem)
	}
	keys = append(keys, "c"+stem, "c"+stem, "c"+stem, "d"+stem)
	dir := write(t, testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: testutil.Rows(keys...), PageBytes: 128, GroupRows: 25}}})
	d := open(t, dir, OpenOptions{})
	all, err := d.Rows(ctx, Query{Limit: 1000, Columns: []string{"size"}}, nil)
	must(t, err)
	index, err := d.r.parts[0].keyIndex(ctx, d.r.parts[0].groups[1].physical, new(ReadStats))
	must(t, err)
	if len(index.pages) < 3 {
		t.Fatalf("row group 1 has %d key pages: the group of b is not across pages", len(index.pages))
	}
	b, c, end := []byte("b"+stem), []byte("c"+stem), []byte("d")
	for _, ranges := range [][]Selection{{{Span: Span{Start: b, End: end}, Cursor: &Cursor{Key: b, Offset: 37}}, {Span: Span{Start: b}, Cursor: &Cursor{Key: b, Offset: 50}}}, {{Span: Span{Start: b}, Cursor: &Cursor{Key: b, Offset: 41}}, {Span: Span{Start: c, End: end}}, Selection{Span: KeySpan(b)}}, {{Span: Span{Start: b, End: c}, Cursor: &Cursor{Key: b, Offset: 59}}, {Span: Span{Prefix: []byte("c")}}}, {{Span: Span{Start: b, End: end}, Cursor: &Cursor{Key: b, Offset: 12}}, {Span: Span{Prefix: []byte("b")}, Cursor: &Cursor{Key: b, Offset: 30}}}} {
		var want []string
		for _, row := range all {
			if slices.ContainsFunc(ranges, func(rng Selection) bool {
				return selects(rng.Span, row.Key) && afterCursor(rng.Cursor, row.Key, row.Offset)
			}) {
				want = append(want, fmt.Sprintf("%s/%d=%d", row.Key, row.Offset, row.Values[0].Int64()))
			}
		}
		plan, err := d.Plan(ctx, ranges, []string{"size"}, nil)
		must(t, err)
		var got []string
		must(t, scanRows(ctx, plan, nil, func(row Row) error {
			got = append(got, fmt.Sprintf("%s/%d=%d", row.Key, row.Offset, row.Values[0].Int64()))
			return nil
		}))
		if n, exact := plan.Count(); !slices.Equal(got, want) || n != int64(len(want)) || !exact {
			t.Fatalf("%s: count %d (exact %v)\n got %q\nwant %q", describe(ranges), n, exact, got, want)
		}
	}
}
