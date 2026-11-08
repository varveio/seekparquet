// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// fixturePages is what a conformance fixture's STRUCTURE.txt says about
// one part file's key pages: per row group with rows, its pages' first
// row indexes and bound lengths.
type fixturePages map[int]struct{ first, minLen, maxLen []int64 }

func readStructure(t *testing.T, path string) map[string]fixturePages {
	t.Helper()
	f, err := os.Open(path)
	must(t, err)
	defer f.Close()
	ints := func(s string) []int64 {
		var out []int64
		for _, f := range strings.Split(strings.Trim(s, "[]"), ",") {
			n, err := strconv.ParseInt(strings.TrimSpace(f), 10, 64)
			must(t, err)
			out = append(out, n)
		}
		return out
	}
	var (
		fileRe  = regexp.MustCompile(`^file (\S+): `)
		groupRe = regexp.MustCompile(`^\s+row_group (\d+): rows (\d+)`)
		listRe  = regexp.MustCompile(`^\s+(offset index: \d+ pages, first_row_index|min lengths|max lengths) (\[.*\])$`)
	)
	out := map[string]fixturePages{}
	var file string
	group := -1
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if m := fileRe.FindStringSubmatch(line); m != nil {
			file, group = m[1], -1
			out[file] = fixturePages{}
		} else if m := groupRe.FindStringSubmatch(line); m != nil && file != "" {
			group, _ = strconv.Atoi(m[1])
		} else if m := listRe.FindStringSubmatch(line); m != nil && group >= 0 {
			// A row group's first chunk block is its key column's
			// (named-key also prints a second one).
			e := out[file][group]
			switch {
			case strings.HasPrefix(m[1], "offset") && e.first == nil:
				e.first = ints(m[2])
			case strings.HasPrefix(m[1], "min") && e.minLen == nil:
				e.minLen = ints(m[2])
			case strings.HasPrefix(m[1], "max") && e.maxLen == nil:
				e.maxLen = ints(m[2])
			}
			out[file][group] = e
		}
	}
	must(t, s.Err())
	return out
}

// TestInspectConformanceFixtures: every part of every valid conformance
// fixture inspects as its manifest and STRUCTURE.txt describe it: row
// count, stamps, and each row group's key pages with their first rows and
// bound lengths, bounds that enclose the part's manifest bounds.
func TestInspectConformanceFixtures(t *testing.T) {
	dirs, err := filepath.Glob("../conformance/datasets/*/manifest.json")
	must(t, err)
	if len(dirs) < 15 {
		t.Fatalf("found %d fixtures", len(dirs))
	}
	for _, path := range dirs {
		dir := filepath.Dir(path)
		raw, err := os.ReadFile(path)
		must(t, err)
		m, err := ParseManifest(raw, Limits{})
		must(t, err)
		structure := readStructure(t, filepath.Join(dir, "STRUCTURE.txt"))
		for _, p := range m.Parts {
			name := filepath.Base(dir) + "/" + p.Path
			f, err := os.Open(filepath.Join(dir, filepath.FromSlash(p.Path)))
			must(t, err)
			ft, err := InspectFooter(f, p.Bytes)
			if err != nil {
				f.Close()
				t.Fatalf("%s: %v", name, err)
			}
			if ft.Rows != p.Rows || ft.Key != m.Key || ft.SchemaID != m.SchemaID || ft.Version != m.Version || len(ft.Columns) == 0 {
				t.Errorf("%s: footer %+v, manifest key %+v rows %d", name, ft, m.Key, p.Rows)
			}
			pages, err := InspectKeyPages(f, p.Bytes, m.Key.Column)
			f.Close()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			want, ok := structure[p.Path]
			if !ok {
				t.Fatalf("%s: not in STRUCTURE.txt", name)
			}
			got := fixturePages{}
			var rows int64
			for i, page := range pages {
				if page.FirstRow != rows || page.Rows <= 0 {
					t.Fatalf("%s: page %d starts at row %d with %d rows, after %d rows", name, i, page.FirstRow, page.Rows, rows)
				}
				e := got[page.RowGroup]
				if len(e.first) == 0 {
					e.first = append(e.first, 0) // row group relative below
				} else {
					e.first = append(e.first, page.FirstRow-pages[i-len(e.first)].FirstRow)
				}
				e.minLen, e.maxLen = append(e.minLen, int64(len(page.Min))), append(e.maxLen, int64(len(page.Max)))
				got[page.RowGroup] = e
				rows += page.Rows
			}
			if rows != p.Rows || fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s: key pages %v (%d rows), STRUCTURE.txt %v (%d rows)", name, got, rows, want, p.Rows)
			}
			if bytes.Compare(pages[0].Min, p.MinKey) > 0 || bytes.Compare(pages[len(pages)-1].Max, p.MaxKey) < 0 {
				t.Errorf("%s: page bounds do not enclose the manifest bounds", name)
			}
		}
	}
}

// countingReader records the byte ranges read through it.
type countingReader struct {
	r     *bytes.Reader
	mu    sync.Mutex
	reads [][2]int64
}

func (c *countingReader) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	c.reads = append(c.reads, [2]int64{off, off + int64(len(p))})
	c.mu.Unlock()
	return c.r.ReadAt(p, off)
}

// TestInspectReadsNoPages: InspectFooter reads only the magic number and
// the footer, and InspectKeyPages adds only the key column's page indexes:
// neither reads a byte of any column chunk.
func TestInspectReadsNoPages(t *testing.T) {
	path := "../conformance/datasets/multipart/shards/a.parquet"
	raw, err := os.ReadFile(path)
	must(t, err)
	size := int64(len(raw))
	footerStart := size - 8 - int64(binary.LittleEndian.Uint32(raw[size-8:]))
	pf, err := parquet.OpenFile(bytes.NewReader(raw), size)
	must(t, err)
	var chunks [][2]int64
	for _, rg := range pf.Metadata().RowGroups {
		for _, c := range rg.Columns {
			start := c.MetaData.DataPageOffset
			if c.MetaData.DictionaryPageOffset > 0 {
				start = min(start, c.MetaData.DictionaryPageOffset)
			}
			chunks = append(chunks, [2]int64{start, start + c.MetaData.TotalCompressedSize})
		}
	}
	inChunk := func(r [2]int64) bool {
		for _, c := range chunks {
			if r[0] < c[1] && c[0] < r[1] {
				return true
			}
		}
		return false
	}

	c := &countingReader{r: bytes.NewReader(raw)}
	if _, err := InspectFooter(c, size); err != nil {
		t.Fatal(err)
	}
	for _, r := range c.reads {
		if !(r[1] <= 4 || r[0] >= footerStart) {
			t.Errorf("InspectFooter read [%d, %d) outside the magic number and the footer (from %d)", r[0], r[1], footerStart)
		}
	}
	c = &countingReader{r: bytes.NewReader(raw)}
	pages, err := InspectKeyPages(c, size, "key")
	must(t, err)
	if len(pages) != 9 {
		t.Errorf("%d key pages, want 9", len(pages))
	}
	index := 0
	for _, r := range c.reads {
		if inChunk(r) {
			t.Errorf("InspectKeyPages read [%d, %d), inside a column chunk", r[0], r[1])
		}
		if r[1] <= footerStart && r[0] >= 4 {
			index++
		}
	}
	if index == 0 {
		t.Errorf("InspectKeyPages read no page index")
	}
	if _, err := InspectKeyPages(bytes.NewReader(raw), size, "size"); CodeOf(err) != CodeStampMismatch {
		t.Errorf("another key column: %v", err)
	}
}

// TestInspectRefusals: a file without valid stamps is refused.
func TestInspectRefusals(t *testing.T) {
	dir := write(t, parts(testutilPart(), []string{"a", "b"}))
	path := partFile(dir, 0)
	for name, c := range map[string]struct {
		edit func(kv map[string][]string)
		code Code
	}{
		"missing stamp":    {func(kv map[string][]string) { delete(kv, StampSchemaID) }, CodeStampMismatch},
		"twice":            {func(kv map[string][]string) { kv[StampKeyUnique] = []string{"true", "true"} }, CodeStampMismatch},
		"leading zero":     {func(kv map[string][]string) { kv[StampDraftRevision] = []string{"01"} }, CodeStampMismatch},
		"unique spelling":  {func(kv map[string][]string) { kv[StampKeyUnique] = []string{"TRUE"} }, CodeStampMismatch},
		"later revision":   {func(kv map[string][]string) { kv[StampDraftRevision] = []string{"2"} }, CodeUnsupportedVersion},
		"final with draft": {func(kv map[string][]string) { kv[StampFormatVersion] = []string{"1"} }, CodeStampMismatch},
		"no key column":    {func(kv map[string][]string) { kv[StampKeyColumn] = []string{"nope"} }, CodeInvalidKeyColumn},
	} {
		raw := stampedCopy(t, path, c.edit)
		if _, err := InspectFooter(bytes.NewReader(raw), int64(len(raw))); CodeOf(err) != c.code {
			t.Errorf("%s: %v, want %s", name, err, c.code)
		}
	}
	if _, err := InspectFooter(bytes.NewReader([]byte("PAR1 not parquet PAR1")), 21); CodeOf(err) != CodeInvalidFooter {
		t.Errorf("not Parquet: %v", err)
	}
}

func testutilPart() testutil.Part { return testutil.Part{} }

// stampedCopy returns the bytes of the part at path with its key-value
// metadata changed by edit.
func stampedCopy(t *testing.T, path string, edit func(kv map[string][]string)) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	must(t, err)
	copyPath := filepath.Join(t.TempDir(), "part.parquet")
	must(t, os.WriteFile(copyPath, raw, 0o644))
	must(t, testutil.RewriteFooter(copyPath, func(meta *format.FileMetaData) error {
		kv := map[string][]string{}
		for _, e := range meta.KeyValueMetadata {
			kv[e.Key] = append(kv[e.Key], e.Value)
		}
		edit(kv)
		meta.KeyValueMetadata = nil
		for _, k := range slices.Sorted(maps.Keys(kv)) {
			for _, v := range kv[k] {
				meta.KeyValueMetadata = append(meta.KeyValueMetadata, format.KeyValue{Key: k, Value: v})
			}
		}
		return nil
	}))
	raw, err = os.ReadFile(copyPath)
	must(t, err)
	return raw
}
