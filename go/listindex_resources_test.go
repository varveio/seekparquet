// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"github.com/varveio/seekparquet/go/internal/testutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestListIndexSharedMemoryIsolation(t *testing.T) {
	var keys []string
	for i := 0; i < 4000; i++ {
		keys = append(keys, fmt.Sprintf("d%04d/f", i))
	}
	dir := write(t, parts(testutil.Part{GroupRows: 200, PageBytes: 200, IndexSizeLimit: 16}, keys))
	probe := open(t, dir, OpenOptions{})
	must(t, probe.Verify(ctx))
	lim := Limits{IndexMemory: probe.r.indexMemory.Load() / 2}
	q := ListQuery{Prefix: []byte("d3999/"), Delimiter: '/', Limit: 10}
	plain := open(t, dir, OpenOptions{Limits: lim})
	want, err := plain.List(ctx, q, nil)
	must(t, err)
	for _, budget := range []int64{1, 0} {
		for _, warm := range []bool{false, true} {
			d := open(t, dir, OpenOptions{Limits: lim})
			if warm {
				_, err = d.List(ctx, ListQuery{Prefix: []byte("d0000/"), Delimiter: '/', Limit: 1}, nil)
				must(t, err)
			}
			before := d.r.indexMemory.Load()
			st, err := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget, Workers: 3})
			must(t, err)
			if d.r.indexMemory.Load() != before || st.GroupsFailed != 0 {
				t.Fatalf("build consumed shared memory: %d -> %d, %+v", before, d.r.indexMemory.Load(), st)
			}
			got, err := d.List(ctx, q, nil)
			must(t, err)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("%v != %v", want, got)
			}
		}
	}
}

func TestListIndexLogarithmicPagination(t *testing.T) {
	var keys []string
	for i := 0; i < 20000; i++ {
		keys = append(keys, fmt.Sprintf("a/%05d/f", i))
	}
	keys = append(keys, "b/f")
	for _, rows := range []int{30000, 20} {
		d := open(t, write(t, parts(testutil.Part{GroupRows: rows, PageBytes: 30}, keys)), OpenOptions{})
		_, err := d.BuildListIndex(ctx, ListIndexOptions{})
		must(t, err)
		first, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1}, nil)
		must(t, err)
		if len(first) != 1 || string(first[0].CommonPrefix) != "a/" {
			t.Fatal(first)
		}
		for i := 0; i < 3; i++ {
			next, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1, Cursor: first[0].Cursor()}, nil)
			must(t, err)
			if len(next) != 1 || string(next[0].CommonPrefix) != "b/" {
				t.Fatal(next)
			}
			s := d.r.newScan(ctx, nil, new(ReadStats))
			must(t, s.seek(nil, true))
			counter := &cancelWalk{Context: ctx, n: 1000000}
			s.ctx = counter
			ok, err := s.skipList([]byte("a/"), '/')
			must(t, err)
			if !ok || 1000000-counter.n > 128 {
				t.Fatalf("rows=%d steps=%d handled=%v", rows, 1000000-counter.n, ok)
			}
		}
	}
	// Cancellation occurs in the binary search, not just before entering it.
	g := listGroup{base: []byte("a/")}
	for i := 0; i < 100000; i++ {
		g.add(int64(i), []byte(fmt.Sprintf("a/%06d/f", i)))
	}
	c := &cancelWalk{Context: ctx, n: 5}
	_, err := g.firstOutside(c, 0, []byte("a/"))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c = &cancelWalk{Context: ctx, n: 1000}
	i, err := g.firstOutside(c, 0, []byte("a/"))
	must(t, err)
	if i != len(g.runs) || 1000-c.n > 20 {
		t.Fatalf("i=%d steps=%d", i, 1000-c.n)
	}
}

func TestListIndexEmptyCacheAndEffectiveOptions(t *testing.T) {
	for _, keys := range [][]string{{"a", "b"}, {"a/x", "b/y"}} {
		d := open(t, write(t, parts(testutil.Part{}, keys)), OpenOptions{})
		opt := ListIndexOptions{MaxBytes: 1}
		st, err := d.BuildListIndex(ctx, opt)
		must(t, err)
		if st.HasIndex || d.r.listIndex.Load() == nil {
			t.Fatal(st)
		}
		again, err := d.BuildListIndex(ctx, opt)
		must(t, err)
		if again != st {
			t.Fatal("empty outcome was rebuilt")
		}
		full, err := d.BuildListIndex(ctx, ListIndexOptions{})
		must(t, err)
		if full.Options.MaxBytes != 64<<20 {
			t.Fatal(full)
		}
		if full.HasIndex {
			reused, err := d.BuildListIndex(ctx, opt)
			must(t, err)
			if !reused.OptionsMismatch {
				t.Fatal("missing options mismatch")
			}
			reused.OptionsMismatch = false
			if reused != full || reused.Options.MaxBytes == opt.MaxBytes {
				t.Fatal("missing effective option mismatch")
			}
		}
	}
}

// Optional cross-language export uses the checked-in conformance fixture bytes.
// Java's parity test consumes these independently built statistics.
func TestListIndexParity(t *testing.T) {
	output := os.Getenv("SEEKPARQUET_LIST_INDEX_PARITY")
	if output == "" {
		t.Skip("set SEEKPARQUET_LIST_INDEX_PARITY for the Java parity check")
	}
	var lines strings.Builder
	for _, name := range []string{"non-utf8-keys", "no-value-page-index", "encodings", "annotated"} {
		root, err := filepath.Abs(filepath.Join("..", "conformance", "datasets", name))
		must(t, err)
		for _, budget := range []int64{1, 599, 600, 1024, 4096, 1 << 20} {
			for _, cap := range []int{1, 2, 8, 1000} {
				d := open(t, root, OpenOptions{})
				st, err := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget, Workers: 2, MaxChangesPerPage: cap})
				must(t, err)
				fmt.Fprintf(&lines, "%s\t%d\t%d\t%d\t%d\t%d\t%t\n", root, budget, cap, st.GroupsIndexed, st.Entries, st.Bytes, st.BudgetExhausted)
			}
		}
	}
	must(t, os.WriteFile(output, []byte(lines.String()), 0600))
}
