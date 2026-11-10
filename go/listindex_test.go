// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

func TestListIndexDifferential(t *testing.T) {
	keys := []string{"/", "//", "//x", "a", "a/", "a//x", "a/0", "a/b/", "a/b/x", "a/c", "ab", "root"}
	for _, head := range []string{"wide/", strings.Repeat("q", 160) + "/", strings.Repeat("\xff", 160) + "/"} {
		for i := 0; i < 12; i++ {
			for j := 0; j < 90; j++ {
				keys = append(keys, fmt.Sprintf("%sd%03d/f%03d", head, i, j))
			}
		}
	}
	for i := 0; i < 16; i++ {
		keys = append(keys, fmt.Sprintf("one/%03d/x", i))
	}
	slices.Sort(keys)
	// Repeated keys pin exact-row offsets and cursor behavior too.
	keys = append(keys, keys...)
	slices.Sort(keys)
	fixture := parts(testutil.Part{GroupRows: 1000, PageBytes: 900, IndexSizeLimit: 16}, keys)
	fixture.RepeatedKeys = true
	dir := write(t, fixture)
	plain := open(t, dir, Options{})
	prefixes := map[string]bool{"": true, "wid": true, "wide/d00": true, "a/b": true}
	for _, k := range keys {
		for i := range k {
			if k[i] == '/' {
				prefixes[k[:i+1]] = true
			}
		}
	}
	for _, cap := range []int{1, 2, 0, 1000000} {
		for _, budget := range []int64{1, 2048, 1 << 24} {
			indexed := open(t, dir, Options{})
			stats, err := indexed.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget, MaxChangesPerPage: cap, Workers: 2})
			must(t, err)
			if stats.Bytes > budget {
				t.Fatalf("budget: %+v", stats)
			}
			for prefix := range prefixes {
				for _, delim := range []byte{'/', 'f'} {
					q := List{Prefix: []byte(prefix), Delimiter: delim, Limit: 1000, Columns: []string{"size", "name"}}
					all, err := plain.List(ctx, q)
					must(t, err)
					cursors := []*Cursor{nil}
					for i, e := range all {
						if i < 4 || i == len(all)-1 {
							cursors = append(cursors, e.Cursor())
						}
					}
					ends := [][]byte{nil}
					if len(all) > 1 {
						k := all[1].Bytes()
						ends = append(ends, bytes.Clone(k), append(bytes.Clone(k), 0))
						pos, _ := slices.BinarySearch(keys, string(k))
						if pos < len(keys) {
							anchor := []byte(keys[pos])
							ends = append(ends, anchor, append(bytes.Clone(anchor), 0), bytes.Clone(anchor[:len(anchor)-1]))
						}
						if len(k) > 0 {
							ends = append(ends, bytes.Clone(k[:len(k)-1]))
						}
					}
					for _, cursor := range cursors {
						for _, end := range ends {
							for _, limit := range []int{1, 2, 4, 1000} {
								q.Cursor = cursor
								q.End = end
								q.Limit = limit
								want, e1 := plain.List(ctx, q)
								got, e2 := indexed.List(ctx, q)
								if e1 != nil || e2 != nil || !reflect.DeepEqual(want, got) {
									t.Fatalf("cap=%d budget=%d prefix=%q cursor=%+v end=%q limit=%d errors=%v/%v\nwant=%v\ngot=%v", cap, budget, prefix, cursor, end, limit, e1, e2, entriesOf(want), entriesOf(got))
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestListIndexPageCountsAndConcurrency(t *testing.T) {
	for _, head := range []string{"wide/", strings.Repeat("x", 200) + "/"} {
		keys := []string{}
		for i := 0; i < 100; i++ {
			for j := 0; j < 100; j++ {
				keys = append(keys, fmt.Sprintf("%sd%03d/f%03d", head, i, j))
			}
		}
		dir := write(t, parts(testutil.Part{GroupRows: 20000, PageBytes: 1000, IndexSizeLimit: 16}, keys))
		d := open(t, dir, Options{})
		q := List{Prefix: []byte(head), Delimiter: '/', Limit: 1000}
		want, err := d.List(ctx, q)
		must(t, err)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 10 {
					got, e := d.List(ctx, q)
					if e != nil || !reflect.DeepEqual(want, got) {
						t.Error("concurrent listing", e)
					}
				}
			}()
		}
		st, err := d.BuildListIndex(ctx, ListIndexOptions{Workers: 2})
		must(t, err)
		wg.Wait()
		var reads ReadStats
		q.Stats = &reads
		got, err := d.List(ctx, q)
		must(t, err)
		if !reflect.DeepEqual(want, got) || reads.KeyPagesDecoded > 2 || reads.ListIndexSkips < 99 {
			t.Fatalf("build=%+v reads=%+v", st, reads)
		}
		q.Cursor = want[49].Cursor()
		got, err = d.List(ctx, q)
		must(t, err)
		if !reflect.DeepEqual(want[50:], got) || reads.KeyPagesDecoded > 2 {
			t.Fatalf("cursor pages: %+v", reads)
		}
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 10 {
					query := q
					query.Stats = nil
					result, e := d.List(ctx, query)
					if e != nil || !reflect.DeepEqual(want[50:], result) {
						t.Error("indexed concurrency", e)
					}
				}
			}()
		}
		wg.Wait()
	}
}

func TestListIndexFailureAndCancellation(t *testing.T) {
	keys := dirKeys(map[string]int{"a": 100, "b": 100}, "a", "b")
	keys[50], keys[51] = keys[51], keys[50]
	d := open(t, write(t, parts(testutil.Part{GroupRows: 200, PageBytes: 200}, keys)), Options{})
	st, err := d.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	if st.GroupsFailed != 1 || st.GroupsIndexed != 0 {
		t.Fatalf("%+v", st)
	}
	_, err = d.List(ctx, List{Prefix: []byte("a/f"), Delimiter: '/', Limit: 1000})
	if CodeOf(err) != CodeKeyOrder {
		t.Fatalf("%v", err)
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	_, err = d.BuildListIndex(c, ListIndexOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// cancelWalk deterministically cancels during the allocation-free run walk.
type cancelWalk struct {
	context.Context
	n int
}

func (c *cancelWalk) Err() error {
	c.n--
	if c.n <= 0 {
		return context.Canceled
	}
	return nil
}
func TestListIndexWalkCancellation(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{GroupRows: 2000, PageBytes: 200}, dirKeys(map[string]int{"a": 1000}, "a"))), Options{})
	_, err := d.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	s := d.r.newScan(ctx, nil, new(ReadStats))
	must(t, s.seek(nil, true))
	g := s.listIndex.group(s.g)
	// Thousands of runs under one parent; no storage reads or searches per step.
	g2 := *g
	g2.runs = nil
	g2.data = nil
	for i := 0; i < 10000; i++ {
		g2.add(int64(i), []byte(fmt.Sprintf("a/%05d/x", i)))
	}
	s.listIndex = &listIndex{groups: []listGroup{g2}}
	s.ctx = &cancelWalk{Context: ctx, n: 500}
	handled, err := s.skipList([]byte("a/"), '/')
	if handled || !errors.Is(err, context.Canceled) {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}

// Faults are injected only after opening and loading the page index.
type listFaultReader struct {
	io.ReaderAt
	offset int64
	cancel context.CancelFunc
}

func (f *listFaultReader) ReadAt(p []byte, off int64) (int, error) {
	if off == f.offset {
		if f.cancel != nil {
			f.cancel()
		} else {
			return 0, io.ErrUnexpectedEOF
		}
	}
	return f.ReaderAt.ReadAt(p, off)
}
func TestListIndexUnreadableAndBuildCancelledDuringRead(t *testing.T) {
	keys := dirKeys(map[string]int{"a": 100, "b": 100}, "a", "b")
	dir := write(t, parts(testutil.Part{GroupRows: 200, PageBytes: 200}, keys))
	for _, cancelBuild := range []bool{false, true} {
		d := open(t, dir, Options{})
		must(t, d.r.checkParts(ctx, new(ReadStats)))
		p := d.r.parts[0]
		idx, err := p.keyIndex(ctx, 0, new(ReadStats))
		must(t, err)
		original := p.file
		fault := &listFaultReader{ReaderAt: original, offset: idx.pages[2].offset}
		p.file = fault
		c, cancel := context.WithCancel(ctx)
		if cancelBuild {
			fault.cancel = cancel
		}
		st, err := d.BuildListIndex(c, ListIndexOptions{Workers: 1})
		if cancelBuild {
			if !errors.Is(err, context.Canceled) || st.Bytes != 0 || d.r.listIndex.Load() != nil {
				t.Fatalf("%+v %v", st, err)
			}
			p.file = original
			st, err = d.BuildListIndex(ctx, ListIndexOptions{})
			must(t, err)
			if st.GroupsIndexed != 1 {
				t.Fatalf("retry: %+v", st)
			}
		} else {
			must(t, err)
			if st.GroupsFailed != 1 {
				t.Fatalf("%+v", st)
			}
			_, err = d.List(ctx, List{Prefix: []byte("a/f"), Delimiter: '/', Limit: 1000})
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal(err)
			}
		}
		cancel()
		p.file = original
	}
}

func TestListIndexFlatAndSingleDirectoryStorage(t *testing.T) {
	for _, prefix := range []string{"", "one/"} {
		keys := []string{}
		for i := 0; i < 1000; i++ {
			keys = append(keys, fmt.Sprintf("%s%01000d", prefix, i))
		}
		d := open(t, write(t, parts(testutil.Part{GroupRows: 1000, PageBytes: 1200}, keys)), Options{})
		st, err := d.BuildListIndex(ctx, ListIndexOptions{})
		must(t, err)
		if prefix == "" {
			if st.Bytes != 0 || st.Entries != 0 {
				t.Fatalf("flat: %+v", st)
			}
		} else if st.Entries != 1 || st.Bytes > 4096 {
			t.Fatalf("one directory: %+v", st)
		}
	}
}

func TestListIndexDisorderedGroupBoundary(t *testing.T) {
	keys := dirKeys(map[string]int{"a": 100, "b": 100}, "a", "b")
	keys[99], keys[100] = keys[100], keys[99]
	d := open(t, write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 200}, keys)), Options{})
	st, err := d.BuildListIndex(ctx, ListIndexOptions{Workers: 2})
	must(t, err)
	if st.GroupsFailed != 2 || st.GroupsIndexed != 0 {
		t.Fatalf("%+v", st)
	}
	_, err = d.Range(ctx, Range{Limit: 1000})
	if CodeOf(err) != CodeKeyOrder {
		t.Fatal(err)
	}
}
