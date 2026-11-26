// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

func listPageFixture(t *testing.T) (*Dataset, []testutil.Row) {
	t.Helper()
	rows := testutil.Rows("a", "a/", "a/1", "a/2", "b", "c/", "c/d/1", "c/d/2", "z")
	d := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows[:4], GroupRows: 2, PageBytes: 32}, {Rows: rows[4:], GroupRows: 2, PageBytes: 32}}}), OpenOptions{})
	return d, rows
}

// Ported from swath page_test.go TestPage: listing one entry beyond each
// page, for every page size, joins the pages into the whole listing with
// no empty final page, and establishes the rows of every common prefix a
// page returns, whose fold equals the fixture's rows.
func TestListPages(t *testing.T) {
	d, rows := listPageFixture(t)
	for _, prefix := range []string{"", "a/", "c/"} {
		expected, err := d.List(ctx, ListQuery{Prefix: []byte(prefix), Delimiter: '/', Limit: 100, Columns: []string{"size"}}, nil)
		must(t, err)
		for _, limit := range []int{1, 2, 3, 100} {
			q := ListQuery{Prefix: []byte(prefix), Delimiter: '/', Limit: limit, Columns: []string{"size"}}
			var got []ListEntry
			for calls := 0; ; calls++ {
				if calls > len(rows) {
					t.Fatal("cursor did not advance")
				}
				entries, next, err := listPage(ctx, d, q, nil)
				must(t, err)
				if len(entries) == 0 && (next != nil || calls > 0) || next != nil && len(entries) != limit {
					t.Fatalf("prefix=%q limit=%d page %d: %+v next=%v", prefix, limit, calls, entries, next)
				}
				var spans []RowSpan
				var prefixes [][]byte
				for _, e := range entries {
					got = append(got, e)
					if e.CommonPrefix == nil {
						continue
					}
					_, start := e.Rows.Start.Ordinal()
					_, end := e.Rows.End.Ordinal()
					if !start || !end {
						t.Fatalf("prefix=%q limit=%d: the rows of %q are not established: %+v", prefix, limit, e.CommonPrefix, e.Rows)
					}
					spans = append(spans, e.Rows)
					prefixes = append(prefixes, e.CommonPrefix)
				}
				folded, err := FoldRows(ctx, d, spans, sizeFold(spans, "size"), FoldOptions{}, nil)
				must(t, err)
				for i, r := range folded {
					var count, sum int64
					for _, row := range rows {
						if bytes.HasPrefix(row.Key, prefixes[i]) {
							count++
							sum += *row.Size
						}
					}
					if r.Status != FoldExact || r.Value.rows != count || r.Value.sum != sum {
						t.Fatalf("prefix=%q limit=%d entry=%q: %+v, want %d rows of size %d", prefix, limit, prefixes[i], r, count, sum)
					}
				}
				if next == nil {
					break
				}
				q.Cursor = next
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("prefix=%q limit=%d: %+v want %+v", prefix, limit, got, expected)
			}
		}
	}
}

// Ported from the incomplete-prefix cases of swath's aggregates_oracle_test.go:
// a cursor inside a common prefix starts its rows at the first remaining
// candidate, and End inside it leaves their end unknown.
func TestListPageClippedPrefixes(t *testing.T) {
	d, _ := listPageFixture(t)
	for _, tc := range []struct {
		list       ListQuery
		prefix     string
		start, end int64 // -1 for unknown
	}{
		{ListQuery{Delimiter: '/', Limit: 100, Cursor: &Cursor{Key: []byte("a/1")}}, "a/", 3, 4},
		{ListQuery{Delimiter: '/', Limit: 100, End: []byte("a/2")}, "a/", 1, -1},
		{ListQuery{Delimiter: '/', Limit: 100, Prefix: []byte("c/"), Cursor: &Cursor{Key: []byte("c/d/1")}}, "c/d/", 7, 8},
	} {
		entries, err := d.List(ctx, tc.list, nil)
		must(t, err)
		found := false
		for _, e := range entries {
			if string(e.CommonPrefix) != tc.prefix {
				continue
			}
			found = true
			start, startKnown := e.Rows.Start.Ordinal()
			end, endKnown := e.Rows.End.Ordinal()
			if !startKnown || start != tc.start || endKnown != (tc.end >= 0) || endKnown && end != tc.end {
				t.Fatalf("%s: rows of %q are %+v, want [%d, %d)", describe(tc.list), tc.prefix, e.Rows, tc.start, tc.end)
			}
		}
		if !found {
			t.Fatalf("%s: no common prefix %q in %+v", describe(tc.list), tc.prefix, entries)
		}
	}
	// A prefix cursor beyond c/ returns no clipped prefix.
	entries, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 100, Cursor: &Cursor{Prefix: []byte("c/d/")}}, nil)
	must(t, err)
	if got := entriesOf(entries); !reflect.DeepEqual(got, []string{"r:z"}) {
		t.Fatalf("after prefix cursor c/d/: %q", got)
	}
}

func TestListPageFailures(t *testing.T) {
	d, _ := listPageFixture(t)
	for _, q := range []ListQuery{{}, {Limit: -1}} {
		st := ReadStats{SourceReads: 9}
		entries, err := d.List(ctx, q, &st)
		if !errors.Is(err, ErrInvalidRequest) || entries != nil || st != (ReadStats{}) {
			t.Fatal(entries, err, st)
		}
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	entries, next, err := listPage(c, d, ListQuery{Delimiter: '/', Limit: 1}, nil)
	if entries != nil || next != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(entries, next, err)
	}
	must(t, d.Close())
	if _, _, err := listPage(ctx, d, ListQuery{Delimiter: '/', Limit: 1}, nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// Ported from swath TestPageCancelledBetweenCalls and TestPageFailures: a
// page's listing fails with its source's error, or with the cancellation
// of its last read, and fills its statistics either way.
func TestListPageSourceFailureStatsAndCancellation(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a/1", "a/2", "b/1", "b/2"), GroupRows: 2, PageBytes: 32}}})
	q := ListQuery{Delimiter: '/', Limit: 1}
	baseline := &readHookSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, baseline, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
	must(t, err)
	var reads atomic.Int64
	baseline.read = func(string, int, int64) error { reads.Add(1); return nil }
	_, _, err = listPage(ctx, d, q, nil)
	must(t, err)
	must(t, d.Close())
	if reads.Load() == 0 {
		t.Fatal("listing fixture did not read")
	}
	for _, cancelled := range []bool{false, true} {
		src := &readHookSource{Source: dirOf(t, dir)}
		d, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
		must(t, err)
		c, cancel := context.WithCancel(ctx)
		var seen atomic.Int64
		failure := errors.New("listing source failure")
		src.read = func(string, int, int64) error {
			n := seen.Add(1)
			if !cancelled {
				return failure
			}
			if n == reads.Load() {
				cancel()
			}
			return nil
		}
		var st ReadStats
		entries, next, err := listPage(c, d, q, &st)
		cancel()
		must(t, d.Close())
		want := failure
		if cancelled {
			want = context.Canceled
		}
		if entries != nil || next != nil || !errors.Is(err, want) || st.SourceReads == 0 || cancelled && seen.Load() != reads.Load() {
			t.Fatal(entries, next, err, st, seen.Load(), reads.Load())
		}
	}
}
