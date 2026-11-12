// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

func aggregatePageFixture(t *testing.T) (*Dataset, []testutil.Row) {
	t.Helper()
	rows := testutil.Rows("a", "a/", "a/1", "a/2", "b", "c/", "c/d/1", "c/d/2", "z")
	d := verified(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows[:4], GroupRows: 2, PageBytes: 32}, {Rows: rows[4:], GroupRows: 2, PageBytes: 32}}}), OpenOptions{})
	return d, rows
}

// Ported from swath page_test.go TestPage: every page size, exact rows,
// complete-prefix reductions and no empty final page.
func TestPagePagination(t *testing.T) {
	d, rows := aggregatePageFixture(t)
	for _, prefix := range []string{"", "a/", "c/"} {
		expected, err := d.List(ctx, ListQuery{Prefix: []byte(prefix), Delimiter: '/', Limit: 100, Columns: []string{"size"}}, nil)
		must(t, err)
		for _, limit := range []int{1, 2, 3, 100} {
			q := PageQuery{ListQuery: ListQuery{Prefix: []byte(prefix), Delimiter: '/', Limit: limit, Columns: []string{"size"}}, Reductions: []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, Aggregates: AggregateOptions{ValueBudgetBytes: -1}}
			var got []ListEntry
			for calls := 0; ; calls++ {
				if calls > len(rows) {
					t.Fatal("cursor did not advance")
				}
				page, err := d.Page(ctx, q, nil)
				must(t, err)
				if len(page.Entries) == 0 && page.Next != nil || page.Next != nil && len(page.Entries) != limit {
					t.Fatalf("%+v", page)
				}
				for _, e := range page.Entries {
					got = append(got, e.ListEntry)
					if e.CommonPrefix == nil {
						if e.Aggregates != nil {
							t.Fatal(e)
						}
						continue
					}
					var count, sum int64
					for _, r := range rows {
						if bytes.HasPrefix(r.Key, e.CommonPrefix) {
							count++
							sum += *r.Size
						}
					}
					if e.Aggregates[0].Status != AggregateExact || e.Aggregates[0].Value.Int64() != count || e.Aggregates[1].Status != AggregateExact || e.Aggregates[1].Value.Int64() != sum {
						t.Fatalf("prefix=%q limit=%d entry=%q: %+v want=%d,%d", prefix, limit, e.CommonPrefix, e.Aggregates, count, sum)
					}
				}
				if page.Next == nil {
					break
				}
				q.Cursor = page.Next
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("prefix=%q limit=%d: %+v want %+v", prefix, limit, got, expected)
			}
		}
	}
}

// Ported from swath TestPageAggregatesReturnedPrefixesOnly, TestPageStats,
// TestPageBudget and incomplete-prefix cases in aggregates_oracle_test.go.
func TestPageBudgetAndClipping(t *testing.T) {
	d, _ := aggregatePageFixture(t)
	q := PageQuery{ListQuery: ListQuery{Delimiter: '/', Limit: 1}, Reductions: []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, Aggregates: AggregateOptions{ValueBudgetBytes: -1}}
	var st, listed ReadStats
	page, err := d.Page(ctx, q, &st)
	must(t, err)
	_, err = d.List(ctx, ListQuery{Delimiter: '/', Limit: 2}, &listed)
	must(t, err)
	if len(page.Entries) != 1 || page.Entries[0].Row == nil || st.ValuePagesDecoded != 0 || st != listed {
		t.Fatalf("%+v stats=%+v listed=%+v", page, st, listed)
	}
	q.Limit = 100
	q.Aggregates.ValueBudgetBytes = 1
	page, err = d.Page(ctx, q, nil)
	must(t, err)
	for _, e := range page.Entries {
		if e.CommonPrefix != nil && (e.Aggregates[0].Status != AggregateExact || e.Aggregates[1].Status != AggregateBudgetExceeded) {
			t.Fatal(e)
		}
	}
	for _, list := range []ListQuery{
		{Delimiter: '/', Limit: 100, Cursor: &Cursor{Key: []byte("a/1")}},
		{Delimiter: '/', Limit: 100, Cursor: &Cursor{Prefix: []byte("c/d/")}},
		{Delimiter: '/', Limit: 100, End: []byte("a/2")},
	} {
		page, err = d.Page(ctx, PageQuery{ListQuery: list, Reductions: q.Reductions, Aggregates: AggregateOptions{ValueBudgetBytes: -1}}, nil)
		must(t, err)
		if len(page.Entries) == 0 {
			t.Fatal("fixture returned no entries")
		}
		found := false
		for _, e := range page.Entries {
			if bytes.Equal(e.CommonPrefix, []byte("a/")) || bytes.Equal(e.CommonPrefix, []byte("c/")) {
				clipped := list.Cursor != nil && (bytes.HasPrefix(list.Cursor.Key, e.CommonPrefix) || bytes.HasPrefix(list.Cursor.Prefix, e.CommonPrefix)) || list.End != nil
				if clipped {
					found = true
					for _, a := range e.Aggregates {
						if a.Status != AggregateUnknown {
							t.Fatalf("clipped prefix %q %+v", e.CommonPrefix, e.Aggregates)
						}
					}
				}
			}
		}
		// A prefix cursor wholly beyond c/ may leave no clipped prefix to return.
		if list.Cursor == nil || list.Cursor.Prefix == nil {
			if !found {
				t.Fatalf("no clipped prefix in %+v", page)
			}
		}
	}
}

func TestPageFailures(t *testing.T) {
	d, _ := aggregatePageFixture(t)
	for _, q := range []PageQuery{{}, {ListQuery: ListQuery{Limit: -1}}, {ListQuery: ListQuery{Limit: math.MaxInt}}, {ListQuery: ListQuery{Limit: 1}, Aggregates: AggregateOptions{ValueBudgetBytes: -2}}, {ListQuery: ListQuery{Limit: 1}, Reductions: []Reduction{{Op: Sum}}}} {
		st := ReadStats{SourceReads: 9}
		page, err := d.Page(ctx, q, &st)
		if !errors.Is(err, ErrInvalidRequest) || page.Entries != nil || page.Next != nil || st != (ReadStats{}) {
			t.Fatal(page, err, st)
		}
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	page, err := d.Page(c, PageQuery{ListQuery: ListQuery{Delimiter: '/', Limit: 1}}, nil)
	if page.Entries != nil || page.Next != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(page, err)
	}
	must(t, d.Close())
	_, err = d.Page(ctx, PageQuery{ListQuery: ListQuery{Delimiter: '/', Limit: 1}}, nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// Ported from swath TestPageCancelledBetweenCalls and TestPageFailures.
func TestPageSourceFailureStatsAndCancellation(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a/1", "a/2", "b/1", "b/2"), GroupRows: 2, PageBytes: 32}}})
	q := PageQuery{ListQuery: ListQuery{Delimiter: '/', Limit: 1}, Reductions: []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, Aggregates: AggregateOptions{ValueBudgetBytes: -1}}
	baseline := &readHookSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, baseline, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
	must(t, err)
	var reads atomic.Int64
	baseline.read = func(string, int, int64) error { reads.Add(1); return nil }
	var listed ReadStats
	list := q.ListQuery
	list.Limit++
	_, err = d.List(ctx, list, &listed)
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
		page, err := d.Page(c, q, &st)
		cancel()
		must(t, d.Close())
		want := failure
		if cancelled {
			want = context.Canceled
		}
		if page.Entries != nil || page.Next != nil || !errors.Is(err, want) || st.SourceReads == 0 || cancelled && seen.Load() != reads.Load() {
			t.Fatal(page, err, st, seen.Load(), reads.Load())
		}
	}
}
