// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestScanUnlimited: a zero Limit scans to the end of the span, the rows a
// limit of the dataset's size returns, through the batch path, and for the
// whole dataset. Its remaining count is 64-bit.
func TestScanUnlimited(t *testing.T) {
	keys := fixtureKeys(300)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 40, PageBytes: 256}, split(keys, 3)...)), OpenOptions{})
	collect := func(q Query) []Row {
		var got []Row
		must(t, d.Scan(ctx, q, nil, func(b *Batch) error {
			for i := range b.Len() {
				got = append(got, b.Row(i))
			}
			return nil
		}))
		return got
	}
	for _, sel := range []Selection{{}, {Span: Span{Start: []byte(keys[17]), End: []byte(keys[251])}}, {Span: PrefixSpan([]byte("d0"))},
		{Span: Span{Start: []byte(keys[120])}, Cursor: RowCursor([]byte(keys[150]), 0)}} {
		want, err := d.Rows(ctx, Query{Selection: sel, Limit: int64(len(keys)) + 1, Columns: []string{"size", "name"}}, nil)
		must(t, err)
		if want == nil {
			t.Fatalf("%s: the fixture selects nothing", describe(sel))
		}
		if got := collect(Query{Selection: sel, Columns: []string{"size", "name"}}); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: unlimited scan %d rows, Rows %d", describe(sel), len(got), len(want))
		}
		if got := collect(Query{Selection: sel, Limit: math.MaxInt64, Columns: []string{"size", "name"}}); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: MaxInt64 limit %d rows, Rows %d", describe(sel), len(got), len(want))
		}
	}
	if n := len(collect(Query{})); n != len(keys) {
		t.Fatalf("whole dataset: %d rows", n)
	}
	if reflect.TypeOf(Query{}.Limit).Kind() != reflect.Int64 || unlimited != math.MaxInt64 {
		t.Fatal("the limit state is not 64-bit")
	}
}

// TestLimits: Rows requires a positive limit, Scan a non-negative one, List
// a positive one; each refusal is ErrInvalidRequest with statistics reset
// and no callback.
func TestRequestLimits(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, fixtureKeys(20))), OpenOptions{})
	for _, limit := range []int64{0, -1, math.MinInt64} {
		st := ReadStats{KeysDecoded: 9}
		if rows, err := d.Rows(ctx, Query{Limit: limit}, &st); !errors.Is(err, ErrInvalidRequest) || rows != nil || st != (ReadStats{}) {
			t.Fatalf("Rows limit %d: %d rows, %v, %+v", limit, len(rows), err, st)
		}
		if limit == 0 {
			continue
		}
		st = ReadStats{KeysDecoded: 9}
		if err := d.Scan(ctx, Query{Limit: limit}, &st, func(*Batch) error { t.Fatal("fn called"); return nil }); !errors.Is(err, ErrInvalidRequest) || st != (ReadStats{}) {
			t.Fatalf("Scan limit %d: %v, %+v", limit, err, st)
		}
		if _, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: int(max(limit, math.MinInt))}, nil); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("List limit %d: %v", limit, err)
		}
	}
	if _, err := d.List(ctx, ListQuery{Delimiter: '/'}, nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("List limit 0: %v", err)
	}
	if rows, err := d.Rows(ctx, Query{Limit: math.MaxInt64}, nil); err != nil || len(rows) != 20 {
		t.Fatalf("Rows MaxInt64: %d rows, %v", len(rows), err)
	}
}

// TestNilCallbacks: Dataset.Scan and Plan.Scan refuse a nil callback as an
// invalid request, before any read, with statistics reset.
func TestNilCallbacks(t *testing.T) {
	src := &countingSource{Source: dirOf(t, write(t, parts(testutil.Part{}, fixtureKeys(20))))}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	st := ReadStats{KeysDecoded: 1}
	if err := d.Scan(ctx, Query{Limit: 5}, &st, nil); !errors.Is(err, ErrInvalidRequest) || st != (ReadStats{}) || src.opened.Load() != 1 {
		t.Fatalf("Dataset.Scan: %v, %+v, %d opened", err, st, src.opened.Load())
	}
	plan, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
	must(t, err)
	st = ReadStats{KeysDecoded: 1}
	if err := plan.Scan(ctx, &st, nil); !errors.Is(err, ErrInvalidRequest) || st != (ReadStats{}) {
		t.Fatalf("Plan.Scan: %v, %+v", err, st)
	}
}

// TestStop: a callback returning Stop ends either scan with a nil error
// after that batch; Stop wrapped is an ordinary error, returned unchanged.
func TestStop(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{GroupRows: 5, PageBytes: 64}, fixtureKeys(60))), OpenOptions{})
	plan, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
	must(t, err)
	scans := map[string]func(fn func(*Batch) error) error{
		"Dataset.Scan": func(fn func(*Batch) error) error { return d.Scan(ctx, Query{Columns: []string{"size"}}, nil, fn) },
		"Plan.Scan":    func(fn func(*Batch) error) error { return plan.Scan(ctx, nil, fn) },
	}
	for name, scan := range scans {
		calls := 0
		if err := scan(func(*Batch) error { calls++; return Stop }); err != nil || calls != 1 {
			t.Fatalf("%s: %v after %d calls", name, err, calls)
		}
		calls = 0
		if err := scan(func(*Batch) error {
			if calls++; calls == 3 {
				return Stop
			}
			return nil
		}); err != nil || calls != 3 {
			t.Fatalf("%s: %v after %d calls", name, err, calls)
		}
		wrapped := errors.Join(Stop)
		if err := scan(func(*Batch) error { return wrapped }); err != wrapped {
			t.Fatalf("%s: wrapped Stop gave %v", name, err)
		}
	}
}

// TestFailedPlanAndSplitStats: a plan or a split that fails still reports
// the reads it made, through its statistics argument.
func TestFailedPlanAndSplitStats(t *testing.T) {
	dir := write(t, lazyParts())
	rewrite(t, dir, 3, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	d := open(t, dir, OpenOptions{})
	st := ReadStats{GroupsSkipped: 5}
	plan, err := d.Plan(ctx, []Selection{{Span: PrefixSpan([]byte("c/"))}, {Span: PrefixSpan([]byte("d/"))}}, []string{"size"}, &st)
	if RefusalCodeOf(err) != CodeStampMismatch || plan != nil {
		t.Fatalf("plan over a bad part: %v", err)
	}
	if st.PartsOpened != 2 || st.SourceReads < 2 || st.GroupsSkipped != 0 {
		t.Fatalf("failed planning: %+v", st)
	}

	// A split cancelled after its first key page read.
	src := &hookSource{Source: dirOf(t, boundaryDataset(t))}
	bd, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = bd.Close() })
	plan, err = bd.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	var whole ReadStats
	_, err = plan.Split(ctx, 4, &whole)
	must(t, err)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reads := 0
	src.hook = func() {
		if reads++; reads == 2 {
			cancel()
		}
	}
	st = ReadStats{}
	shards, err := plan.Split(cctx, 4, &st)
	src.hook = nil
	if !errors.Is(err, context.Canceled) || shards != nil {
		t.Fatalf("cancelled split: %d shards, %v", len(shards), err)
	}
	if st.SourceReads < 1 || st.KeyPagesDecoded < 1 || st.KeyPagesDecoded >= whole.KeyPagesDecoded {
		t.Fatalf("failed split %+v, whole split %+v", st, whole)
	}
}

// TestRowsNoRowsOnError: Rows returns no rows with an error, even when the
// key discovery succeeded and a projection failed.
func TestRowsNoRowsOnError(t *testing.T) {
	type nested struct {
		Key []byte `parquet:"key"`
		Tag struct {
			A int64 `parquet:"a"`
		} `parquet:"tag"`
	}
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: testutil.Rows("a", "b")},
		{Rows: []nested{{Key: []byte("c")}}},
	}}), OpenOptions{})
	rows, err := d.Rows(ctx, Query{Limit: 10, Columns: []string{"tag"}}, nil)
	var u *UnsupportedError
	if !errors.As(err, &u) || rows != nil {
		t.Fatalf("%d rows, %v", len(rows), err)
	}
}

// TestColumnsAccessors: Batch.Columns and Plan.Columns are the projection,
// as copies the caller may change.
func TestColumnsAccessors(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, fixtureKeys(10))), OpenOptions{})
	columns := []string{"size", "name", "size"}
	plan, err := d.Plan(ctx, []Selection{{}}, columns, nil)
	must(t, err)
	columns[0] = "changed"
	if got := plan.Columns(); !slices.Equal(got, []string{"size", "name", "size"}) {
		t.Fatalf("plan columns %q", got)
	}
	plan.Columns()[1] = "changed"
	shards, err := plan.Split(ctx, 2, nil)
	must(t, err)
	for _, p := range append(shards, plan) {
		if got := p.Columns(); !slices.Equal(got, []string{"size", "name", "size"}) {
			t.Fatalf("columns %q", got)
		}
		must(t, p.Scan(ctx, nil, func(b *Batch) error {
			got := b.Columns()
			got[0] = "changed"
			if !slices.Equal(b.Columns(), []string{"size", "name", "size"}) || b.Row(0).Columns[0] != "size" {
				t.Fatalf("batch columns %q", b.Columns())
			}
			return nil
		}))
	}
}

// TestCursorConstructors: RowCursor and PrefixCursor own their bytes and
// make a cursor of exactly one kind, the empty key or prefix included.
func TestCursorConstructors(t *testing.T) {
	key := []byte("k")
	c := RowCursor(key, math.MaxInt64)
	key[0] = 'x'
	if string(c.Key) != "k" || c.Offset != math.MaxInt64 || c.Prefix != nil || c.check(false) != nil {
		t.Fatalf("%+v", c)
	}
	for _, c := range []*Cursor{RowCursor(nil, 0), RowCursor([]byte{}, 3)} {
		if c.Key == nil || len(c.Key) != 0 || c.check(false) != nil {
			t.Fatalf("empty key cursor %+v", c)
		}
	}
	for _, c := range []*Cursor{PrefixCursor(nil), PrefixCursor([]byte("a/"))} {
		if c.Prefix == nil || c.Key != nil || c.Offset != 0 || c.check(true) != nil || !errors.Is(c.check(false), ErrInvalidRequest) {
			t.Fatalf("prefix cursor %+v", c)
		}
	}
	if !bytes.Equal(PrefixCursor([]byte("a/")).Prefix, []byte("a/")) {
		t.Fatal("prefix bytes")
	}
}

// TestCachedRefusalDetached: a part's refusal is returned to each read as
// a copy of its own, so that changing one does not change what a later
// read is told.
func TestCachedRefusalDetached(t *testing.T) {
	dir := write(t, lazyParts())
	rewrite(t, dir, 3, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	d := open(t, dir, OpenOptions{})
	read := func() *RefusalError {
		_, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("d/"))}, Limit: 1}, nil)
		var refusal *RefusalError
		if !errors.As(err, &refusal) || refusal.Code != CodeStampMismatch {
			t.Fatalf("%v", err)
		}
		return refusal
	}
	first := read()
	want := *first
	first.Code, first.Part, first.Detail, first.Key = "changed", "changed", "changed", []byte("changed")
	for range 2 {
		again := read()
		if again == first || again.Code != want.Code || again.Part != want.Part || again.Detail != want.Detail || again.Key != nil {
			t.Fatalf("cached refusal changed: %+v", again)
		}
		again.Detail = "changed again"
	}
	// detach copies an unsupported error too, and a refusal's key.
	detached := detach(&UnsupportedError{Code: UnsupportedEncoding, Detail: "x"}).(*UnsupportedError)
	if detached.Code != UnsupportedEncoding || detached.Detail != "x" {
		t.Fatal("unsupported detach")
	}
	r := &RefusalError{Code: CodeKeyOrder, Key: []byte("k")}
	if c := detach(r).(*RefusalError); c == r || &c.Key[0] == &r.Key[0] || string(c.Key) != "k" {
		t.Fatal("refusal detach shares its key")
	}
}

// TestEncryptedErrorsDetached: each encrypted part, and each InspectFooter,
// reports an UnsupportedError of its own: changing the one a read returned
// changes neither another part's, another dataset's, nor InspectFooter's.
func TestEncryptedErrorsDetached(t *testing.T) {
	encrypt := func(dir string, i int) {
		raw, err := os.ReadFile(partFile(dir, i))
		must(t, err)
		copy(raw[len(raw)-4:], "PARE")
		must(t, os.WriteFile(partFile(dir, i), raw, 0o644))
	}
	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}, []string{"c", "d"}))
	encrypt(dir, 0)
	encrypt(dir, 1)
	d := open(t, dir, OpenOptions{})
	read := func(d *Dataset, prefix string) *UnsupportedError {
		_, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte(prefix))}, Limit: 1}, nil)
		var u *UnsupportedError
		if !errors.As(err, &u) || u.Code != UnsupportedEncryption {
			t.Fatalf("%s: %v", prefix, err)
		}
		return u
	}
	inspect := func() *UnsupportedError {
		f, err := os.Open(partFile(dir, 1))
		must(t, err)
		defer f.Close()
		info, err := f.Stat()
		must(t, err)
		_, err = InspectFooter(f, info.Size())
		var u *UnsupportedError
		if !errors.As(err, &u) || u.Code != UnsupportedEncryption {
			t.Fatalf("InspectFooter: %v", err)
		}
		return u
	}
	first := read(d, "a")
	want := *first
	first.Code, first.Detail = "changed", "changed"
	second := inspect()
	second.Code, second.Detail = "changed", "changed"
	for _, u := range []*UnsupportedError{read(d, "c"), read(d, "a"), read(open(t, dir, OpenOptions{}), "c"), inspect()} {
		if u == first || u == second || *u != want {
			t.Fatalf("a changed error leaked: %+v", u)
		}
	}
}
