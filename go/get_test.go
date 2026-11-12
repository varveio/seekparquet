// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// getFixture is a repeated-key dataset whose groups g4, g5 and g6 hold 4, 5
// and 6 rows, between single keys, over small pages.
func getFixture(t *testing.T) string {
	var keys []string
	keys = append(keys, "a")
	for _, g := range []struct {
		key string
		n   int
	}{{"g4", 4}, {"g5", 5}, {"g6", 6}} {
		for range g.n {
			keys = append(keys, g.key)
		}
	}
	keys = append(keys, "z")
	doc := testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: testutil.Rows(keys...), PageBytes: 64, GroupRows: 7}}}
	return write(t, doc)
}

// TestGet: Get returns a whole group at the cap's edges, with its values,
// the rows Rows returns for KeySpan(key); one row over the cap fails with
// a *LimitError naming GetRows, with no rows and no value page read; a
// missing key has no rows.
func TestGet(t *testing.T) {
	dir := getFixture(t)
	d := open(t, dir, OpenOptions{Limits: Limits{GetRows: 5}})
	columns := []string{"size", "name"}
	for _, key := range []string{"g4", "g5", "a", "z"} {
		var st ReadStats
		got, err := d.Get(ctx, []byte(key), &st, columns...)
		must(t, err)
		want, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(key))}, Limit: 100, Columns: columns}, nil)
		must(t, err)
		if !reflect.DeepEqual(got, want) || len(got) != map[string]int{"g4": 4, "g5": 5, "a": 1, "z": 1}[key] {
			t.Fatalf("%s: %d rows, Rows %d", key, len(got), len(want))
		}
		if st.ValuePagesDecoded == 0 || st.KeysDecoded == 0 {
			t.Fatalf("%s: stats %+v", key, st)
		}
	}
	for _, key := range []string{"g", "g40", "", "zz"} {
		if got, err := d.Get(ctx, []byte(key), nil, columns...); err != nil || got != nil {
			t.Fatalf("missing %q: %v, %v", key, got, err)
		}
	}
	st := ReadStats{SourceReads: 1}
	got, err := d.Get(ctx, []byte("g6"), &st, columns...)
	var le *LimitError
	if got != nil || !errors.Is(err, ErrLimitExceeded) || !errors.As(err, &le) || le.Limit != LimitGetRows || le.Max != 5 ||
		!strings.Contains(err.Error(), "GetRows") || !strings.Contains(err.Error(), "KeySpan") {
		t.Fatalf("over the cap: %v, %v", got, err)
	}
	if st.ValuePagesDecoded != 0 || st.KeysDecoded == 0 {
		t.Fatalf("over the cap read value pages or nothing: %+v", st)
	}
	// Keys only, and the default cap.
	d2 := open(t, dir, OpenOptions{})
	if got, err := d2.Get(ctx, []byte("g6"), nil); err != nil || len(got) != 6 || got[5].Offset != 5 || len(got[0].Values) != 0 {
		t.Fatalf("default cap: %d rows, %v", len(got), err)
	}
	// The largest cap does not overflow.
	d3 := open(t, dir, OpenOptions{Limits: Limits{GetRows: math.MaxInt}})
	if got, err := d3.Get(ctx, []byte("g6"), nil, "size"); err != nil || len(got) != 6 {
		t.Fatalf("MaxInt cap: %d rows, %v", len(got), err)
	}
	// Invalid requests.
	if _, err := d.Get(ctx, []byte("g4"), nil, "size", "nested?"); err != nil {
		t.Fatalf("an unknown column is absent: %v", err)
	}
	must(t, d.Close())
	if _, err := d.Get(ctx, []byte("g4"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

// TestGetCapOne: a cap of one row returns a unique key's row and refuses a
// group of two.
func TestGetCapOne(t *testing.T) {
	d := open(t, getFixture(t), OpenOptions{Limits: Limits{GetRows: 1}})
	if got, err := d.Get(ctx, []byte("a"), nil, "size"); err != nil || len(got) != 1 || got[0].Values[0].Int64() != 0 {
		t.Fatalf("a: %v, %v", got, err)
	}
	if _, err := d.Get(ctx, []byte("g4"), nil); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("g4: %v", err)
	}
}

// TestNegativeLimits: every negative limit is an invalid request when the
// dataset is opened; zero takes the default.
func TestNegativeLimits(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, fixtureKeys(5)))
	fields := reflect.TypeOf(Limits{})
	for i := range fields.NumField() {
		var lim Limits
		reflect.ValueOf(&lim).Elem().Field(i).SetInt(-1)
		if _, err := OpenDir(ctx, dir, OpenOptions{Limits: lim}); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), fields.Field(i).Name) {
			t.Fatalf("%s -1: %v", fields.Field(i).Name, err)
		}
	}
	d := open(t, dir, OpenOptions{Limits: Limits{}})
	if d.r.limits.GetRows != 1000 || d.r.limits.Parts != 1_000_000 {
		t.Fatalf("defaults %+v", d.r.limits)
	}
}

// TestLimitError: a limit failure is a *LimitError naming the limit, with
// the declared value and the maximum, wrapping ErrLimitExceeded.
func TestLimitError(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, fixtureKeys(5)))
	_, err := OpenDir(ctx, dir, OpenOptions{Limits: Limits{ManifestBytes: 10}})
	var le *LimitError
	if !errors.As(err, &le) || !errors.Is(err, ErrLimitExceeded) || le.Limit != LimitManifestBytes || le.Max != 10 || le.Value <= 10 {
		t.Fatalf("%v", err)
	}
	d := open(t, dir, OpenOptions{Limits: Limits{FooterBytes: 16}})
	_, err = d.Rows(ctx, Query{Limit: 1}, nil)
	if !errors.As(err, &le) || le.Limit != LimitFooterBytes || le.Max != 16 || le.Value <= 16 {
		t.Fatalf("%v", err)
	}
}
