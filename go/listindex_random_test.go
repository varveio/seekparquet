// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

func listIndexRandomKeys(rng *rand.Rand, n int) []string {
	alpha := []string{"a", "b", "/", "a/", "b/", "\xff", "c", "//", "ab"}
	set := map[string]bool{}
	for len(set) < n {
		l := 1 + rng.Intn(6)
		s := ""
		for i := 0; i < l; i++ {
			s += alpha[rng.Intn(len(alpha))]
		}
		set[s] = true
	}
	out := make([]string, 0, n)
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestListIndexRandomizedDifferential(t *testing.T) {
	// Keep all 25 seeds, with at most two fixtures and index builds active.
	// Seed subtests own their files and handles, releasing both after each seed.
	for worker := range 2 {
		t.Run(fmt.Sprintf("worker-%d", worker), func(t *testing.T) {
			t.Parallel()
			for seed := int64(0); seed < 25; seed++ {
				// Alternate pairs so each worker exercises both unique and repeated keys.
				if (seed/2)%2 != int64(worker) {
					continue
				}
				t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
					testListIndexRandomSeed(t, seed)
				})
			}
		})
	}
}

func testListIndexRandomSeed(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	keys := listIndexRandomKeys(rng, 200+rng.Intn(300))
	repeated := seed%2 == 1
	if repeated {
		var more []string
		for _, k := range keys {
			for r := rng.Intn(3); r >= 0; r-- {
				more = append(more, k)
			}
		}
		keys = more
	}
	// split into parts at distinct-key boundaries
	var ps [][]string
	cut := len(keys) / 3
	for cut < len(keys) && keys[cut] == keys[cut-1] {
		cut++
	}
	ps = append(ps, keys[:cut], keys[cut:])
	geo := testutil.Part{GroupRows: 40 + rng.Intn(60), PageBytes: 60 + rng.Intn(200), IndexSizeLimit: 8}
	fixture := parts(geo, ps...)
	fixture.RepeatedKeys = repeated
	dir := write(t, fixture)
	plain := open(t, dir, Options{})
	var cases []listIndexCase
	prefixes := []string{"", "a", "a/", "b/", "a/a", "//", "/", "\xff", "a/b/", "ab"}
	for _, pfx := range prefixes {
		for _, limit := range []int{1, 3, 1000} {
			q := List{Prefix: []byte(pfx), Delimiter: '/', Limit: limit, Columns: []string{"size"}}
			for page := 0; page < 400; page++ {
				want, err := plain.List(ctx, q)
				cases = append(cases, listIndexCase{query: q, want: want, err: err})
				if len(want) < limit {
					break
				}
				q.Cursor = want[len(want)-1].Cursor()
			}
		}
	}
	for _, cap := range []int{1, 3, 1000} {
		for _, budget := range []int64{600, 3000, 1 << 24} {
			t.Run(fmt.Sprintf("cap-%d/budget-%d", cap, budget), func(t *testing.T) {
				idx := open(t, dir, Options{})
				if _, err := idx.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget, MaxChangesPerPage: cap, Workers: 3}); err != nil {
					t.Fatal(err)
				}
				for _, tc := range cases {
					got, err := idx.List(ctx, tc.query)
					if (tc.err == nil) != (err == nil) || !reflect.DeepEqual(tc.want, got) {
						t.Fatalf("seed=%d cap=%d budget=%d prefix=%q limit=%d cursor=%+v err=%v/%v\nwant=%v\ngot =%v", seed, cap, budget, tc.query.Prefix, tc.query.Limit, tc.query.Cursor, tc.err, err, entriesOf(tc.want), entriesOf(got))
					}
				}
			})
		}
	}
}

// TestListIndexExportRandomFixtures supplies identical Parquet bytes to the two
// conformance adapters for the optional cross-language differential harness.
func TestListIndexExportRandomFixtures(t *testing.T) {
	root := os.Getenv("SEEKPARQUET_LIST_RANDOM_FIXTURES")
	if root == "" {
		t.Skip("set SEEKPARQUET_LIST_RANDOM_FIXTURES to export shared random fixtures")
	}
	for seed := int64(0); seed < 8; seed++ {
		rng := rand.New(rand.NewSource(seed))
		keys := listIndexRandomKeys(rng, 300)
		for i := 0; i < 20; i++ {
			keys = append(keys, fmt.Sprintf("wide/d%03d/", i))
			for j := 0; j < 30; j++ {
				keys = append(keys, fmt.Sprintf("wide/d%03d/f%03d", i, j))
			}
		}
		slices.Sort(keys)
		if seed%2 == 1 {
			var repeated []string
			for _, key := range keys {
				for n := rng.Intn(3); n >= 0; n-- {
					repeated = append(repeated, key)
				}
			}
			keys = repeated
		}
		cut := len(keys) / 2
		for cut < len(keys) && keys[cut] == keys[cut-1] {
			cut++
		}
		fixture := parts(testutil.Part{GroupRows: 80, PageBytes: 80 + rng.Intn(100), IndexSizeLimit: 8}, keys[:cut], keys[cut:])
		fixture.RepeatedKeys = seed%2 == 1
		dir := filepath.Join(root, fmt.Sprintf("seed-%02d", seed))
		must(t, testutil.Write(dir, fixture))
		d := open(t, dir, Options{})
		st, err := d.BuildListIndex(ctx, ListIndexOptions{})
		must(t, err)
		if !st.HasIndex {
			t.Fatal("fixture must exercise indexed groups", st)
		}
		encoded := make([][]byte, len(keys))
		for i, key := range keys {
			encoded[i] = []byte(key)
		}
		metadata, err := json.Marshal(encoded)
		must(t, err)
		must(t, os.WriteFile(filepath.Join(dir, "keys.json"), metadata, 0600))
	}
}
