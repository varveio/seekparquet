// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"math/rand"
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
	for seed := int64(0); seed < 25; seed++ {
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
		for _, cap := range []int{1, 3, 1000} {
			for _, budget := range []int64{600, 3000, 1 << 24} {
				idx := open(t, dir, Options{})
				if _, err := idx.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget, MaxChangesPerPage: cap, Workers: 3}); err != nil {
					t.Fatal(err)
				}
				prefixes := []string{"", "a", "a/", "b/", "a/a", "//", "/", "\xff", "a/b/", "ab"}
				for _, pfx := range prefixes {
					for _, limit := range []int{1, 3, 1000} {
						q := List{Prefix: []byte(pfx), Delimiter: '/', Limit: limit, Columns: []string{"size"}}
						var cur *Cursor
						for page := 0; page < 400; page++ {
							q.Cursor = cur
							want, e1 := plain.List(ctx, q)
							got, e2 := idx.List(ctx, q)
							if (e1 == nil) != (e2 == nil) || !reflect.DeepEqual(want, got) {
								t.Fatalf("seed=%d cap=%d budget=%d prefix=%q limit=%d cursor=%+v err=%v/%v\nwant=%v\ngot =%v", seed, cap, budget, pfx, limit, cur, e1, e2, entriesOf(want), entriesOf(got))
							}
							if len(want) < limit {
								break
							}
							cur = want[len(want)-1].Cursor()
						}
					}
				}
			}
		}
	}
}
