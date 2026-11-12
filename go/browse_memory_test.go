// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// Observe the plan while later groups' indexes are being read, rather than
// only checking its final size: unbudgeted locations must never accumulate.
func TestPlanColumnsReservesBeforeRetaining(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 25, PageBytes: 64}, locsKeys()))
	src := &hookSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, OpenOptions{
		Limits: Limits{OffsetIndexMemory: 1},
		Read:   ReadPolicy{DisableSpeculation: true},
	})
	must(t, err)
	defer d.Close()
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	p := &Plan{r: d.r, columns: []string{"size", "name"}, inputs: []rowRange{{0, d.r.rows}}}
	p.union(d.r.ordinalSpans([]rowRange{{0, d.r.rows}}))
	observed := 0
	src.hook = func() {
		for _, gb := range p.groups {
			observed++
			for _, pc := range gb.chunks {
				if pc.locs != nil {
					t.Fatal("plan retained locations outside the Dataset cache")
				}
			}
		}
	}
	must(t, p.planColumns(ctx, new(ReadStats)))
	if observed == 0 || len(p.groups) < 2 {
		t.Fatal("did not observe planning between groups")
	}
	for _, gb := range p.groups {
		for _, pc := range gb.chunks {
			if pc.locs != nil {
				t.Fatal("final group retained unkept locations")
			}
		}
	}
	if used := d.r.locsMemory.Load(); used != 0 {
		t.Fatalf("plan reserved %d bytes of private locations", used)
	}
	if _, exact := p.ProjectedBytes(); !exact {
		t.Fatal("transient locations should price the original plan exactly")
	}
	s, err := p.SelectRows(p.RowSpans())
	must(t, err)
	if _, exact := s.ProjectedBytes(); exact {
		t.Fatal("unretained locations should make selected pricing inexact")
	}
}
