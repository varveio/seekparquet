// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"runtime"
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
					t.Fatal("plan accumulated locations before reserving OffsetIndexMemory")
				}
			}
		}
	}
	must(t, p.planColumns(ctx, new(ReadStats)))
	if observed == 0 || len(p.groups) < 2 {
		t.Fatal("did not observe planning between groups")
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

func TestPlanColumnsFailureReleasesLocations(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 25, PageBytes: 64}, locsKeys()))
	d := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
	p := &Plan{r: d.r, columns: []string{"size"}}
	p.union(d.r.ordinalSpans([]rowRange{{0, d.r.rows}}))
	// Model a reservation made for an earlier chunk, before a later load
	// fails. Failure must return it immediately, without waiting for GC.
	gb := groupBytes{chunks: []plannedChunk{{private: true, locs: []pageLoc{{size: 1}}}}}
	p.keep(&gb)
	if p.pin == nil {
		t.Fatal("setup did not reserve locations")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.planColumns(cctx, new(ReadStats)); !errors.Is(err, context.Canceled) {
		t.Fatalf("planning: %v", err)
	}
	if used := d.r.locsMemory.Load(); used != 0 {
		t.Fatalf("failed planning still reserves %d bytes", used)
	}
	runtime.KeepAlive(p)
}

// Private reservations must become available again while other plans on
// the same reader remain alive. In particular their cleanup owners must
// not share tiny allocations with live owners (runtime.AddCleanup).
func TestSelectRowsReleasesPrivateLocations(t *testing.T) {
	const count = 128
	locs := []pageLoc{{first: 0, size: 1}}
	size := locsSize(locs)
	r := &reader{limits: Limits{OffsetIndexMemory: count * size}}
	kept := make([]*locsPin, 0, count/2)
	for i := range count {
		p := &Plan{r: r}
		gb := groupBytes{chunks: []plannedChunk{{present: true, private: true, locs: locs}}}
		p.keep(&gb)
		p.release()
		if p.pin == nil {
			t.Fatal("private locations were not charged")
		}
		if i%2 == 0 {
			kept = append(kept, p.pin)
		}
	}
	want := int64(len(kept)) * size
	for range 100 {
		// A channel handshake gives cleanup workers an opportunity to run
		// after each GC, without sleeps or assumptions about their order.
		done := make(chan struct{})
		sentinel := &struct{ done chan struct{} }{done}
		runtime.AddCleanup(sentinel, func(ch chan struct{}) { close(ch) }, done)
		runtime.GC()
		<-done
		if r.locsMemory.Load() == want {
			break
		}
	}
	if got := r.locsMemory.Load(); got != want {
		t.Fatalf("private reservations retained %d bytes, want %d for live plans", got, want)
	}
	runtime.KeepAlive(kept)
}
