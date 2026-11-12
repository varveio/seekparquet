// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"runtime"
	"testing"
)

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
