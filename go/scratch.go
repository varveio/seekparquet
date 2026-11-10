// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "github.com/parquet-go/parquet-go"

// pageScratch belongs to one projection slot in one request/Scan, and survives
// chunk changes. It is never shared by concurrent calls or used for key pages.
// Dictionaries use independent storage retained by their chunk, including when
// loaded lazily while an encoded data page is live in raw/plain.
//
// Byte buffers follow PageBytes (raw also includes the bounded header); element
// buffers follow PageValues. Both compact and optional expanded Values are
// retained at their high-water sizes until the call ends, along with codec state.
// This transient memory is separate from retained indexes and multiplies with
// active calls, not just the shards of any one plan.
type pageScratch struct {
	raw, plain, levels, data []byte
	indexes                  []int32
	offsets                  []uint32
	values, out              []parquet.Value
	codec                    codecScratch
}

// resize retains the allocation without geometric growth. Callers check the
// relevant page byte/value limits before requesting space.
func resize[T any](buf []T, n int) []T {
	// Preserve make([]T, 0) semantics: parquet-go numeric dictionary lookups
	// compute field pointers even for an empty output, and require a non-nil base.
	if buf == nil || cap(buf) < n {
		return make([]T, n)
	}
	return buf[:n]
}

func (s *pageScratch) readBuffer(n int) []byte {
	if s == nil {
		return make([]byte, n)
	}
	s.raw = resize(s.raw, n)
	return s.raw
}

// decodedBuffer remembers a decoder's replacement allocation, or restores the
// retained capacity when it returned the capped destination we supplied.
func decodedBuffer[T any](previous, decoded []T) []T {
	// A zero-byte decode may return a zero-capacity view. There is no data to
	// preserve in that view; keep the larger allocation for the next page.
	if len(decoded) == 0 && cap(decoded) <= cap(previous) {
		return previous[:0]
	}
	if cap(previous) > 0 && cap(decoded) > 0 && &previous[:cap(previous)][0] == &decoded[:cap(decoded)][0] {
		return previous[:len(decoded)]
	}
	return decoded
}

func (s *pageScratch) clearValues() {
	if s != nil {
		clear(s.values)
		clear(s.out)
	}
}
