// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "github.com/parquet-go/parquet-go"

// pageScratch belongs to one projection slot in one request/Scan, or to the
// key column of one ScanBatches/RangeBatches call (batcher.keys), and survives
// chunk changes. It is never shared by concurrent calls. Key pages use it only
// on that batch path; every other key walker decodes each page into storage
// of its own. Dictionaries use independent storage retained by their chunk,
// including when loaded lazily while an encoded data page is live in raw/plain.
//
// Byte buffers follow PageBytes, plus header reads up to headerBytes (16 MiB,
// bounded by MetadataMemory); indexed raw extents include both. Element
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

// decodedBuffer keeps the larger allocation: encodings append into the supplied
// destination and replace it only when they need more capacity.
func decodedBuffer[T any](previous, decoded []T) []T {
	if cap(decoded) > cap(previous) {
		return decoded
	}
	return previous
}

// local supplies ephemeral buffers for the non-reusing key/dictionary path.
// Without scratch, every page decoded gets storage of its own.
func (s *pageScratch) local() *pageScratch {
	if s == nil {
		return new(pageScratch)
	}
	return s
}

func (s *pageScratch) clearValues() {
	if s != nil {
		clear(s.values)
		clear(s.out)
	}
}
