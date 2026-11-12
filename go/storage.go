// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// This file holds the optional storage capabilities a Source and its
// files may offer beyond io.ReaderAt, and the operation context the reader
// gives them: batched range reads (RangeReader), opens with the manifest's
// size (SizedSource) and a bounded whole-manifest read (ManifestReader).
// Every capability is optional: a Source with none of them
// is read as before, through Open and ReadAt.

// ReadRange requests exactly len(Buf) bytes beginning at Off.
//
// The caller allocates and owns Buf. Until ReadRanges returns, it must
// neither access Buf nor mutate the descriptor. Destination byte regions
// must not overlap, including between concurrent calls. File extents may
// overlap, touch, repeat, and appear in any order.
type ReadRange struct {
	Off int64
	Buf []byte
}

const (
	// MaxReadRanges and MaxReadBatchBytes bound one ReadRanges call: its
	// number of ranges and the sum of their lengths. They bound one
	// invocation, not an entire query.
	MaxReadRanges     = 4096
	MaxReadBatchBytes = int64(128 << 20)
)

// RangeReader is an optional capability of the original SourceFile: a
// file that reads several ranges in one call. The reader discovers it
// before applying ContextBinder. A panic in ReadRanges is recovered and
// returned as an environment error, including the recovered value.
type RangeReader interface {
	// ReadRanges fills the supplied buffers synchronously.
	//
	// Validate the entire batch before I/O: descriptor count, checked
	// sum of buffer lengths, and 0 <= Off <= Size with
	// len(Buf) <= Size-Off. Invalid arguments wrap ErrInvalidRequest.
	// A zero-length range is valid through Off == Size and needs no I/O.
	//
	// nil means every buffer was filled completely and correctly.
	// *BatchError gives one outcome per input range. Any other error
	// invalidates every range, regardless of buffer contents.
	//
	// A failed buffer contains unspecified bytes and must not be used.
	// No descriptors or buffers may be retained, accessed or modified
	// after return. No background work belonging to this call survives
	// return, including after cancellation.
	//
	// Calls are concurrency-safe. ctx governs waiting, I/O and retries;
	// it does not change the handle's identity or other calls' contexts.
	//
	// A store may merge, split, reorder and deduplicate the ranges, and
	// read a bounded amount beyond them within the object, but must keep
	// each range's own outcome, the handle's identity, and must never
	// report a range partly obtained as successful.
	ReadRanges(ctx context.Context, ranges []ReadRange) error
}

// BatchError reports the outcome of each range of a ReadRanges call, in
// input order, not completion order.
//
// Errs has exactly the input length and at least one non-nil entry. A nil
// entry guarantees that range's complete buffer is valid. Cancellation is
// recorded on unfinished ranges; completed ranges may remain successful.
// The producer does not mutate Errs after return.
type BatchError struct {
	Errs []error
}

func (e *BatchError) Error() string {
	failed := 0
	var first error
	for _, err := range e.Errs {
		if err != nil {
			if first == nil {
				first = err
			}
			failed++
		}
	}
	var b strings.Builder
	b.WriteString("seekparquet: ")
	b.WriteString(strconv.Itoa(failed))
	b.WriteString(" of ")
	b.WriteString(strconv.Itoa(len(e.Errs)))
	b.WriteString(" ranges failed")
	if first != nil {
		b.WriteString(": ")
		b.WriteString(first.Error())
	}
	return b.String()
}

// Unwrap returns the non-nil member errors.
//
// Consumers must examine Errs in semantic consumption order.
// An aggregate errors.Is or errors.As match does not identify which
// ranges failed and must not invalidate successful, needed ranges
// solely because an unused range failed.
func (e *BatchError) Unwrap() []error {
	var out []error
	for _, err := range e.Errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// SizedSource is an optional capability of a Source: it opens a part with
// the size its manifest entry declares, which permits the source to avoid
// a metadata request before the part's first read; it does not promise
// that a source can always avoid one. The reader uses it for every part of
// 12 bytes or more (the least a Parquet file holds) when the Source has
// it; a smaller part is opened with Open, which establishes its size.
type SizedSource interface {
	// OpenSized opens a part with the manifest's expected byte length.
	// expectedSize must be non-negative.
	//
	// The returned Size is expectedSize and remains stable. This value
	// alone is NOT evidence that the stored object has that size.
	//
	// Before any non-empty read can succeed, the source must establish
	// one immutable object identity and verify its actual total size.
	// It may use the first range response. If that response cannot
	// establish both, it must obtain matching metadata or fail.
	//
	// An initial actual-size disagreement returns *SizeMismatchError,
	// from OpenSized or a read. A violated previously established pin
	// returns ErrObjectChanged instead. An object found missing, by
	// OpenSized or by the first read, returns an error wrapping
	// fs.ErrNotExist.
	//
	// A zero-length read establishes nothing. OpenSized need not perform
	// I/O, including for expectedSize == 0.
	OpenSized(ctx context.Context, path string, expectedSize int64) (SourceFile, error)
}

// SizeMismatchError means the initial object does not have the manifest's
// expected size. Actual is an observed total length, never an estimate.
// It is not an ErrObjectChanged. The reader refuses the part with
// part_mismatch.
type SizeMismatchError struct {
	Path     string
	Expected int64
	Actual   int64
}

func (e *SizeMismatchError) Error() string {
	return "seekparquet: " + e.Path + " is " + strconv.FormatInt(e.Actual, 10) + " bytes, expected " + strconv.FormatInt(e.Expected, 10)
}

// ManifestReader optionally reads the complete manifest within a byte
// limit. Implementations should use one whole-object request on the
// ordinary successful remote path; retries may require more requests.
// Without it the reader opens manifest.json and reads it through ReadAt.
type ManifestReader interface {
	// ReadManifest returns the complete manifest.json and an opaque
	// identity for exactly those bytes, or no usable data on failure.
	// An empty identity means the source exposes no version identifier.
	//
	// maxBytes is a strict accepted-payload cap. Check a trustworthy
	// declared length before allocating. With unknown length, consume
	// at most maxBytes+1 bytes, using checked arithmetic, then reject
	// excess with ErrObjectTooLarge. Never allocate from an unchecked
	// response length. No silently truncated manifest is successful.
	//
	// Returned data belongs to the caller. The source must not modify
	// it or return a mutable alias of its cache.
	//
	// Missing manifests wrap fs.ErrNotExist. Implementations should use
	// one whole-object request on the successful ordinary remote path.
	ReadManifest(ctx context.Context, maxBytes int64) (data []byte, identity string, err error)
}

var (
	// ErrObjectTooLarge reports an object beyond the size a read accepts
	// (ManifestReader). The reader reports a manifest beyond
	// Limits.ManifestBytes as a *LimitError naming ManifestBytes.
	ErrObjectTooLarge = errors.New("seekparquet: object exceeds read limit")

	// ErrObjectChanged reports failure to maintain an established
	// immutable handle or source pin. It is an environment error,
	// never a RefusalError.
	ErrObjectChanged = errors.New("seekparquet: object changed")
)

// ReadPolicy controls the selection of the reader's
// logical reads. A zero field takes its default; a negative one is an
// invalid request when the dataset is opened. Windows hold up to 64 MiB
// of fetched, undecoded bytes per operation. Concurrent operations each
// hold their own windows; this is not a per-Dataset bound. Decoded pages,
// retained metadata, and results add memory outside this bound.
type ReadPolicy struct {
	// MetadataPrefetchBytes is the speculative read of a part's end when
	// the part is first checked: the footer and, when they fit, its page
	// indexes in one read. The page-index bytes it holds are kept
	// (Limits.PageIndexCacheMemory) and serve later loads of the part's
	// ColumnIndexes and OffsetIndexes, decoded and checked only when a
	// request needs them. It is clipped to the part's size; a footer larger
	// than it costs one more read of exactly the missing bytes. Default
	// 2 MiB; OpenDir's default is 64 KiB.
	MetadataPrefetchBytes int64
	// DisableSpeculation disables optional fetching, including listing
	// read-ahead, windows beyond the pages a planned scan needs, and value
	// routing probes. It does not disable batching of demanded ranges or
	// reuse of already available bytes. A part's end is then still read
	// in one window of up to 64 KiB, the footer read strategy that
	// precedes these capabilities, and only the footer is kept.
	DisableSpeculation bool
}

// check rejects a negative field.
func (p ReadPolicy) check() error {
	if p.MetadataPrefetchBytes < 0 {
		return invalidRequest("ReadPolicy.MetadataPrefetchBytes is negative (%d)", p.MetadataPrefetchBytes)
	}
	return nil
}

// tailWindow is the most bytes of a part's end its first check reads.
func (p ReadPolicy) tailWindow() int64 {
	if p.DisableSpeculation {
		return min(p.MetadataPrefetchBytes, footerWindow)
	}
	return p.MetadataPrefetchBytes
}

// withDefaults returns p with its zero fields set to their defaults.
func (p ReadPolicy) withDefaults() ReadPolicy {
	if p.MetadataPrefetchBytes == 0 {
		p.MetadataPrefetchBytes = 2 << 20
	}
	return p
}
