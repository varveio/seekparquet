// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
)

// This file is how the reader reaches storage: the classification of a
// source's errors (sourceError), the collector of an operation's physical
// requests (collector), the bound on an operation's concurrent reads
// (opLimit) and the one function that issues a set of ranges together
// (readRanges).

// storageError marks an error the source itself returned from a read, so
// that a failure to read storage is never mistaken for corrupt data
// (CHECK-5): it is reported as an error, not a refusal.
type storageError struct{ err error }

func (e *storageError) Error() string { return e.err.Error() }
func (e *storageError) Unwrap() error { return e.err }

// sourceError classifies an error a source returned from a read of the
// part (or manifest) name, at every part-read boundary: an object found
// to differ in size from its manifest entry when first read is refused
// with part_mismatch, one found missing with missing_part; a broken pin
// (ErrObjectChanged) and every other error are the environment's.
func sourceError(name string, err error) error {
	var mismatch *SizeMismatchError
	switch {
	case errors.Is(err, ErrObjectChanged):
		return &storageError{err}
	case errors.As(err, &mismatch):
		return refusePart(CodePartMismatch, name, -1, "the object is %d bytes, its manifest entry %d", mismatch.Actual, mismatch.Expected)
	case errors.Is(err, fs.ErrNotExist):
		return refusePart(CodeMissingPart, name, -1, "%v", err)
	}
	return &storageError{err}
}

// storageReader classifies the errors of a source's ReadAt (sourceError)
// and counts its calls in st. io.EOF is left unmarked: every location the
// reader follows is first checked to lie inside the file, so reading past
// its end means the data lied.
//
// Reads call its readFull method, so that it is not boxed into an
// io.ReaderAt and allocated for every read. name names the file in
// errors, and unreported is set when the file leaves its physical requests
// unreported (IOReporter), each call then counting in UnreportedIOCalls.
type storageReader struct {
	r          io.ReaderAt
	st         *ReadStats // counts the reads of a request, nil for none
	name       string
	unreported bool
}

func (s storageReader) ReadAt(p []byte, off int64) (int, error) {
	if s.st != nil {
		s.st.SourceReads++
		s.st.SourceBytes += int64(len(p))
		if s.unreported {
			s.st.UnreportedIOCalls++
		}
	}
	n, err := s.r.ReadAt(p, off)
	switch {
	case err != nil && !onlyEOF(err):
		err = sourceError(s.name, err)
	case n < len(p):
		// Every extent the reader reads lies inside the file's Size: a
		// short read is the source's failure, not the data's.
		err = shortRead(s.name, n, len(p), off, err)
	default:
		err = nil
	}
	return n, err
}

// onlyEOF reports whether err says no more than io.EOF: io.EOF, or an
// error whose every cause is io.EOF. An error that also carries anything
// else (a pin broken or a size found different, the context's end, any
// failure of the source) is the source's error even when the read was
// complete: ReadAt may return io.EOF with a full read, but nothing else.
func onlyEOF(err error) bool {
	if err == io.EOF {
		return true
	}
	var mismatch *SizeMismatchError
	if errors.Is(err, ErrObjectChanged) || errors.As(err, &mismatch) {
		return false
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		causes := e.Unwrap()
		for _, c := range causes {
			if !onlyEOF(c) {
				return false
			}
		}
		return len(causes) > 0
	case interface{ Unwrap() error }:
		return onlyEOF(e.Unwrap())
	}
	return false
}

// readFull is readFull of s without boxing s into an io.ReaderAt, which
// would allocate for every page read.
// ReadAt returns no io.EOF: a whole read has no error, a short one the
// environment's.
func (s storageReader) readFull(p []byte, off int64) error {
	n, err := s.ReadAt(p, off)
	switch {
	case err != nil:
		return err
	case n == len(p):
		return nil
	}
	return io.ErrUnexpectedEOF
}

// shortRead is the environment's error of a read of want bytes at off
// that returned n, with io.EOF or no error.
func shortRead(name string, n, want int, off int64, cause error) error {
	if cause == nil {
		cause = io.ErrUnexpectedEOF
	}
	return &storageError{fmt.Errorf("%s: %d of %d bytes at %d read: %w (%w)", name, n, want, off, io.ErrUnexpectedEOF, cause)}
}

// reportsIO reports whether x promises to report its physical requests.
func reportsIO(x any) bool {
	r, ok := x.(IOReporter)
	return ok && r.ReportsIO()
}

// collector is an operation's IOObserver: it counts the physical requests
// reporting sources and files make for the operation, in aligned atomic
// counters, since a source may report from its own goroutines, and passes
// every event on to the observer the caller set, if any. addTo adds its
// counts to the operation's ReadStats once the operation's reads have
// ended.
type collector struct {
	parent IOObserver

	requests, bytes, retries, failures      atomic.Int64
	plannedRanges, plannedRequests, planned atomic.Int64
	cacheHits, inFlight, peak               atomic.Int64
}

// withCollector returns ctx with a new collector for an operation.
func withCollector(ctx context.Context) (context.Context, *collector) {
	c := &collector{parent: IOObserverFromContext(ctx)}
	return WithIOObserver(ctx, c), c
}

// addTo adds the collector's counts to st.
func (c *collector) addTo(st *ReadStats) {
	if c == nil || st == nil {
		return
	}
	st.PhysicalRequests += c.requests.Load()
	st.PhysicalBytes += c.bytes.Load()
	st.PhysicalRetries += c.retries.Load()
	st.PhysicalFailures += c.failures.Load()
	st.PlannedRanges += c.plannedRanges.Load()
	st.PlannedRequests += c.plannedRequests.Load()
	st.PlannedOverReadBytes += c.planned.Load()
	st.ByteCacheHitBytes += c.cacheHits.Load()
	st.PeakPhysicalRequests = max(st.PeakPhysicalRequests, c.peak.Load())
}

func (c *collector) Begin(req IORequest) func(IOResult) {
	c.requests.Add(1)
	if req.Retry {
		c.retries.Add(1)
	}
	n := c.inFlight.Add(1)
	for {
		peak := c.peak.Load()
		if n <= peak || c.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	var done func(IOResult)
	if c.parent != nil {
		done = c.parent.Begin(req)
	}
	return func(res IOResult) {
		c.inFlight.Add(-1)
		c.bytes.Add(res.Bytes)
		if res.Err != nil {
			c.failures.Add(1)
		}
		if done != nil {
			done(res)
		}
	}
}

func (c *collector) CacheHit(bytes int64) {
	c.cacheHits.Add(bytes)
	if c.parent != nil {
		c.parent.CacheHit(bytes)
	}
}

func (c *collector) BatchPlanned(ranges, requests, overReadBytes int64) {
	c.plannedRanges.Add(ranges)
	c.plannedRequests.Add(requests)
	c.planned.Add(overReadBytes)
	if c.parent != nil {
		c.parent.BatchPlanned(ranges, requests, overReadBytes)
	}
}

// checkCacheMode rejects an unknown cache mode before any I/O.
func checkCacheMode(ctx context.Context) error {
	if mode := CacheModeFromContext(ctx); mode > CacheBypass {
		return invalidRequest("unknown cache mode %d", mode)
	}
	return nil
}

// opLimit bounds the logical ranges an operation has outstanding at once
// (ReadPolicy.Concurrency) when it reads from several goroutines (Verify):
// a weighted semaphore whose waiters are served in arrival order and can
// give up when their context ends.
type opLimit struct {
	mu      sync.Mutex
	free    int
	waiters []*opWaiter
}

type opWaiter struct {
	n     int
	ready chan struct{}
}

type opLimitKey struct{}

// withOpLimit returns ctx carrying a limit of n outstanding ranges for the
// operation's concurrent reads.
func withOpLimit(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, opLimitKey{}, &opLimit{free: n})
}

func opLimitOf(ctx context.Context) *opLimit {
	l, _ := ctx.Value(opLimitKey{}).(*opLimit)
	return l
}

// acquire takes n slots, n at most the limit's size.
func (l *opLimit) acquire(ctx context.Context, n int) error {
	l.mu.Lock()
	if len(l.waiters) == 0 && l.free >= n {
		l.free -= n
		l.mu.Unlock()
		return nil
	}
	w := &opWaiter{n: n, ready: make(chan struct{})}
	l.waiters = append(l.waiters, w)
	l.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		defer l.mu.Unlock()
		select {
		case <-w.ready: // granted meanwhile: give the slots back
			l.free += n
			l.grant()
		default:
			for i, q := range l.waiters {
				if q == w {
					l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
					break
				}
			}
			l.grant() // the next waiter may now fit
		}
		return ctx.Err()
	}
}

func (l *opLimit) release(n int) {
	l.mu.Lock()
	l.free += n
	l.grant()
	l.mu.Unlock()
}

// grant wakes the waiters at the front that fit, with mu held.
func (l *opLimit) grant() {
	for len(l.waiters) > 0 && l.waiters[0].n <= l.free {
		w := l.waiters[0]
		l.waiters = l.waiters[1:]
		l.free -= w.n
		close(w.ready)
	}
}

// rangeFile is a file read by readRanges: the source's file, its name for
// errors, whether it reports its physical requests, and the most ranges it
// reads at once.
type rangeFile struct {
	file        io.ReaderAt
	name        string
	reports     bool
	concurrency int
}

// readRanges reads every range of rf under ctx and returns nil, or one
// error per range, nil where its buffer was filled. It is the one way the
// reader issues independent reads together: as ReadRanges calls when the
// file is a RangeReader (at most Concurrency ranges, MaxReadRanges and
// MaxReadBatchBytes per call), and otherwise as concurrent ReadAt calls on
// ContextBinder views by at most Concurrency workers, never one goroutine
// per range. A single range, or a Concurrency of 1, is read on the
// caller's goroutine. When ctx carries an operation's limit (opLimit), its
// ranges count against it. Errors are classified as every read's are
// (sourceError, shortRead). Every read it starts has ended when it
// returns. Counts reach st, on the caller's goroutine, only for the calls
// actually made.
func readRanges(ctx context.Context, rf rangeFile, ranges []ReadRange, st *ReadStats) []error {
	var todo []int
	for i, rg := range ranges {
		if len(rg.Buf) > 0 {
			todo = append(todo, i)
		}
	}
	if len(todo) == 0 {
		return nil
	}
	width := max(1, rf.concurrency)
	limit := opLimitOf(ctx)
	uncounted := unreported(ctx, rf.reports)
	if rr, ok := rf.file.(RangeReader); ok {
		return readBatches(ctx, rf, rr, ranges, todo, width, limit, uncounted, st)
	}
	errs := make([]error, len(ranges))
	var failed atomic.Bool
	var calls, bytes atomic.Int64
	at := storageReader{r: bind(ctx, rf.file), name: rf.name}
	read := func(i int) {
		if limit != nil {
			if err := limit.acquire(ctx, 1); err != nil {
				errs[i] = err
				failed.Store(true)
				return
			}
			defer limit.release(1)
		}
		if err := ctx.Err(); err != nil {
			errs[i] = err
			failed.Store(true)
			return
		}
		calls.Add(1)
		bytes.Add(int64(len(ranges[i].Buf)))
		if err := at.readFull(ranges[i].Buf, ranges[i].Off); err != nil {
			errs[i] = err
			failed.Store(true)
		}
	}
	if workers := min(width, len(todo)); workers <= 1 {
		for _, i := range todo {
			read(i)
		}
	} else {
		var next atomic.Int64
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for {
					j := next.Add(1) - 1
					if j >= int64(len(todo)) {
						return
					}
					read(todo[j])
				}
			})
		}
		wg.Wait()
	}
	if st != nil {
		n := calls.Load()
		st.SourceReads += n
		st.SourceBytes += bytes.Load()
		st.ScalarFallbackReads += n
		if uncounted {
			st.UnreportedIOCalls += n
		}
	}
	if !failed.Load() {
		return nil
	}
	return errs
}

// piece is part of one input range in a ReadRanges call: ranges beyond
// MaxReadBatchBytes are read in pieces.
type piece struct {
	of int
	rg ReadRange
}

// readBatches reads ranges through a RangeReader, in calls of at most
// width ranges and MaxReadBatchBytes bytes.
func readBatches(ctx context.Context, rf rangeFile, rr RangeReader, ranges []ReadRange, todo []int, width int, limit *opLimit, uncounted bool, st *ReadStats) []error {
	var pieces []piece
	for _, i := range todo {
		buf, off := ranges[i].Buf, ranges[i].Off
		for len(buf) > 0 {
			n := int(min(int64(len(buf)), MaxReadBatchBytes))
			pieces = append(pieces, piece{i, ReadRange{Off: off, Buf: buf[:n:n]}})
			buf, off = buf[n:], off+int64(n)
		}
	}
	width = min(width, MaxReadRanges)
	errs := make([]error, len(ranges))
	failed := false
	fail := func(of int, err error) {
		if errs[of] == nil {
			errs[of] = err
		}
		failed = true
	}
	for len(pieces) > 0 {
		n, bytes := 0, int64(0)
		for n < len(pieces) && n < width && (n == 0 || bytes+int64(len(pieces[n].rg.Buf)) <= MaxReadBatchBytes) {
			bytes += int64(len(pieces[n].rg.Buf))
			n++
		}
		batch := make([]ReadRange, n)
		for j := range batch {
			batch[j] = pieces[j].rg
		}
		err := ctx.Err()
		if err == nil && limit != nil {
			err = limit.acquire(ctx, n)
		}
		if err == nil {
			err = rr.ReadRanges(ctx, batch)
			if limit != nil {
				limit.release(n)
			}
			if st != nil {
				st.SourceBatches++
				st.SourceReads += int64(n)
				st.SourceBytes += bytes
				if uncounted {
					st.UnreportedIOCalls++
				}
			}
		}
		var be *BatchError
		switch {
		case err == nil:
		case errors.As(err, &be) && len(be.Errs) == n:
			for j, e := range be.Errs {
				if e != nil {
					fail(pieces[j].of, batchError(ctx, rf.name, e))
				}
			}
		default:
			for j := range n {
				fail(pieces[j].of, batchError(ctx, rf.name, err))
			}
		}
		pieces = pieces[n:]
	}
	if !failed {
		return nil
	}
	return errs
}

// batchError classifies one range's error from ReadRanges.
// The source's own error is kept, a broken pin first: a short or ended
// range is the environment's error, like a short scalar read.
func batchError(ctx context.Context, name string, err error) error {
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return err
	}
	return sourceError(name, err)
}

// retained is the raw page-index bytes of a part's end, kept from its
// check: data holds the file's bytes from off.
type retained struct {
	off  int64
	data []byte
	// used is when a load last used the bytes, on the metaCache's clock.
	used atomic.Int64
}

// retainTail keeps, from the end of part p's file that its check read,
// the bytes of its page-index region the tail holds: from the first
// ColumnIndex or OffsetIndex the footer locates (or the tail's start, when
// an index begins before it) to the footer. They are charged to
// Limits.PageIndexCacheMemory before they are copied, evicting the parts
// used least recently, or not kept when they cannot fit, and copied, so
// that the rest of the tail is not pinned. Nothing is decoded or checked
// here: an index is decoded when a request first needs it, so an index no
// request uses never causes a refusal. With DisableSpeculation, or as
// OpenDir reads a local file, nothing is kept.
func (r *reader) retainTail(p *part, tail footerTail) {
	if r.policy.DisableSpeculation || r.local || len(tail.data) == 0 || tail.footerStart <= tail.off {
		return
	}
	start := tail.footerStart
	for _, rg := range p.footer.Metadata().RowGroups {
		for _, c := range rg.Columns {
			for _, off := range [2]int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if off > 0 && off < start {
					start = off
				}
			}
		}
	}
	start = max(start, tail.off)
	size := tail.footerStart - start
	if size <= 0 || size > r.limits.PageIndexCacheMemory {
		return
	}
	c := r.metaCache()
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.memory+size > r.limits.PageIndexCacheMemory {
		lru := 0
		for i, q := range c.parts {
			if q.meta.Load().used.Load() < c.parts[lru].meta.Load().used.Load() {
				lru = i
			}
		}
		q := c.parts[lru]
		c.memory -= int64(len(q.meta.Load().data))
		q.meta.Store(nil)
		c.parts = append(c.parts[:lru], c.parts[lru+1:]...)
	}
	c.memory += size // reserved before the copy is allocated
	kept := &retained{off: start, data: ownCopy(tail.data[start-tail.off : tail.footerStart-tail.off])}
	kept.used.Store(c.clock.Add(1))
	p.meta.Store(kept)
	c.parts = append(c.parts, p)
}

// metaCache accounts for the parts' retained metadata: mu guards parts,
// those that hold some, and memory, their bytes within
// Limits.PageIndexCacheMemory; clock orders their uses for eviction.
type metaCache struct {
	mu     sync.Mutex
	parts  []*part
	memory int64
	clock  atomic.Int64
}

// metaCache returns the reader's metaCache, made by the first part that
// keeps metadata: a reader that keeps none, as a local one, never has one.
func (r *reader) metaCache() *metaCache {
	if c := r.meta.Load(); c != nil {
		return c
	}
	r.meta.CompareAndSwap(nil, new(metaCache))
	return r.meta.Load()
}

// retainedBytes returns the length bytes at off from the part's retained
// metadata, or nil when they are not all kept. The bytes are shared and
// read-only.
func (p *part) retainedBytes(off, length int64) []byte {
	m := p.meta.Load()
	if m == nil || length <= 0 || off < m.off || length > int64(len(m.data)) || off-m.off > int64(len(m.data))-length {
		return nil
	}
	m.used.Store(p.r.meta.Load().clock.Add(1))
	return m.data[off-m.off : off-m.off+length : off-m.off+length]
}

// retainedSuffix returns how many of the length bytes at off, a suffix of
// them, the part's retained metadata holds, and those bytes: zero when it
// holds none of them, or all of them (retainedBytes).
func (p *part) retainedSuffix(off, length int64) (int64, []byte) {
	m := p.meta.Load()
	if m == nil || length <= 0 || off < 0 || off >= m.off || length > m.off-off+int64(len(m.data)) {
		return 0, nil
	}
	n := off + length - m.off
	if n <= 0 || n >= length {
		return 0, nil
	}
	m.used.Store(p.r.meta.Load().clock.Add(1))
	return n, m.data[:n]
}

// width is the most ranges the reader issues together: one when it reads
// as suits a local file.
func (r *reader) width() int {
	if r.local {
		return 1
	}
	return r.policy.Concurrency
}

// collected reports whether ctx carries an operation's collector.
func collected(ctx context.Context) bool {
	_, ok := IOObserverFromContext(ctx).(*collector)
	return ok
}

// unreported reports whether a storage call under ctx on a receiver that
// reports (or not) its requests leaves them out of the operation's counts:
// it does not report them, or no collector counts them.
func unreported(ctx context.Context, reports bool) bool {
	return !reports || !collected(ctx)
}

// bypassCache returns ctx asking a Source's byte cache to be bypassed
// (CacheBypass): scans and list-index builds read each page once, and
// would only evict what interactive reads reuse. An unknown mode the
// caller set is kept, to be rejected.
func bypassCache(ctx context.Context) context.Context {
	if CacheModeFromContext(ctx) != CacheDefault {
		return ctx
	}
	return WithCacheMode(ctx, CacheBypass)
}
