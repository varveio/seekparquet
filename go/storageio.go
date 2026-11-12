// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
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
// It stays three words, so that wrapping a read costs no more than before
// it classified errors: what it knows of the file is behind src.
type storageReader struct {
	r   io.ReaderAt
	st  *ReadStats // counts the reads of a request, nil for none
	src *sourceInfo
}

// sourceInfo is what classifying a file's reads needs: its name for
// errors, and whether it leaves its physical requests unreported
// (IOReporter), each call then counting in UnreportedIOCalls.
type sourceInfo struct {
	name       string
	unreported bool
}

func (s storageReader) ReadAt(p []byte, off int64) (int, error) {
	if s.st != nil {
		s.st.SourceReads++
		s.st.SourceBytes += int64(len(p))
		if s.src.unreported {
			s.st.UnreportedIOCalls++
		}
	}
	n, err := s.r.ReadAt(p, off)
	if err != nil && !errors.Is(err, io.EOF) {
		err = sourceError(s.src.name, err)
	}
	return n, err
}

// reportsIO reports whether x promises to report its physical requests.
func reportsIO(x any) bool {
	r, ok := x.(IOReporter)
	return ok && r.ReportsIO()
}

// collector is an operation's IOObserver: it counts the physical requests
// a reporting source makes for the operation into its ReadStats, with
// atomic adds, since a source may report from its own goroutines, and
// passes every event on to the observer the caller set, if any.
type collector struct {
	st       *ReadStats
	parent   IOObserver
	inFlight atomic.Int64
}

// observing returns ctx with an operation's collector for st, when the
// source of the operation reports its physical requests.
func observing(ctx context.Context, st *ReadStats, reports bool) context.Context {
	if !reports || st == nil {
		return ctx
	}
	if c, ok := IOObserverFromContext(ctx).(*collector); ok && c.st == st {
		return ctx
	}
	return WithIOObserver(ctx, &collector{st: st, parent: IOObserverFromContext(ctx)})
}

func (c *collector) Begin(req IORequest) func(IOResult) {
	atomic.AddInt64(&c.st.PhysicalRequests, 1)
	if req.Retry {
		atomic.AddInt64(&c.st.PhysicalRetries, 1)
	}
	n := c.inFlight.Add(1)
	for {
		peak := atomic.LoadInt64(&c.st.PeakPhysicalRequests)
		if n <= peak || atomic.CompareAndSwapInt64(&c.st.PeakPhysicalRequests, peak, n) {
			break
		}
	}
	var done func(IOResult)
	if c.parent != nil {
		done = c.parent.Begin(req)
	}
	return func(res IOResult) {
		c.inFlight.Add(-1)
		atomic.AddInt64(&c.st.PhysicalBytes, res.Bytes)
		if res.Err != nil {
			atomic.AddInt64(&c.st.PhysicalFailures, 1)
		}
		if done != nil {
			done(res)
		}
	}
}

func (c *collector) CacheHit(bytes int64) {
	atomic.AddInt64(&c.st.ByteCacheHitBytes, bytes)
	if c.parent != nil {
		c.parent.CacheHit(bytes)
	}
}

func (c *collector) BatchPlanned(ranges, requests, overReadBytes int64) {
	atomic.AddInt64(&c.st.PlannedRanges, ranges)
	atomic.AddInt64(&c.st.PlannedRequests, requests)
	atomic.AddInt64(&c.st.PlannedOverReadBytes, overReadBytes)
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
// errors, whether it reports its physical requests, and the policy's
// bound on the ranges it reads at once.
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
// per range. A single range is read on the caller's goroutine. When ctx
// carries an operation's limit (opLimit), its ranges count against it.
// Errors are classified as every read's are (sourceError); a range that
// ends with io.EOF, or short, is io.ErrUnexpectedEOF. Every read it
// starts has ended when it returns. Counts go to st on the caller's
// goroutine.
func readRanges(ctx context.Context, rf rangeFile, ranges []ReadRange, st *ReadStats) []error {
	var todo []int
	for i, rg := range ranges {
		if len(rg.Buf) > 0 {
			todo = append(todo, i)
			if st != nil {
				st.SourceReads++
				st.SourceBytes += int64(len(rg.Buf))
			}
		}
	}
	if len(todo) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		errs := make([]error, len(ranges))
		for _, i := range todo {
			errs[i] = err
		}
		return errs
	}
	width := max(1, rf.concurrency)
	limit := opLimitOf(ctx)
	if rr, ok := rf.file.(RangeReader); ok {
		return readBatches(ctx, rf, rr, ranges, todo, width, limit, st)
	}
	if st != nil {
		st.ScalarFallbackReads += int64(len(todo))
		if !rf.reports {
			st.UnreportedIOCalls += int64(len(todo))
		}
	}
	errs := make([]error, len(ranges))
	failed := atomic.Bool{}
	at := bind(ctx, rf.file)
	src := &sourceInfo{name: rf.name}
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
		if err := readFull(storageReader{r: at, src: src}, ranges[i].Buf, ranges[i].Off); err != nil {
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
func readBatches(ctx context.Context, rf rangeFile, rr RangeReader, ranges []ReadRange, todo []int, width int, limit *opLimit, st *ReadStats) []error {
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
		var err error
		if limit != nil {
			if err = limit.acquire(ctx, n); err == nil {
				err = rr.ReadRanges(ctx, batch)
				limit.release(n)
			}
		} else if err = ctx.Err(); err == nil {
			err = rr.ReadRanges(ctx, batch)
		}
		if st != nil {
			st.SourceBatches++
			if !rf.reports {
				st.UnreportedIOCalls++
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
func batchError(ctx context.Context, name string, err error) error {
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return err
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return io.ErrUnexpectedEOF
	}
	return sourceError(name, err)
}

// retained is the raw page-index bytes of a part's end, kept from its
// check: data holds the file's bytes from off.
type retained struct {
	off  int64
	data []byte
}

// retainTail keeps, from the end of part p's file that its check read,
// the bytes of its page-index region: from the first ColumnIndex or
// OffsetIndex the footer locates within the tail, to the footer. They are
// copied, so that the rest of the tail is not pinned, and charged to
// Limits.MetadataCacheMemory, evicting the parts used least recently, or
// not kept when they cannot fit. Nothing is decoded or checked here: an
// index is decoded when a request first needs it, so an index no request
// uses never causes a refusal. With DisableSpeculation nothing is kept.
func (r *reader) retainTail(p *part, tail footerTail) {
	if r.policy.DisableSpeculation || len(tail.data) == 0 || tail.footerStart <= tail.off {
		return
	}
	start := tail.footerStart
	for _, rg := range p.footer.Metadata().RowGroups {
		for _, c := range rg.Columns {
			for _, off := range [2]int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if off >= tail.off && off < start {
					start = off
				}
			}
		}
	}
	size := tail.footerStart - start
	if size <= 0 || size > r.limits.MetadataCacheMemory {
		return
	}
	kept := &retained{off: start, data: ownCopy(tail.data[start-tail.off : tail.footerStart-tail.off])}
	r.metaMu.Lock()
	defer r.metaMu.Unlock()
	for r.metaMemory+size > r.limits.MetadataCacheMemory {
		lru := 0
		for i, q := range r.metaParts {
			if q.metaUsed.Load() < r.metaParts[lru].metaUsed.Load() {
				lru = i
			}
		}
		q := r.metaParts[lru]
		r.metaMemory -= int64(len(q.meta.Load().data))
		q.meta.Store(nil)
		r.metaParts = append(r.metaParts[:lru], r.metaParts[lru+1:]...)
	}
	r.metaMemory += size
	p.metaUsed.Store(r.metaClock.Add(1))
	p.meta.Store(kept)
	r.metaParts = append(r.metaParts, p)
}

// retainedBytes returns the length bytes at off from the part's retained
// metadata, or nil when they are not all kept. The bytes are shared and
// read-only.
func (p *part) retainedBytes(off, length int64) []byte {
	m := p.meta.Load()
	if m == nil || off < m.off || length > int64(len(m.data)) || off-m.off > int64(len(m.data))-length {
		return nil
	}
	p.metaUsed.Store(p.r.metaClock.Add(1))
	return m.data[off-m.off : off-m.off+length : off-m.off+length]
}
