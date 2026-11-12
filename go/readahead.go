// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// maxReadAhead bounds ListQuery.ReadAhead: larger values are read as it.
const maxReadAhead = 64

// readAheadPageBytes is the byte budget of each read-ahead slot: a listing
// with ReadAhead n holds at most n times this many bytes read ahead and not
// yet used, in flight or fetched. A key page larger than the whole budget
// is never read ahead; it is read when the scan reaches it.
const readAheadPageBytes = 1 << 20

// testListReadAhead, when positive, is the read-ahead of a listing that
// asks for none. Tests set it to run every listing with read-ahead; it is
// zero otherwise.
var testListReadAhead int

// readAhead is one List request's read-ahead of key pages (ListQuery.ReadAhead).
// The request's scan predicts, from the checked key page index of the row
// group it is in, the key pages it will decode next, and readAhead reads
// their raw bytes (header and payload, the extent their OffsetIndex entry
// gives) concurrently, at most slots reads in flight and at most budget
// bytes held. When the scan decodes a page, the page read path takes the
// bytes read ahead for exactly that extent instead of reading it, and
// checks them exactly as it checks bytes it reads itself (CHECK-2):
// nothing is trusted because it was read ahead. A read ahead that failed
// or was cut short is not used; the page is read again as if it had not
// been read ahead. A prediction is a hint: a page predicted wrongly costs
// its bytes, never a result.
//
// Only the request's goroutine calls its methods; the reads run in
// goroutines that close waits for, so none outlives the request. Their
// context is the request's: a cancelled request, or Close, ends them.
type readAhead struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	slots  int
	budget int64
	// maxPending bounds len(pending): a small multiple of slots, so that
	// the bookkeeping stays small however long the listing is.
	maxPending int
	// maxPage is the largest extent the page read path reads in one read
	// (chunk.located): a larger one is not read ahead.
	maxPage int64
	// inflight is the number of reads not yet ended: raised by the request's
	// goroutine when it starts one, lowered by the read's goroutine when it
	// ends, before it closes its done channel.
	inflight atomic.Int32
	// held is the bytes of pages in flight or fetched and not yet taken
	// or dropped.
	held int64
	// pending holds the pages read ahead and not yet taken or dropped, by
	// extent. queue holds them in the order they were read ahead, from
	// qhead on, with the entries taken or dropped since (gone) left in it
	// until the queue is compacted: pages are used in the order the scan
	// reaches them, so drop only looks at the queue's front.
	pending map[aheadKey]*aheadPage
	queue   []*aheadPage
	qhead   int
	// reads, readBytes, unused and unusedBytes count the pages read ahead
	// that were taken or dropped, for the request's counts; the pages
	// still pending are counted when the read-ahead closes.
	reads, readBytes, unused, unusedBytes int64
	closed                                bool
	// lastG and lastAt are the page the last prediction of rows started
	// from: a scan stepping through rows predicts again only on a new page.
	lastG  groupRef
	lastAt int
}

// aheadKey is a page's extent in a part.
type aheadKey struct {
	p         *part
	off, size int64
}

// aheadPage is one page read ahead: g and page locate it in the scan's
// walk. raw, read and err are set by its goroutine before done is closed.
type aheadPage struct {
	key  aheadKey
	g    groupRef
	page int
	size int64
	done chan struct{}
	raw  []byte
	read bool // the goroutine called ReadAt
	err  error
	gone bool // taken or dropped: no longer pending, and counted
}

// errReadAheadPanic is the failure of a read ahead whose source panicked.
var errReadAheadPanic = errors.New("the read ahead panicked")

// newReadAhead returns the read-ahead of a request with n slots, or nil
// for none.
func (r *reader) newReadAhead(ctx context.Context, n int) *readAhead {
	if n <= 0 {
		return nil
	}
	n = min(n, maxReadAhead)
	actx, cancel := context.WithCancel(ctx)
	return &readAhead{ctx: actx, cancel: cancel, slots: n, budget: int64(n) * readAheadPageBytes, maxPending: 4 * n,
		maxPage: r.limits.PageBytes + r.limits.headerBytes(), pending: map[aheadKey]*aheadPage{}, lastAt: -1}
}

// finished reports whether e's read has ended.
func (e *aheadPage) finished() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

// retire removes e, which is pending, from the read-ahead and counts it:
// used when the scan took its bytes.
func (a *readAhead) retire(e *aheadPage, used bool) {
	delete(a.pending, e.key)
	a.held -= e.size
	e.gone = true
	e.raw = nil
	if !e.read {
		return
	}
	a.reads++
	a.readBytes += e.size
	if !used {
		a.unused++
		a.unusedBytes += e.size
	}
}

// compact removes the entries gone from the queue.
func (a *readAhead) compact() {
	kept := a.queue[:0]
	for _, e := range a.queue[a.qhead:] {
		if !e.gone {
			kept = append(kept, e)
		}
	}
	clear(a.queue[len(kept):])
	a.queue, a.qhead = kept, 0
}

// fetch reads ahead page i of row group g of part p, which loc locates,
// unless it is pending already. It reports false when there is no room
// for it: every slot in flight, the budget held, or the most pages
// pending.
func (a *readAhead) fetch(p *part, g groupRef, i int, loc pageLoc) bool {
	if a.closed {
		return false
	}
	key := aheadKey{p, loc.offset, loc.size}
	if _, ok := a.pending[key]; ok {
		return true
	}
	if loc.size <= 0 || loc.size > a.maxPage || a.held+loc.size > a.budget || int(a.inflight.Load()) >= a.slots || len(a.pending) >= a.maxPending {
		return false
	}
	e := &aheadPage{key: key, g: g, page: i, size: loc.size, done: make(chan struct{})}
	a.pending[key] = e
	if len(a.queue) >= 4*a.maxPending {
		a.compact()
	}
	a.queue = append(a.queue, e)
	a.held += loc.size
	a.inflight.Add(1)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(e.done)
		defer a.inflight.Add(-1)
		defer func() {
			// A panic of the source ends the read ahead, not the process:
			// the page is read again by the scan, which meets the panic, if
			// it repeats, as it would without a read-ahead.
			if r := recover(); r != nil {
				e.raw, e.err = nil, errReadAheadPanic
			}
		}()
		if err := a.ctx.Err(); err != nil {
			e.err = err
			return
		}
		e.read = true
		raw := make([]byte, loc.size)
		if err := readFull(bind(a.ctx, p.file), raw, loc.offset); err != nil {
			e.err = err
			return
		}
		e.raw = raw
	}()
	return true
}

// take returns the bytes read ahead for the extent of size bytes at off in
// part p, waiting for their read to end, or false when none were read
// ahead or their read failed: the caller then reads the extent itself.
func (a *readAhead) take(p *part, off, size int64) ([]byte, bool) {
	key := aheadKey{p, off, size}
	e, ok := a.pending[key]
	if !ok {
		return nil, false
	}
	select {
	case <-e.done:
	case <-a.ctx.Done():
		return nil, false // the read the caller makes fails with the request
	}
	raw := e.raw
	ok = e.err == nil && raw != nil
	a.retire(e, ok)
	return raw, ok
}

// drop forgets the pages read ahead that the scan, now on page at of row
// group g, has passed: pages of other row groups and pages at or before
// at, once their reads have ended. They count as unused. Only the front of
// the queue is looked at, so a call costs the pages it drops.
func (a *readAhead) drop(g groupRef, at int) {
	for a.qhead < len(a.queue) {
		e := a.queue[a.qhead]
		if !e.gone {
			if (e.g == g && e.page > at) || !e.finished() {
				break
			}
			a.retire(e, false)
		}
		a.queue[a.qhead] = nil
		a.qhead++
	}
}

// close ends the reads still in flight, waits for them, and adds the
// read-ahead's reads to st: every read made is in SourceReads and
// SourceBytes, and in ReadAheadReads; those the scan did not use in
// ReadAheadUnused and ReadAheadUnusedBytes. It may be called again: the
// reads are added once.
func (a *readAhead) close(st *ReadStats) {
	if a == nil || a.closed {
		return
	}
	a.closed = true
	a.cancel()
	a.wg.Wait()
	for _, e := range a.pending {
		a.retire(e, false)
	}
	st.SourceReads += a.reads
	st.SourceBytes += a.readBytes
	st.ReadAheadReads += a.reads
	st.ReadAheadUnused += a.unused
	st.ReadAheadUnusedBytes += a.unusedBytes
	a.queue, a.qhead = nil, 0
}

// readAhead predicts the key pages a listing will decode next in the row
// group the scan is on, and reads ahead those not yet pending, as far as
// the read-ahead has room, at most one per entry the listing can still
// return (remaining). The scan is on its current key, after the listing
// has emitted its entry: a common prefix c, after which the scan seeks to
// successor(c) and lands on the first page from the current one whose
// maximum reaches it (keyPages.seek, groupIndex.pageFor), or a row, after
// which it decodes the next page once the current one is done. A page's
// maximum then stands for the entry that follows it: its common prefix,
// or a row when it has none. Bounds may be shortened or overlap (RG-5,
// RG-6), so the prediction may be wrong; it stops at the row group's end
// and at a page beginning at or after the scan's upper bound.
func (s *scan) readAhead(req *ListQuery, remaining int) {
	a := s.ahead
	if a == nil || s.kp == nil || s.synthetic || s.key == nil || s.kp.at < 0 || remaining <= 0 {
		return
	}
	index, at := s.kp.index, s.kp.at
	cur := commonPrefix(s.key, req.Prefix, req.Delimiter)
	if cur == nil && a.lastG == s.g && a.lastAt == at {
		return
	}
	a.lastG, a.lastAt = s.g, at
	a.drop(s.g, at)
	pos := at
	for n := 0; n < remaining; {
		j := pos + 1
		if cur != nil {
			next, ok := successor(cur)
			if !ok {
				return
			}
			if j = index.pageFor(pos, next, true); j == pos {
				// The next entry may be on this page: predict past the
				// common prefix of its maximum when that is later.
				if c := commonPrefix(index.pages[pos].hi, req.Prefix, req.Delimiter); c != nil && bytes.Compare(c, cur) > 0 {
					cur = c
					continue
				}
				j = pos + 1
			}
		}
		if j >= len(index.pages) {
			return
		}
		page := &index.pages[j]
		if s.end != nil && bytes.Compare(page.lo, s.end) >= 0 {
			return
		}
		if !a.fetch(s.kp.part, s.g, j, page.pageLoc) {
			return
		}
		n++
		pos = j
		cur = commonPrefix(page.hi, req.Prefix, req.Delimiter)
	}
}
