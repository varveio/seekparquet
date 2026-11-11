// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"sync"
)

// maxReadAhead bounds List.ReadAhead: larger values are read as it.
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

// readAhead is one List request's read-ahead of key pages (List.ReadAhead).
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
	// maxPage is the largest extent the page read path reads in one read
	// (chunk.located): a larger one is not read ahead.
	maxPage int64
	// held is the bytes of pages in flight or fetched and not yet taken
	// or dropped.
	held int64
	// pending holds the pages read ahead and not yet taken or dropped, by
	// extent; all holds every page read ahead, for the request's counts.
	pending map[aheadKey]*aheadPage
	all     []*aheadPage
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
	g    groupRef
	page int
	size int64
	done chan struct{}
	raw  []byte
	read bool // the goroutine called ReadAt
	err  error
	used bool // the scan took it
}

// newReadAhead returns the read-ahead of a request with n slots, or nil
// for none.
func (r *reader) newReadAhead(ctx context.Context, n int) *readAhead {
	if n <= 0 {
		return nil
	}
	n = min(n, maxReadAhead)
	actx, cancel := context.WithCancel(ctx)
	return &readAhead{ctx: actx, cancel: cancel, slots: n, budget: int64(n) * readAheadPageBytes,
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

// inFlight returns the number of reads not yet ended.
func (a *readAhead) inFlight() int {
	n := 0
	for _, e := range a.pending {
		if !e.finished() {
			n++
		}
	}
	return n
}

// fetch reads ahead page i of row group g of part p, which loc locates,
// unless it is pending already. It reports false when there is no room
// for it: every slot in flight, or the budget held.
func (a *readAhead) fetch(p *part, g groupRef, i int, loc pageLoc) bool {
	key := aheadKey{p, loc.offset, loc.size}
	if _, ok := a.pending[key]; ok {
		return true
	}
	if loc.size <= 0 || loc.size > a.maxPage || a.held+loc.size > a.budget || a.inFlight() >= a.slots {
		return false
	}
	e := &aheadPage{g: g, page: i, size: loc.size, done: make(chan struct{})}
	a.pending[key] = e
	a.all = append(a.all, e)
	a.held += loc.size
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(e.done)
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
	delete(a.pending, key)
	a.held -= e.size
	if e.err != nil || e.raw == nil {
		return nil, false
	}
	e.used = true
	raw := e.raw
	e.raw = nil
	return raw, true
}

// drop forgets the pages read ahead that the scan, now on page at of row
// group g, has passed: pages of other row groups and pages at or before
// at, once their reads have ended. They count as unused.
func (a *readAhead) drop(g groupRef, at int) {
	for key, e := range a.pending {
		if (e.g != g || e.page <= at) && e.finished() {
			delete(a.pending, key)
			a.held -= e.size
			e.raw = nil
		}
	}
}

// close ends the reads still in flight, waits for them, and adds the
// read-ahead's reads to st: every read made is in SourceReads and
// SourceBytes, and in ReadAheadReads; those the scan did not use in
// ReadAheadUnused and ReadAheadUnusedBytes.
func (a *readAhead) close(st *ReadStats) {
	if a == nil {
		return
	}
	a.cancel()
	a.wg.Wait()
	for _, e := range a.all {
		if !e.read {
			continue
		}
		st.SourceReads++
		st.SourceBytes += e.size
		st.ReadAheadReads++
		if !e.used {
			st.ReadAheadUnused++
			st.ReadAheadUnusedBytes += e.size
		}
	}
	a.pending, a.all = nil, nil
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
func (s *scan) readAhead(req *List, remaining int) {
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
