// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"math"
)

// testListReadAhead, when positive, is the read-ahead of a listing that
// asks for none. Tests set it to run every listing with read-ahead; it is
// zero otherwise.
var testListReadAhead int

// readAhead is one List request's read-ahead of key pages
// (ListQuery.ReadAhead): from the checked key page index of the row group
// the scan is in, it predicts the key pages the listing will decode next,
// and reads them ahead together, speculatively, in the operation's windows
// (windows.fetch): runs of adjacent pages in one window, up to
// readWindowBytes. When the scan decodes a page, the page read path takes its
// bytes from the windows instead of reading them, and checks them exactly
// as it checks bytes it reads itself (CHECK-2): nothing is trusted because
// it was read ahead. A read ahead that failed is not used; the page is
// read again as if it had not been read ahead. A prediction is a hint: a
// page predicted wrongly costs its bytes, never a result. Only the
// request's goroutine calls its methods.
type readAhead struct {
	w *windows
	// pages bounds the pages one prediction reads ahead; bytes their
	// bytes.
	pages int
	bytes int64
	// lastG and lastAt are the page the last prediction of rows started
	// from: a scan stepping through rows predicts again only on a new page.
	lastG  groupRef
	lastAt int
	// passed is the first row of the page the last prediction started
	// from, a dataset ordinal: the windows of rows before it are dropped
	// at the next.
	passed int64
	// remaining is the most entries the listing can still return, as the
	// last prediction found it.
	remaining int
}

// newReadAhead returns the read-ahead of a listing under ctx that asks
// for n pages (ListQuery.ReadAhead), or nil for none: n of zero reads
// adaptively ahead over a remote Source, and nothing from a local file.
// Read-ahead is speculation: ReadPolicy.DisableSpeculation turns it off,
// and each wave is bounded internally.
func (r *reader) newReadAhead(ctx context.Context, n, limit int) *readAhead {
	o := operationOf(ctx)
	if r.policy.DisableSpeculation || o == nil || o.r == nil || n == 0 && r.local {
		return nil
	}
	if n == 0 {
		n = math.MaxInt
	}
	return &readAhead{w: o.windows(), pages: n, bytes: readWindowBytes, lastAt: -1, passed: -1, remaining: limit}
}

// close drops the windows of the pages read ahead, once their reads have
// ended: the key scan is over.
func (a *readAhead) close() {
	if a != nil {
		a.w.release(math.MaxInt64)
	}
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
	a.remaining = remaining
	ordinal := s.r.group(s.g).start + index.pages[at].first
	a.w.release(min(a.passed, ordinal))
	a.passed = ordinal
	p := s.kp.part
	var exts []extent
	size := int64(0)
	pos, n := at, 0
	for n < min(remaining, a.pages) {
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
			// The listing continues in the next row group, from its first
			// page.
			exts, _ = a.following(s, s.g.next(), 0, exts, size, n)
			break
		}
		page := &index.pages[j]
		if s.end != nil && bytes.Compare(page.lo, s.end) >= 0 {
			break
		}
		if len(exts) == 0 && a.w.holds(p, page.offset, page.size) {
			return // read ahead already: predict again once the scan reaches what is not
		}
		if size+page.size > a.bytes && len(exts) > 0 {
			break
		}
		size += page.size
		exts = append(exts, extent{p, page.offset, page.size, s.r.group(s.g).start + page.first + page.rows})
		n++
		pos = j
		cur = commonPrefix(page.hi, req.Prefix, req.Delimiter)
	}
	a.w.fetch(exts, true)
}

// landing is called before a listing's walker decodes key page i of row
// group g, which the read-ahead did not predict: when no window holds it,
// it reads it ahead with the pages after it, up to the read-ahead's
// bounds, in the row group and the ones after it in the part, as the
// listing reads them when it does not skip.
func (a *readAhead) landing(s *scan, k *keyPages, i int) {
	loc := k.index.pages[i].pageLoc
	if a.w.holds(k.part, loc.offset, loc.size) {
		return
	}
	exts, _ := a.following(s, k.g, i, nil, 0, 0)
	a.w.fetch(exts, true)
}

// following appends to exts, of size bytes and n pages, the key pages of
// row group g from page i on, and of the groups after it in its part, up
// to the read-ahead's bounds and the listing's upper bound, and returns
// them and their pages. Pages are located by the group's checked key page
// index, or by the OffsetIndex held in the part's retained metadata; a
// group located by neither ends them.
func (a *readAhead) following(s *scan, g groupRef, i int, exts []extent, size int64, n int) ([]extent, int) {
	r := s.r
	p := r.parts[g.part]
	for ; g.index < len(p.groups) && n < min(a.remaining, a.pages); g, i = g.next(), 0 {
		if s.end != nil && i == 0 && r.startsAtOrAfter(g, s.end) {
			break
		}
		grp := p.groups[g.index]
		meta := &p.footer.Metadata().RowGroups[grp.physical].Columns[p.key.index].MetaData
		start, stop, ok := chunkRange(meta)
		if !ok || stop > p.size {
			break
		}
		locs := p.planLocs(grp.physical, p.key, start, stop)
		if locs == nil {
			break
		}
		index := p.index[grp.physical].Load()
		for ; i < len(locs) && n < min(a.remaining, a.pages); i++ {
			loc := locs[i]
			if s.end != nil && index != nil && bytes.Compare(index.pages[i].lo, s.end) >= 0 {
				return exts, n
			}
			if size+loc.size > a.bytes && len(exts) > 0 {
				return exts, n
			}
			size += loc.size
			exts = append(exts, extent{p, loc.offset, loc.size, grp.start + loc.first + loc.rows})
			n++
		}
	}
	return exts, n
}
