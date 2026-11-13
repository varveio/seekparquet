// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"math"

	"github.com/parquet-go/parquet-go/format"
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
	// prefix and delim are the listing's (ListQuery.Prefix, Delimiter).
	prefix []byte
	delim  byte
	// hintIndex is the key page index decoded from bytes at hand for row
	// group hintG (hint), nil for none.
	hintG     groupRef
	hintIndex *groupIndex
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

// readAhead predicts the key pages a listing will decode next, and reads
// ahead those not yet pending, as far as the read-ahead has room, at most
// one per entry the listing can still return (remaining). The scan is on
// its current key, after the listing has emitted its entry: a common prefix
// c, after which the scan seeks to successor(c), or a row, after which it
// decodes the next page once the current one is done (chain).
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
	a.w.fetchAhead(a.chain(s, s.g, index, at, cur, nil, 0, 0))
}

// chain appends to exts, of size bytes and n pages, the key pages the
// listing is predicted to decode after page pos of row group g, whose key
// page index is index, when the entry that follows that page is cur: a
// common prefix, after which the scan seeks to its successor and lands on
// the first page from pos whose maximum reaches it (keyPages.seek,
// groupIndex.pageFor), or nil for a row, after which it decodes the next
// page. A page's maximum then stands for the entry that follows it: its
// common prefix, or a row when it has none. So a listing that skips from
// prefix to prefix is predicted one page per prefix, however many pages
// lie between them, and a listing of rows page after page.
//
// The prediction continues into the row groups after g in its part while
// their key page indexes are at hand without a read (hint), through at most
// indexAhead groups. Where one is not, the pages of the following groups
// are predicted in file order when the step into that group follows the
// page before it, and not at all when it skips pages: the scan loads that group's index when it gets there, and
// the prediction resumes from the page it lands on (landing).
//
// Bounds may be shortened or overlap (RG-5, RG-6), so the prediction may be
// wrong; it stops at the part's end and at a page beginning at or after the
// scan's upper bound. With no extent yet, a first predicted page the
// windows already hold ends it with nothing: the listing predicts again
// once the scan reaches what is not read ahead.
func (a *readAhead) chain(s *scan, g groupRef, index *groupIndex, pos int, cur []byte, exts []extent, size int64, n int) []extent {
	r := s.r
	p := r.parts[g.part]
	// adjacent says the last step went to the page after the one before
	// it; crossed counts the row groups this prediction has entered.
	adjacent, crossed := true, 0
	for n < min(a.remaining, a.pages, MaxReadRanges) {
		j := pos + 1
		if cur != nil {
			next, ok := successor(cur)
			if !ok {
				break
			}
			if j = index.pageFor(max(pos, 0), next, true); j == pos {
				// The next entry may be on this page: predict past the
				// common prefix of its maximum when that is later.
				if c := commonPrefix(index.pages[pos].hi, a.prefix, a.delim); c != nil && bytes.Compare(c, cur) > 0 {
					cur = c
					continue
				}
				j = pos + 1
			}
		}
		adjacent = j == pos+1
		if j >= len(index.pages) {
			// The listing continues in the next row group. A prediction
			// enters at most indexAhead groups, so a prefix that spans
			// many costs it a bounded walk, and it ends with the request.
			g = g.next()
			if crossed++; crossed > indexAhead || s.ctx.Err() != nil {
				break
			}
			if g.index >= len(p.groups) || s.end != nil && r.startsAtOrAfter(g, s.end) {
				break
			}
			if index = a.hint(r, g); index == nil {
				if cur == nil || adjacent {
					exts, _ = a.following(s, g, 0, exts, size, n)
				}
				exts = append(exts, a.indexExtents(s, g, cur != nil && !adjacent)...)
				break
			}
			pos = -1
			continue
		}
		page := &index.pages[j]
		if s.end != nil && bytes.Compare(page.lo, s.end) >= 0 {
			break
		}
		if len(exts) == 0 && a.w.holds(p, page.offset, page.size) {
			return nil // read ahead already: predict again once the scan reaches what is not
		}
		if page.size > r.limits.PageBytes || size+page.size > a.bytes && len(exts) > 0 {
			break
		}
		size += page.size
		exts = append(exts, extent{p, page.offset, page.size, r.group(g).start + page.first + page.rows})
		n++
		pos = j
		cur = commonPrefix(page.hi, a.prefix, a.delim)
	}
	return exts
}

// indexAhead is the number of row groups whose key page indexes a listing
// that skips from prefix to prefix reads ahead at once, when they are not
// at hand: the prediction then runs through that many groups before it
// waits for an index again.
const indexAhead = 8

// indexExtents returns the key ColumnIndex and OffsetIndex of row group g
// to read ahead, and of the groups after it in its part up to indexAhead
// when the listing is skipping: those the Dataset has not loaded, that the
// part's retained metadata does not hold, and that no window holds. The
// scan loads each index from the windows when it opens the group, checking
// it as it checks one it reads itself, and the prediction reads it there
// too (hint).
func (a *readAhead) indexExtents(s *scan, g groupRef, skipping bool) []extent {
	r := s.r
	p := r.parts[g.part]
	groups := 1
	if skipping {
		groups = indexAhead
	}
	var exts []extent
	for ; g.index < len(p.groups) && groups > 0; g, groups = g.next(), groups-1 {
		if s.end != nil && r.startsAtOrAfter(g, s.end) {
			break
		}
		grp := p.groups[g.index]
		if p.index[grp.physical].Load() != nil {
			continue
		}
		meta := &p.footer.Metadata().RowGroups[grp.physical].Columns[p.key.index]
		for _, x := range [2][2]int64{{meta.ColumnIndexOffset, int64(meta.ColumnIndexLength)}, {meta.OffsetIndexOffset, int64(meta.OffsetIndexLength)}} {
			if x[0] <= 0 || x[1] <= 0 || x[1] > math.MaxInt32 || p.checkIndexExtent(grp.physical, x[0], int32(x[1])) != nil {
				return exts
			}
			if p.retainedBytes(x[0], x[1]) == nil && !a.w.holds(p, x[0], x[1]) {
				exts = append(exts, extent{p, x[0], x[1], grp.start + grp.rows})
			}
		}
	}
	return exts
}

// hint returns the key page index of row group g to predict with, without
// reading storage: the checked index the Dataset keeps, or one decoded and
// checked from page-index bytes at hand, in the part's retained metadata or
// read ahead into the windows (indexExtents), which the read-ahead keeps
// for the one group it last predicted into. It is nil when the bytes are
// not at hand or do not make a valid index. A hint decides only which pages
// are read ahead; the scan loads and checks the index itself before it
// uses the group.
func (a *readAhead) hint(r *reader, g groupRef) *groupIndex {
	p := r.parts[g.part]
	physical := p.groups[g.index].physical
	if index := p.index[physical].Load(); index != nil {
		return index
	}
	if a.hintIndex != nil && a.hintG == g {
		return a.hintIndex
	}
	a.hintG, a.hintIndex = g, p.keyIndexAtHand(physical, a.w)
	return a.hintIndex
}

// keyIndexAtHand decodes and checks the key page index of a physical row
// group from bytes already read: the part's retained metadata, or the
// windows w. It is nil when neither holds both of its page indexes or they
// are not a valid index. It publishes nothing and counts nothing.
func (p *part) keyIndexAtHand(physical int, w *windows) *groupIndex {
	rg := &p.footer.Metadata().RowGroups[physical]
	meta := &rg.Columns[p.key.index]
	start, end, ok := chunkRange(&meta.MetaData)
	if !ok || end > p.size {
		return nil
	}
	atHand := func(off int64, length int32) []byte {
		if off <= 0 || length <= 0 || p.checkIndexExtent(physical, off, length) != nil {
			return nil
		}
		if raw := p.retainedBytes(off, int64(length)); raw != nil {
			return raw
		}
		if !w.holds(p, off, int64(length)) {
			return nil
		}
		raw := make([]byte, length)
		if ok, err := w.copy(p, raw, off); !ok || err != nil {
			return nil
		}
		return raw
	}
	rawColumns := atHand(meta.ColumnIndexOffset, meta.ColumnIndexLength)
	if rawColumns == nil {
		return nil
	}
	rawOffsets := atHand(meta.OffsetIndexOffset, meta.OffsetIndexLength)
	if rawOffsets == nil {
		return nil
	}
	var columns format.ColumnIndex
	if _, err := decodeBounded(rawColumns, &columns, p.r.limits.MetadataMemory); err != nil {
		return nil
	}
	offsets, err := p.decodeOffsetIndex(rawOffsets)
	if err != nil {
		return nil
	}
	index, err := checkKeyIndex(&columns, offsets, rg.NumRows, start, end, p.utf8Key)
	if err != nil {
		return nil
	}
	return index
}

// landing is called before a listing's walker decodes key page i of row
// group g, which the read-ahead did not predict: when no window holds it,
// it reads it ahead with the pages predicted to follow it (chain), from
// the entry its maximum stands for.
func (a *readAhead) landing(s *scan, k *keyPages, i int) {
	page := &k.index.pages[i]
	if page.size > s.r.limits.PageBytes || a.w.holds(k.part, page.offset, page.size) {
		return
	}
	if a.delim == '/' && s.listIndex != nil && s.listIndex.group(k.g) != nil {
		// The list index answers this group's skips without decoding the
		// pages a prediction from bounds would name: read the pages in
		// file order from this one, as the walk reads them when it does
		// decode.
		exts, _ := a.following(s, k.g, i, nil, 0, 0)
		a.w.fetchAhead(exts)
		return
	}
	exts := []extent{{k.part, page.offset, page.size, s.r.group(k.g).start + page.first + page.rows}}
	a.w.fetchAhead(a.chain(s, k.g, k.index, i, commonPrefix(page.hi, a.prefix, a.delim), exts, page.size, 1))
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
	for ; g.index < len(p.groups) && n < min(a.remaining, a.pages, MaxReadRanges); g, i = g.next(), 0 {
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
		for ; i < len(locs) && n < min(a.remaining, a.pages, MaxReadRanges); i++ {
			loc := locs[i]
			if s.end != nil && index != nil && bytes.Compare(index.pages[i].lo, s.end) >= 0 {
				return exts, n
			}
			if loc.size > r.limits.PageBytes || size+loc.size > a.bytes && len(exts) > 0 {
				return exts, n
			}
			size += loc.size
			exts = append(exts, extent{p, loc.offset, loc.size, grp.start + loc.first + loc.rows})
			n++
		}
	}
	return exts, n
}
