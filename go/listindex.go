// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"time"
)

// ListIndexOptions configures the optional in-memory slash listing index.
// Zero fields use 64 MiB, one worker, and eight directory changes per page.
type ListIndexOptions struct {
	// MaxBytes bounds all retained index storage, including containers and anchors.
	// Existing footer, routing and Parquet page-index caches have their own Limits.
	MaxBytes int64
	// Workers bounds concurrently decoded row groups and their temporary storage.
	Workers int
	// MaxChangesPerPage omits the boundaries of denser pages; reads decode them.
	MaxChangesPerPage int
}

// ListIndexStats describes a build. Failed groups remain on the ordinary read
// path. Pages includes decoded work in a cancelled batch; cancelled builds
// retain no bytes, entries or groups. BudgetExhausted means the next group would
// exceed MaxBytes.
type ListIndexStats struct {
	GroupsIndexed, GroupsSkipped, GroupsFailed, PartsFailed int64
	Pages, Entries, Bytes                                   int64
	BudgetExhausted                                         bool
	Duration                                                time.Duration
	// Options are the effective settings of the cached build. Compare with normalized
	// requested settings to detect reuse of a nonempty index with different options.
	Options ListIndexOptions
	// OptionsMismatch reports that a nonempty index was reused with different requested options.
	OptionsMismatch bool
	// HasIndex reports whether any group is indexed.
	HasIndex bool
}

type listBoundary struct {
	row               int64
	off, size, shared int
	denseThrough      int
}
type listGroup struct {
	g          groupRef
	dense      bool
	base, data []byte
	last       []byte         // build-only boundary check; cleared before publication
	runs       []listBoundary // negative size complements a dense marker's suffix length
}
type listIndex struct {
	groups []listGroup
	stats  ListIndexStats
}

// BuildListIndex decodes every key page once to build a bounded, optional index
// of directory runs for List with delimiter '/'. It does not run at open.
// Complete groups are admitted in dataset order; the first over-budget group
// stops admission. Groups with data refusals or unsupported codecs/encodings
// are omitted; subsequent reads still perform their ordinary checks. Storage
// failures and resource limits fail this attempt without publishing it and permit retry. Build-only page indexes and first keys
// never consume Limits.IndexMemory.
// Publication is atomic: concurrent reads use either the old path or the whole
// completed index. Cancellation discards this attempt and permits retry. After
// a nonempty successful build (including budget exhaustion), later calls return its stats
// without rebuilding; Stats.Options reports the effective settings and
// Stats.OptionsMismatch flags differing requests. Empty outcomes
// are cached for the most recent options and rebuilt when those options change.
// Stats.HasIndex reports whether any groups are indexed. Close cancels the build.
// It asks a Source's byte cache to be bypassed (CacheBypass), unless ctx sets
// a cache mode.
func (d *Dataset) BuildListIndex(ctx context.Context, opt ListIndexOptions) (stats ListIndexStats, err error) {
	if opt.MaxBytes < 0 || opt.Workers < 0 || opt.MaxChangesPerPage < 0 {
		return stats, invalidRequest("negative list index option")
	}
	if opt.MaxBytes == 0 {
		opt.MaxBytes = 64 << 20
	}
	if opt.Workers == 0 {
		opt.Workers = 1
	}
	if opt.MaxChangesPerPage == 0 {
		opt.MaxChangesPerPage = 8
	}
	r := d.r
	rctx, err := r.begin(bypassCache(ctx))
	if err != nil {
		return stats, err
	}
	defer rctx.end(nil)
	for !r.listBuild.TryLock() {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-rctx.Done():
			timer.Stop()
			return stats, finish(ctx, rctx, rctx.Err())
		}
	}
	defer r.listBuild.Unlock()
	if err := rctx.Err(); err != nil {
		return stats, finish(ctx, rctx, err)
	}
	if prior := r.listIndex.Load(); prior != nil && (len(prior.groups) > 0 || prior.stats.Options == opt) {
		stats = prior.stats
		stats.OptionsMismatch = stats.Options != opt
		return stats, nil
	}
	stats.Options = opt
	started := time.Now()
	defer func() {
		if err != nil || stats.Duration == 0 {
			stats.Duration = time.Since(started)
		}
		if err != nil {
			stats.Bytes = 0
			stats.Entries = 0
			stats.GroupsIndexed = 0
		}
		err = finish(ctx, rctx, err)
	}()
	built := &listIndex{}
	var readStats ReadStats
	// Over a remote Source, the parts are checked together first, and every
	// key page the build decodes is read ahead in windows (buildWindows).
	var win *windows
	if !r.local {
		_ = r.verifyParts(rctx, new(pageMemo), &readStats, probeNone) // a part that fails is met again below
		win = rctx.windows()
	}
	var previousLast []byte
	var previousRef groupRef
	var previousFailed, previousAdmitted bool
outer:
	for pi := range r.parts {
		if err = rctx.Err(); err != nil {
			return stats, err
		}
		var p *part
		p, err = r.part(rctx, pi, &readStats)
		if err != nil {
			if rctx.Err() != nil {
				return stats, rctx.Err()
			}
			if !listDataFailure(err) {
				return stats, err
			}
			stats.PartsFailed++
			err = nil
			continue
		}
		for start := 0; start < len(p.groups); {
			if err = rctx.Err(); err != nil {
				return stats, err
			}
			n := min(opt.Workers, len(p.groups)-start)
			if win != nil {
				win.release(p.groups[start].start)
				r.buildWindows(win, groupRef{pi, start})
			}
			type result struct {
				group listGroup
				pages int64
				err   error
			}
			results := make([]result, n)
			var wg sync.WaitGroup
			for j := range n {
				if rctx.Err() != nil {
					break
				}
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					if e := rctx.Err(); e != nil {
						results[j].err = e
						return
					}
					results[j].group, results[j].pages, results[j].err = r.buildListGroup(rctx, groupRef{pi, start + j}, opt)
				}(j)
			}
			wg.Wait()
			for _, out := range results {
				stats.Pages += out.pages
			}
			if err = rctx.Err(); err != nil {
				return stats, err
			}
			for _, out := range results {
				if out.err != nil && !listDataFailure(out.err) {
					return stats, out.err
				}
			}
			for oi, out := range results {
				badBoundary := previousLast != nil && out.group.base != nil && previousRef.part == out.group.g.part && previousRef.index+1 == out.group.g.index && r.outOfOrder(previousLast, out.group.base)
				if badBoundary && !previousFailed {
					stats.GroupsFailed++
					if previousAdmitted {
						last := built.groups[len(built.groups)-1]
						stats.Bytes -= last.memoryBytes()
						stats.Entries -= int64(len(last.runs))
						stats.GroupsIndexed--
						stats.GroupsSkipped++
						built.groups[len(built.groups)-1] = listGroup{}
						built.groups = built.groups[:len(built.groups)-1]
						if len(built.groups) == 0 {
							stats.Bytes -= 256
						}
					}
				}
				// Keep the boundary even after rejecting a group: a chain of
				// descending boundaries must exclude every participant once.
				previousLast, previousRef = out.group.last, out.group.g
				if previousLast == nil {
					previousLast = out.group.base
				}
				previousFailed = out.err != nil || badBoundary
				previousAdmitted = false
				out.group.last = nil
				if previousFailed {
					stats.GroupsSkipped++
					stats.GroupsFailed++
					continue
				}
				if len(out.group.runs) == 0 {
					stats.GroupsSkipped++
					continue
				}
				cost := out.group.memoryBytes()
				if len(built.groups) == 0 {
					cost += 256
				}
				if cost > opt.MaxBytes-stats.Bytes {
					stats.BudgetExhausted = true
					stats.GroupsSkipped += int64(len(results) - oi)
					for _, extra := range results[oi+1:] {
						if extra.err != nil {
							stats.GroupsFailed++
						}
					}
					break outer
				}
				built.groups = append(built.groups, out.group)
				stats.Bytes += cost
				stats.Entries += int64(len(out.group.runs))
				stats.GroupsIndexed++
				previousAdmitted = true
			}
			start += n
		}
	}
	if err = rctx.Err(); err != nil {
		return stats, err
	}
	built.groups = slices.Clip(slices.Clone(built.groups))
	stats.Duration = time.Since(started)
	stats.HasIndex = len(built.groups) > 0
	built.stats = stats
	r.listIndex.Store(built)
	return stats, nil
}

// Conservative allocation accounting includes compact buffers, allocator rounding and
// each group's slot in the snapshot. No maps or per-page arrays are retained.
func (g *listGroup) memoryBytes() int64 {
	return 256 + listAllocation(len(g.base)) + listAllocation(len(g.data)) + listAllocation(40*len(g.runs))
}

// A common, allocator-independent upper bound used by both implementations.
func listAllocation(n int) int64 {
	if n == 0 {
		return 0
	}
	return int64(1) << bits.Len(uint(n-1))
}
func listDataFailure(err error) bool {
	var refusal *RefusalError
	var unsupported *UnsupportedError
	return errors.As(err, &refusal) || errors.As(err, &unsupported)
}
func (g *listGroup) add(row int64, key []byte) {
	b := listBoundary{row: row, size: -1}
	if key == nil {
		g.dense = true
	}
	if key != nil {
		for b.shared < len(g.base) && b.shared < len(key) && g.base[b.shared] == key[b.shared] {
			b.shared++
		}
		b.off = len(g.data)
		b.size = len(key) - b.shared
		g.data = append(g.data, key[b.shared:]...)
	}
	if len(g.runs) > 0 {
		b.denseThrough = g.runs[len(g.runs)-1].denseThrough
	}
	if key == nil {
		b.denseThrough++
	}
	g.runs = append(g.runs, b)
}
func directory(key []byte) []byte { return key[:bytes.LastIndexByte(key, '/')+1] }

// buildWindows reads ahead, in the build's windows, the key pages of the
// row groups from g on, in checked parts, up to readWaveBytes:
// the build decodes every one of them. A group whose key column chunk
// does not lie in its file is left out, for its build to refuse.
func (r *reader) buildWindows(w *windows, g groupRef) {
	var exts []extent
	total, most := int64(0), readWaveBytes
	for ; g.part < len(r.parts) && r.parts[g.part].ready.Load(); g = (groupRef{g.part + 1, 0}) {
		p := r.parts[g.part]
		for ; g.index < len(p.groups); g = g.next() {
			grp := p.groups[g.index]
			meta := &p.footer.Metadata().RowGroups[grp.physical].Columns[p.key.index].MetaData
			start, stop, ok := chunkRange(meta)
			if !ok || stop > p.size {
				continue
			}
			if total > 0 && total+stop-start > most {
				w.fetch(exts, false)
				return
			}
			total += stop - start
			end := grp.start + grp.rows
			if locs := p.planLocs(grp.physical, p.key, start, stop); locs != nil {
				for _, loc := range locs {
					exts = append(exts, extent{p, loc.offset, loc.size, end})
				}
			} else {
				exts = append(exts, extent{p, start, stop - start, end})
			}
		}
	}
	w.fetch(exts, false)
}

func (r *reader) buildListGroup(ctx context.Context, ref groupRef, opt ListIndexOptions) (g listGroup, pages int64, err error) {
	g.g = ref
	st := new(ReadStats)
	defer func() {
		pages = st.KeyPagesDecoded
		if g.base == nil {
			if first := r.group(ref).first.Load(); first != nil {
				g.base = bytes.Clone(*first)
			}
		}
	}()
	log := pageLog{unique: r.key.Unique}
	k, err := r.loadKeyPages(ctx, ref, &log, st, false)
	if err != nil {
		return g, 0, err
	}
	k.deferFirst = true
	var previous []byte
	denseBefore := false
	for pg, meta := range k.index.pages {
		if err = k.decode(pg); err != nil {
			return g, pages, err
		}
		// Sequential builds need only the preceding page for order validation.
		log.spans[0] = log.spans[len(log.spans)-1]
		clear(log.spans[1:])
		log.spans = log.spans[:1]
		pages++
		if pg == 0 {
			g.base = bytes.Clone(k.page.key(0))
		}
		g.last = bytes.Clone(k.page.key(k.page.len() - 1))
		changes := 0
		prev := previous
		for j := range k.page.len() {
			dir := directory(k.page.key(j))
			if prev != nil && !bytes.Equal(prev, dir) {
				changes++
			}
			prev = dir
		}
		if changes > opt.MaxChangesPerPage {
			g.add(meta.first, k.page.key(0))
			g.runs[len(g.runs)-1].size = ^g.runs[len(g.runs)-1].size
			g.runs[len(g.runs)-1].denseThrough++
			g.dense = true
			denseBefore = true
		} else {
			for j := range k.page.len() {
				key := k.page.key(j)
				dir := directory(key)
				if pg == 0 && j == 0 || denseBefore && j == 0 || !bytes.Equal(previous, dir) {
					g.add(meta.first+int64(j), key)
				}
				previous = dir
			}
			denseBefore = false
		}
		previous = bytes.Clone(directory(k.page.key(k.page.len() - 1)))
		// The candidate cannot be admitted. Bound temporary storage too; the
		// caller treats this as an over-budget candidate, not a failed group.
		// A flat run retains nothing if the entire group stays flat. Finish
		// decoding before deciding, even when its temporary anchor is large.
		flatSoFar := len(g.runs) == 1 && g.runs[0].size >= 0 && len(directory(g.base)) == 0
		if !flatSoFar && g.memoryBytes() > opt.MaxBytes {
			return g, pages, nil
		}
	}
	// Flat groups need no directory information at all.
	if len(g.runs) == 1 && g.runs[0].size >= 0 && len(directory(g.base)) == 0 {
		g.runs = nil
		g.data = nil
		return g, pages, nil
	}
	g.last = bytes.Clone(k.page.key(k.page.len() - 1))
	g.base = bytes.Clone(g.base)
	g.data = append(make([]byte, 0, len(g.data)), g.data...)
	g.runs = append(make([]listBoundary, 0, len(g.runs)), g.runs...)
	return g, pages, nil
}
func (x *listIndex) group(ref groupRef) *listGroup {
	i := sort.Search(len(x.groups), func(i int) bool { return !x.groups[i].g.less(ref) })
	if i < len(x.groups) && x.groups[i].g == ref {
		return &x.groups[i]
	}
	return nil
}
func (g *listGroup) hasPrefix(i int, p []byte) bool {
	b := g.runs[i]
	if b.size < 0 || b.shared+b.size < len(p) {
		return false
	}
	for j, c := range p {
		var v byte
		if j < b.shared {
			v = g.base[j]
		} else {
			v = g.data[b.off+j-b.shared]
		}
		if v != c {
			return false
		}
	}
	return true
}

// compare uses the compact anchor directly, without allocating a decoded key.
func (g *listGroup) compare(i int, key []byte) int {
	b := g.runs[i]
	size := b.size
	if size < 0 {
		size = ^size
	}
	n := b.shared + size
	for j := 0; j < min(n, len(key)); j++ {
		v := byte(0)
		if j < b.shared {
			v = g.base[j]
		} else {
			v = g.data[b.off+j-b.shared]
		}
		if v < key[j] {
			return -1
		}
		if v > key[j] {
			return 1
		}
	}
	if n < len(key) {
		return -1
	}
	if n > len(key) {
		return 1
	}
	return 0
}

// A prefix continuation can start from the run containing its predecessor,
// without first decoding all ambiguous pages before it. Dense groups decline.
func (s *scan) startListCursor(prefix, from []byte) (bool, error) {
	if s.listIndex == nil || len(s.listIndex.groups) == 0 {
		return false, nil
	}
	if len(prefix) == 0 || prefix[len(prefix)-1] != '/' {
		s.stats.ListIndexUnsafeRun++
		return false, nil
	}
	after, ok := successor(prefix)
	if !ok || !bytes.Equal(after, from) {
		return false, nil
	}
	ref, err := s.r.groupFor(s.ctx, from, true, &s.log, s.stats)
	if err != nil {
		return false, err
	}
	g := s.listIndex.group(ref)
	if g == nil {
		s.stats.ListIndexMissingGroup++
		return false, nil
	}
	if g.dense {
		s.stats.ListIndexDensePage++
		return false, nil
	}
	i := sort.Search(len(g.runs), func(i int) bool { return g.compare(i, from) >= 0 }) - 1
	if i < 0 || !g.hasPrefix(i, prefix) {
		s.stats.ListIndexUnsafeRun++
		return false, nil
	}
	s.g, s.kp = ref, nil
	s.land(g.key(i), g.runs[i].row)
	s.synthetic = true
	return s.skipList(prefix, '/')
}

func (g *listGroup) key(i int) []byte {
	b := g.runs[i]
	key := make([]byte, b.shared+b.size)
	copy(key, g.base[:b.shared])
	copy(key[b.shared:], g.data[b.off:b.off+b.size])
	return key
}

// firstOutside uses sorted exact anchors and cumulative dense-marker counts.
// A dense marker in the skipped interval forces ordinary decoding.
func (g *listGroup) firstOutside(ctx context.Context, start int, prefix []byte) (int, error) {
	target, ok := successor(prefix)
	lo, hi := start, len(g.runs)
	for lo < hi {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		mid := int(uint(lo+hi) >> 1)
		if !ok || g.compare(mid, target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Include the destination marker: it is not a certified anchor either.
	end := min(lo+1, len(g.runs))
	before := 0
	if start > 0 {
		before = g.runs[start-1].denseThrough
	}
	if end > start && g.runs[end-1].denseThrough != before {
		return -1, nil
	}
	return lo, nil
}

// skipList routes over whole groups by exact first keys, then searches run
// anchors in the destination group. Logical position survives synthetic moves.
func (s *scan) skipList(prefix []byte, delim byte) (bool, error) {
	if s.listIndex == nil || len(s.listIndex.groups) == 0 {
		return false, nil
	}
	if delim != '/' {
		s.stats.ListIndexDelimiter++
		return false, nil
	}
	if err := s.ctx.Err(); err != nil {
		return false, err
	}
	current := s.listIndex.group(s.g)
	if current == nil {
		s.stats.ListIndexMissingGroup++
		return false, nil
	}
	start := sort.Search(len(current.runs), func(i int) bool { return current.runs[i].row > s.row }) - 1
	if start < 0 || current.runs[start].size < 0 {
		s.stats.ListIndexDensePage++
		return false, nil
	}
	if !current.hasPrefix(start, prefix) {
		s.stats.ListIndexUnsafeRun++
		return false, nil
	}
	target, exists := successor(prefix)
	ref := s.r.end()
	var err error
	if exists {
		ref, err = s.r.groupFor(s.ctx, target, true, &s.log, s.stats)
		if err != nil {
			return false, err
		}
	}
	if err = s.ctx.Err(); err != nil {
		return false, err
	}
	if ref.part >= len(s.r.parts) {
		s.key = nil
		s.bound = s.r.rows
		s.stats.ListIndexSkips++
		return true, nil
	}
	if ref.less(s.g) {
		s.stats.ListIndexUnsafeRun++
		return false, nil
	}
	g := s.listIndex.group(ref)
	if g == nil {
		s.stats.ListIndexMissingGroup++
		return false, nil
	}
	if ref != s.g {
		start = 0
	}
	i, err := g.firstOutside(s.ctx, start, prefix)
	if err != nil {
		return false, err
	}
	if i < 0 {
		s.stats.ListIndexDensePage++
		return false, nil
	}
	// The predecessor run must certify the interior in which target falls.
	if i > start && !g.hasPrefix(i-1, prefix) {
		s.stats.ListIndexUnsafeRun++
		return false, nil
	}
	if i == len(g.runs) {
		ref = ref.next()
		if ref.index == len(s.r.parts[ref.part].groups) {
			ref = groupRef{ref.part + 1, 0}
		}
		if ref.part >= len(s.r.parts) {
			s.key = nil
			s.bound = s.r.rows
			s.stats.ListIndexSkips++
			return true, nil
		}
		g = s.listIndex.group(ref)
		i = 0
		if g == nil {
			s.stats.ListIndexMissingGroup++
			return false, nil
		}
		if len(g.runs) == 0 || g.runs[0].size < 0 {
			s.stats.ListIndexDensePage++
			return false, nil
		}
	}
	if err = s.ctx.Err(); err != nil {
		return false, err
	}
	key := g.key(i)
	if bytes.Compare(key, target) < 0 {
		s.stats.ListIndexUnsafeRun++
		return false, nil
	}
	s.g, s.kp = ref, nil
	s.land(key, g.runs[i].row)
	s.synthetic = true
	if s.key != nil {
		s.groupKey, s.groupStart = s.key, s.ordinal()
	}
	s.stats.ListIndexSkips++
	return true, nil
}
