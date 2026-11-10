// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
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
// path. BudgetExhausted means the next group would exceed MaxBytes.
type ListIndexStats struct {
	GroupsIndexed, GroupsSkipped, GroupsFailed, PartsFailed int64
	Pages, Entries, Bytes                                   int64
	BudgetExhausted                                         bool
	Duration                                                time.Duration
}

type listBoundary struct {
	row               int64
	off, size, shared int
}
type listGroup struct {
	g          groupRef
	dense      bool
	base, data []byte
	last       []byte         // build-only boundary check; cleared before publication
	runs       []listBoundary // size -1 denotes a dense page, whose keys must be decoded
}
type listIndex struct {
	groups []listGroup
	stats  ListIndexStats
}

// BuildListIndex decodes every key page once to build a bounded, optional index
// of directory runs for List with delimiter '/'. It does not run at open.
// Complete groups are admitted in dataset order; the first over-budget group
// stops admission. An unreadable or disordered group is omitted, not certified:
// subsequent reads still perform their ordinary checks and report their errors.
// Publication is atomic: concurrent reads use either the old path or the whole
// completed index. Cancellation discards this attempt and permits retry. After
// a nonempty successful build (including budget exhaustion), later calls return its stats
// without rebuilding, regardless of options. Close cancels the build.
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
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return stats, err
	}
	defer end()
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
	if prior := r.listIndex.Load(); prior != nil {
		return prior.stats, nil
	}
	started := time.Now()
	defer func() {
		stats.Duration = time.Since(started)
		if err != nil {
			stats.Bytes = 0
			stats.Entries = 0
			stats.GroupsIndexed = 0
		}
		err = finish(ctx, rctx, err)
	}()
	built := &listIndex{}
	var readStats ReadStats
	var previousLast []byte
	var previousRef groupRef
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
			stats.PartsFailed++
			err = nil
			continue
		}
		for start := 0; start < len(p.groups); {
			if err = rctx.Err(); err != nil {
				return stats, err
			}
			n := min(opt.Workers, len(p.groups)-start)
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
			if err = rctx.Err(); err != nil {
				return stats, err
			}
			for _, out := range results {
				stats.Pages += out.pages
			}
			for oi, out := range results {
				if out.err != nil {
					stats.GroupsSkipped++
					stats.GroupsFailed++
					previousLast = nil
					continue
				}
				if len(out.group.runs) == 0 {
					previousLast = nil
					stats.GroupsSkipped++
					continue
				}
				if previousLast != nil && previousRef.part == out.group.g.part && previousRef.index+1 == out.group.g.index && r.outOfOrder(previousLast, out.group.base) {
					// Neither side of a bad group boundary may certify a skip.
					last := built.groups[len(built.groups)-1]
					stats.Bytes -= last.memoryBytes()
					stats.Entries -= int64(len(last.runs))
					stats.GroupsIndexed--
					built.groups[len(built.groups)-1] = listGroup{}
					built.groups = built.groups[:len(built.groups)-1]
					if len(built.groups) == 0 {
						stats.Bytes -= 256
					}
					stats.GroupsFailed += 2
					stats.GroupsSkipped += 2
					previousLast = nil
					continue
				}
				previousLast, previousRef = out.group.last, out.group.g
				out.group.last = nil
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
			}
			start += n
		}
	}
	if err = rctx.Err(); err != nil {
		return stats, err
	}
	built.groups = slices.Clip(slices.Clone(built.groups))
	stats.Duration = time.Since(started)
	built.stats = stats
	if len(built.groups) > 0 {
		r.listIndex.Store(built)
	}
	return stats, nil
}

// Conservative allocation accounting includes capacity, allocator rounding and
// each group's slot in the snapshot. No maps or per-page arrays are retained.
func (g *listGroup) memoryBytes() int64 {
	return 256 + int64(cap(g.base)+cap(g.data)) + int64(cap(g.runs))*32
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
	g.runs = append(g.runs, b)
}
func directory(key []byte) []byte { return key[:bytes.LastIndexByte(key, '/')+1] }

func (r *reader) buildListGroup(ctx context.Context, ref groupRef, opt ListIndexOptions) (g listGroup, pages int64, err error) {
	g.g = ref
	st := new(ReadStats)
	defer func() { pages = st.KeyPagesDecoded }()
	log := pageLog{unique: r.key.Unique}
	k, err := r.keyPages(ctx, ref, &log, st)
	if err != nil {
		return g, 0, err
	}
	var previous []byte
	denseBefore := false
	for pg, meta := range k.index.pages {
		if err = k.decode(pg); err != nil {
			return g, pages, err
		}
		pages++
		if pg == 0 {
			g.base = bytes.Clone(k.values[0].ByteArray())
		}
		changes := 0
		prev := previous
		for _, v := range k.values {
			dir := directory(v.ByteArray())
			if prev != nil && !bytes.Equal(prev, dir) {
				changes++
			}
			prev = dir
		}
		if changes > opt.MaxChangesPerPage {
			g.add(meta.first, nil)
			denseBefore = true
		} else {
			for j, v := range k.values {
				key := v.ByteArray()
				dir := directory(key)
				if pg == 0 && j == 0 || denseBefore && j == 0 || !bytes.Equal(previous, dir) {
					g.add(meta.first+int64(j), key)
				}
				previous = dir
			}
			denseBefore = false
		}
		previous = bytes.Clone(directory(k.values[len(k.values)-1].ByteArray()))
		// The candidate cannot be admitted. Bound temporary storage too; the
		// caller treats this as an over-budget candidate, not a failed group.
		if g.memoryBytes() > opt.MaxBytes {
			return g, pages, nil
		}
	}
	// Flat groups need no directory information at all.
	if len(g.runs) == 1 && g.runs[0].size >= 0 && len(directory(g.base)) == 0 {
		return listGroup{}, pages, nil
	}
	g.last = bytes.Clone(k.values[len(k.values)-1].ByteArray())
	g.base = bytes.Clone(g.base)
	g.data = bytes.Clone(g.data)
	g.runs = slices.Clone(g.runs)
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
	n := b.shared + b.size
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
	if s.listIndex == nil {
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
	if ok, err := s.open(ref); err != nil || !ok {
		return ok, err
	}
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

// skipList walks run starts from the logical row, independent of the last
// decoded page. Each step checks cancellation, uses no page search and allocates
// nothing. Only the final exact anchor is materialized.
func (s *scan) skipList(prefix []byte, delim byte) (bool, error) {
	if s.listIndex == nil {
		return false, nil
	}
	if delim != '/' {
		s.stats.ListIndexDelimiter++
		return false, nil
	}
	ref, row := s.g, s.row
	first := true
	for {
		if err := s.ctx.Err(); err != nil {
			return false, err
		}
		if ref.part >= len(s.r.parts) {
			s.key = nil
			s.bound = s.r.rows
			s.stats.ListIndexSkips++
			return true, nil
		}
		p := s.r.parts[ref.part]
		if !p.ready.Load() {
			s.stats.ListIndexMissingGroup++
			return false, nil
		}
		if ref.index >= len(p.groups) {
			ref = groupRef{ref.part + 1, 0}
			row = 0
			continue
		}
		g := s.listIndex.group(ref)
		if g == nil {
			s.stats.ListIndexMissingGroup++
			return false, nil
		}
		i := sort.Search(len(g.runs), func(i int) bool { return g.runs[i].row > row }) - 1
		for ; i < len(g.runs); i++ {
			if err := s.ctx.Err(); err != nil {
				return false, err
			}
			if i < 0 || g.runs[i].size < 0 {
				s.stats.ListIndexDensePage++
				return false, nil
			}
			if !g.hasPrefix(i, prefix) {
				if first {
					s.stats.ListIndexUnsafeRun++
					return false, nil
				}
				key := g.key(i)
				// A run beginning before the successor but outside P cannot certify
				// its interior. This includes a parent directory's interleaved objects.
				if bytes.Compare(key, prefix) < 0 {
					s.stats.ListIndexUnsafeRun++
					return false, nil
				}
				if ok, err := s.open(ref); err != nil || !ok {
					if err != nil {
						return false, err
					}
					s.stats.ListIndexSkips++
					return true, nil
				}
				s.land(key, g.runs[i].row)
				s.synthetic = true
				if s.key != nil {
					s.groupKey, s.groupStart = s.key, s.ordinal()
				}
				s.stats.ListIndexSkips++
				return true, nil
			}
			first = false
		}
		ref = ref.next()
		row = 0
	}
}
