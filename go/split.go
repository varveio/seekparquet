// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"sort"
)

// Split divides the plan into at most n shards, for scans run
// concurrently: plans over consecutive sub-ranges of its rows, bound to the
// same Dataset, in dataset order. Scanning every shard yields exactly the
// rows of the plan, each once, with the same offsets and values; within a
// shard in dataset order, and across shards by shard index. A plan with no
// rows, or n below 1, has no shard; a plan with fewer row groups than n
// has fewer shards.
//
// A boundary between two shards is the first row of a row group of the
// plan (or of a part no read has checked yet, whose row groups are not
// known), never inside a group of equal keys: when keys are not unique and
// a group continues from the row group before, the boundary moves to the
// next row group that begins a group. The first shard begins and the last
// ends where the plan does, at its ranges' bounds and cursors. Boundaries
// are placed to balance ProjectedBytes when the plan has columns and its
// bytes are exact, and the row count otherwise.
//
// The shards' Counts add up to the plan's. Their ProjectedBytes add up to
// the plan's, since each row group's pages and dictionaries are counted in
// the one shard holding the group; a shard's bytes are exact when the
// plan's are. Shards share the Dataset's checked parts and page indexes as
// any concurrent requests do; each Scan takes its own context and
// ReadStats, and a part a read refuses is refused to every shard touching
// it.
//
// Splitting reads nothing when keys are unique. When they are not, it
// decodes the two key pages around each boundary inside a part, checking
// them as a Range does (CHECK-2), to keep groups whole; a shard's Stats are
// the reads that placed its end. Splitting is a convenience of
// this reader, not a rule of the format.
func (plan *RangePlan) Split(ctx context.Context, n int) (_ []*RangePlan, err error) {
	if n < 1 || len(plan.spans) == 0 {
		return nil, nil
	}
	r := plan.r
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = finish(ctx, rctx, err)
		end()
	}()
	cuts, stats, err := plan.cuts(rctx, n)
	if err != nil {
		return nil, err
	}
	shards := make([]*RangePlan, 0, len(cuts)+1)
	lo := int64(0)
	for i := range len(cuts) + 1 {
		hi := r.rows
		if i < len(cuts) {
			hi = cuts[i]
		}
		shard := plan.shard(lo, hi)
		shard.stats = *stats[i]
		shards = append(shards, shard)
		lo = hi
	}
	return shards, nil
}

// cuts returns at most n-1 shard boundaries, ascending dataset ordinals
// each with rows of the plan on both sides, and the read statistics of the
// shards they divide the plan into.
func (plan *RangePlan) cuts(ctx context.Context, n int) ([]int64, []*ReadStats, error) {
	r, spans := plan.r, plan.spans
	first, last := spans[0].start, spans[len(spans)-1].end
	// The candidates are the row groups' first rows, or a part's first row
	// when no read has checked it, with rows of the plan on both sides.
	var candidates []int64
	for i := r.partAt(first); i < len(r.parts) && r.parts[i].start < last; i++ {
		p := r.parts[i]
		if !p.ready.Load() {
			if p.start > first {
				candidates = append(candidates, p.start)
			}
			continue
		}
		for _, grp := range p.groups {
			if grp.start > first && grp.start < last {
				candidates = append(candidates, grp.start)
			}
		}
	}
	// rows[j] is the plan's rows before candidate j, weight[j] what the
	// boundaries balance: the projected bytes of the row groups before it,
	// or rows.
	rows := make([]int64, len(candidates))
	for j, k, before := 0, 0, int64(0); j < len(candidates); j++ {
		c := candidates[j]
		for ; k < len(spans) && spans[k].end <= c; k++ {
			before += spans[k].end - spans[k].start
		}
		rows[j] = before
		if k < len(spans) && spans[k].start < c {
			rows[j] += c - spans[k].start
		}
	}
	weight, total := rows, plan.rows
	if len(plan.columns) > 0 && !plan.partial && plan.bytes > 0 {
		weight, total = make([]int64, len(candidates)), plan.bytes
		for j, k, before := 0, 0, int64(0); j < len(candidates); j++ {
			for ; k < len(plan.groups) && plan.groups[k].start < candidates[j]; k++ {
				before += plan.groups[k].bytes
			}
			weight[j] = before
		}
	}
	// chosen are the candidates nearest the even shares of the weight.
	var chosen []int
	n = min(n, len(candidates)+1)
	for k := 1; k < n; k++ {
		target := float64(total) * float64(k) / float64(n)
		j := sort.Search(len(candidates), func(j int) bool { return float64(weight[j]) >= target })
		if j > 0 && (j == len(candidates) || target-float64(weight[j-1]) < float64(weight[j])-target) {
			j--
		}
		if m := len(chosen); m > 0 && rows[j] <= rows[chosen[m-1]] {
			continue
		}
		chosen = append(chosen, j)
	}
	// Each boundary inside a group of equal keys moves to the next
	// candidate that is not, before the next boundary, or is dropped. The
	// reads of placing a boundary are the stats of the shard it ends.
	cuts := make([]int64, 0, len(chosen))
	stats := []*ReadStats{new(ReadStats)}
	s := r.newScan(ctx, nil, stats[0])
	for i, j := range chosen {
		next := len(candidates)
		if i+1 < len(chosen) {
			next = chosen[i+1]
		}
		before := func(j int) bool { return j < next && (next == len(candidates) || rows[j] < rows[next]) }
		for ; before(j); j++ {
			inside, err := plan.insideGroup(s, candidates[j])
			if err != nil {
				return nil, nil, err
			}
			if !inside {
				break
			}
		}
		if before(j) {
			cuts = append(cuts, candidates[j])
			stats = append(stats, new(ReadStats))
			s.stats = stats[len(stats)-1]
		}
	}
	return cuts, stats, nil
}

// insideGroup reports whether row c, the first of its row group, and the
// row before it are rows of the plan with equal keys, so that a boundary
// at c would divide their group. Only when keys are not unique and the two
// rows are in one part (PART-4) does it decode the key pages holding them.
func (plan *RangePlan) insideGroup(s *scan, c int64) (bool, error) {
	r := plan.r
	if r.key.Unique || r.parts[r.partAt(c)].start == c {
		return false, nil
	}
	k := sort.Search(len(plan.spans), func(k int) bool { return plan.spans[k].end > c })
	if k == len(plan.spans) || plan.spans[k].start >= c {
		return false, nil
	}
	if err := s.seekOrdinal(c - 1); err != nil {
		return false, err
	}
	before := bytes.Clone(s.key)
	if err := s.next(); err != nil {
		return false, err
	}
	return bytes.Equal(before, s.key), nil
}

// shard returns the plan's rows in [lo, hi), which are the first rows of
// row groups (or lo is 0 and hi the dataset's row count), with the
// projected bytes of the row groups holding them.
func (plan *RangePlan) shard(lo, hi int64) *RangePlan {
	shard := &RangePlan{r: plan.r, columns: plan.columns}
	for _, sp := range plan.spans {
		if sp.end <= lo || sp.start >= hi {
			continue
		}
		if sp.start < lo {
			sp = span{ordinal: true, start: lo, end: sp.end}
		}
		sp.end = min(sp.end, hi)
		shard.spans = append(shard.spans, sp)
		shard.rows += sp.end - sp.start
	}
	for _, gb := range plan.groups {
		if gb.start >= lo && gb.start < hi {
			shard.groups = append(shard.groups, gb)
			shard.bytes += gb.bytes
			shard.partial = shard.partial || gb.partial
		}
	}
	shard.exact = shard.checked()
	return shard
}
