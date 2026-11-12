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
// shard in dataset order, and across shards by shard index. n below 1 is
// ErrInvalidRequest; a plan with no rows has no shard; a plan with fewer
// row groups than n has fewer shards.
//
// A boundary between two shards is a selected row that is the first row
// of a row group (or of a part no read has checked yet, whose row groups
// are not known), never inside a group of equal keys: when keys are not unique and
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
// it. Each shard's Columns are the plan's.
//
// Splitting reads nothing when keys are unique. When they are not, it
// decodes the two key pages around each boundary inside a part, checking
// them as Rows does (CHECK-2), to keep groups whole. st, when not nil, is
// reset and receives the reads of splitting, whether it succeeds or fails.
// A cancelled ctx, or a Close, fails a split whether or not it reads.
// Splitting is a convenience of this reader, not a rule of the format.
func (plan *Plan) Split(ctx context.Context, n int, st *ReadStats) (_ []*Plan, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if n < 1 {
		return nil, invalidRequest("split into %d shards", n)
	}
	r := plan.r
	rctx, end, err := r.begin(ctx, st)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = finish(ctx, rctx, err)
		end()
	}()
	// Splitting reads nothing when keys are unique, so it observes its
	// context itself: on entry and through its loops over the plan.
	if err := rctx.Err(); err != nil {
		return nil, err
	}
	if len(plan.spans) == 0 {
		return nil, nil
	}
	cuts, err := plan.cuts(rctx, n, st)
	if err != nil {
		return nil, err
	}
	shards := make([]*Plan, 0, len(cuts)+1)
	lo := int64(0)
	for i := range len(cuts) + 1 {
		if err := rctx.Err(); err != nil {
			return nil, err
		}
		hi := r.rows
		if i < len(cuts) {
			hi = cuts[i]
		}
		shards = append(shards, plan.shard(lo, hi))
		lo = hi
	}
	return shards, nil
}

// cuts returns at most n-1 shard boundaries, ascending dataset ordinals
// each with rows of the plan on both sides, counting its reads in st.
func (plan *Plan) cuts(ctx context.Context, n int, st *ReadStats) ([]int64, error) {
	r, spans := plan.r, plan.spans
	first := spans[0].start
	// The candidates are the selected rows that begin a row group, or a
	// part no read has checked, after the plan's first row.
	var candidates []int64
	for _, sp := range spans {
		for i := r.partAt(sp.start); i < len(r.parts) && r.parts[i].start < sp.end; i++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			p := r.parts[i]
			if !p.ready.Load() {
				if p.start > first && p.start >= sp.start {
					candidates = append(candidates, p.start)
				}
				continue
			}
			for j := sort.Search(len(p.groups), func(j int) bool { return p.groups[j].start >= max(sp.start, first+1) }); j < len(p.groups) && p.groups[j].start < sp.end; j++ {
				candidates = append(candidates, p.groups[j].start)
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
	// candidate that is not, before the next boundary, or is dropped.
	cuts := make([]int64, 0, len(chosen))
	s := r.newScan(ctx, nil, st)
	for i, j := range chosen {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next := len(candidates)
		if i+1 < len(chosen) {
			next = chosen[i+1]
		}
		before := func(j int) bool { return j < next && (next == len(candidates) || rows[j] < rows[next]) }
		for ; before(j); j++ {
			inside, err := plan.insideGroup(s, candidates[j])
			if err != nil {
				return nil, err
			}
			if !inside {
				break
			}
		}
		if before(j) {
			cuts = append(cuts, candidates[j])
		}
	}
	return cuts, nil
}

// insideGroup reports whether row c, the first of its row group, and the
// row before it are rows of the plan with equal keys, so that a boundary
// at c would divide their group. Only when keys are not unique and the two
// rows are in one part (PART-4) does it decode the key pages holding them.
func (plan *Plan) insideGroup(s *scan, c int64) (bool, error) {
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
// projected bytes of the row groups holding them. The plan's spans and
// row groups are ascending and disjoint, so it visits only those in
// [lo, hi), after a binary search: many shards cost no more than few.
func (plan *Plan) shard(lo, hi int64) *Plan {
	shard := &Plan{r: plan.r, columns: plan.columns}
	k := sort.Search(len(plan.spans), func(k int) bool { return plan.spans[k].end > lo })
	for _, sp := range plan.spans[k:] {
		if sp.start >= hi {
			break
		}
		if sp.start < lo {
			sp = span{ordinal: true, start: lo, end: sp.end}
		}
		sp.end = min(sp.end, hi)
		shard.spans = append(shard.spans, sp)
		shard.rows += sp.end - sp.start
	}
	g := sort.Search(len(plan.groups), func(g int) bool { return plan.groups[g].start >= lo })
	for _, gb := range plan.groups[g:] {
		if gb.start >= hi {
			break
		}
		shard.groups = append(shard.groups, gb)
		shard.bytes += gb.bytes
		shard.partial = shard.partial || gb.partial
	}
	shard.exact = shard.checked()
	return shard
}
