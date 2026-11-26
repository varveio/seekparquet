// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"cmp"
	"context"
	"slices"
	"sort"
)

// rowWork is the work a fold of row spans asks of their admission.
type rowWork struct {
	// columns is the projection the work reads.
	columns []string
	// budget is the value budget, zero for unlimited. It applies only to
	// work with columns.
	budget int64
	// workers bounds the key-checking scan of checkRefusedKeys.
	workers int
	// checkKeys requests that every selected key is checked.
	checkKeys bool
}

// rowAdmission is the outcome of admitting row spans' work.
type rowAdmission struct {
	// status is each span's admission. FoldUnknown: its row count is not
	// exact and keys are not checked, so it has no work. FoldExact: it is
	// empty or its work is admitted. FoldBudgetExceeded or FoldUnpriced:
	// the value budget refused its work.
	status []FoldStatus
	// counts is each span's planned row count (Plan.Count).
	counts []int64
	// candidates are the spans holding rows whose status is not
	// FoldUnknown, in ascending order of their starts.
	candidates []int
	// selected are the candidates whose rows plan scans, in the order of
	// plan's inputs.
	selected []int
	// plan selects the rows of selected with the work's columns; it is
	// nil or selects no rows when selected is empty.
	plan *Plan
}

// admitRows plans each span alone, without columns, to learn its count
// and whether it is exact, and then admits the work of the spans that
// hold rows. Work with columns and a finite budget is admitted by
// budgetValues; otherwise every candidate is admitted.
func (d *Dataset) admitRows(ctx context.Context, spans []RowSpan, w rowWork, st *ReadStats) (*rowAdmission, error) {
	a := &rowAdmission{status: make([]FoldStatus, len(spans)), counts: make([]int64, len(spans))}
	for i, sp := range spans {
		p, err := d.PlanRows(ctx, []RowSpan{sp}, nil, nil)
		if err != nil {
			return nil, err
		}
		n, exact := p.Count()
		a.counts[i] = n
		if !exact && !w.checkKeys {
			continue
		}
		a.status[i] = FoldExact
		if n != 0 {
			a.candidates = append(a.candidates, i)
		}
	}
	slices.SortStableFunc(a.candidates, func(x, y int) int {
		return cmp.Compare(spans[x].Start.ordinal, spans[y].Start.ordinal)
	})

	if len(w.columns) > 0 && w.budget > 0 {
		plan, selected, status, err := d.budgetValues(ctx, spans, a.candidates, w.columns, w.budget, st)
		if err != nil {
			return nil, err
		}
		for _, i := range a.candidates {
			a.status[i] = status[i]
		}
		a.plan, a.selected = plan, selected
		return a, nil
	}

	rows := make([]RowSpan, len(a.candidates))
	for k, i := range a.candidates {
		rows[k] = spans[i]
	}
	var planned ReadStats
	plan, err := d.PlanRows(ctx, rows, w.columns, &planned)
	st.Add(planned)
	if err != nil {
		return nil, err
	}
	a.plan, a.selected = plan, a.candidates
	return a, nil
}

// checkRefusedKeys scans the keys of every span when w checks keys and
// the budget refused a candidate's value work: refusing the values does
// not remove the obligation to check the keys.
func (d *Dataset) checkRefusedKeys(ctx context.Context, spans []RowSpan, a *rowAdmission, w rowWork, st *ReadStats) error {
	if !w.checkKeys || len(a.selected) == len(a.candidates) {
		return nil
	}
	var planned ReadStats
	p, err := d.PlanRows(ctx, spans, nil, &planned)
	st.Add(planned)
	if err != nil {
		return err
	}
	var scanned ReadStats
	err = p.ScanParallel(ctx, w.workers, &scanned, func(*Batch) error { return nil })
	st.Add(scanned)
	return err
}

// minValuePageBytes is the least value work a part holding a projected
// column is charged before its metadata prices it: the floor of
// partValueFloor.
const minValuePageBytes = 16

// budgetValues is swath budgetSums: discover candidates part by part, abandon
// unaffordable candidates before opening later parts, and price the trial plan
// before selectRows can discard transient locations. Metadata is uncharged.
func (d *Dataset) budgetValues(ctx context.Context, spans []RowSpan, candidates []int, columns []string, budget int64, st *ReadStats) (*Plan, []int, []FoldStatus, error) {
	parts := d.r.parts
	ends := make([]int64, len(parts))
	for i, p := range parts {
		ends[i] = p.start + p.rows
	}
	charged := make([]bool, len(parts))
	status := make([]FoldStatus, len(spans))
	var selected []int
	var rows []RowSpan
	var accepted *Plan
	var used int64
	for _, i := range candidates {
		t := spans[i]
		start, end := t.Start.ordinal, t.End.ordinal
		first := sort.Search(len(ends), func(j int) bool { return ends[j] > start })
		last := sort.Search(len(ends), func(j int) bool { return ends[j] >= end })
		floor := int64(0)
		partFloors := make([]int64, last-first+1)
		for j := first; j <= last; j++ {
			if !charged[j] {
				partFloors[j-first] = partValueFloor(parts[j], columns)
				floor += partFloors[j-first]
			}
		}
		trial := slices.Clone(rows)
		from := t.Start
		cost := used
		var universe *Plan
		fits := true
		status[i] = FoldBudgetExceeded
		for j := first; j <= last; j++ {
			if floor > budget-cost {
				fits = false
				break
			}
			to := t.End
			if j < last {
				to = d.r.pos(ends[j])
			}
			trial = append(trial, RowSpan{from, to})
			var planned ReadStats
			p, err := d.PlanRows(ctx, trial, columns, &planned)
			st.Add(planned)
			if err != nil {
				return nil, nil, nil, err
			}
			universe = p
			var exact bool
			cost, exact = p.ProjectedBytes()
			if !exact {
				status[i] = FoldUnpriced
				fits = false
				break
			}
			if cost > budget {
				fits = false
				break
			}
			if !charged[j] {
				// Opening a part may reveal that every projected column is absent.
				// Remove its original floor, before that metadata was known.
				floor -= partFloors[j-first]
			}
			from = to
		}
		if fits {
			selected, rows = append(selected, i), append(rows, t)
			accepted, used = universe, cost
			status[i] = FoldExact
			for j := first; j <= last; j++ {
				charged[j] = true
			}
		}
	}
	if accepted == nil {
		return nil, nil, status, nil
	}
	plan, err := accepted.selectRows(rows)
	return plan, selected, status, err
}

// partValueFloor is the value work of part p charged before its metadata
// is known. Unlike swath's mandatory size column, generic projected
// columns may all be absent in a checked part, whose value work has a
// zero lower bound.
func partValueFloor(p *part, columns []string) int64 {
	if p.ready.Load() {
		for _, name := range columns {
			if _, ok := p.leaves[name]; ok {
				return minValuePageBytes
			}
		}
		return 0
	}
	return minValuePageBytes
}
