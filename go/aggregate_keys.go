// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// aggregateKeySpan is swath totals' keySpan with generic exclusive starts and
// row cursors. after=-1 includes the whole first key, otherwise skips offsets
// through after. MaxInt64 excludes the whole first key.
type aggregateKeySpan struct {
	lo, hi []byte
	after  int64
	keyEnd []byte
}

func (s aggregateKeySpan) contains(t aggregateKeySpan) bool {
	cmp := bytes.Compare(s.lo, t.lo)
	return (cmp < 0 || cmp == 0 && s.after <= t.after) && (s.hi == nil || t.hi != nil && bytes.Compare(t.hi, s.hi) <= 0)
}

func aggregateKeySpans(selections []Selection) ([]aggregateKeySpan, []int, []Selection, error) {
	spans := make([]aggregateKeySpan, len(selections))
	order := make([]int, len(spans))
	for i, sel := range selections {
		if err := sel.Cursor.check(false); err != nil {
			return nil, nil, nil, err
		}
		lo, exclusive, hi := sel.bounds()
		after := int64(-1)
		if exclusive {
			after = math.MaxInt64
		}
		if c := sel.Cursor; c != nil {
			cmp := bytes.Compare(c.Key, lo)
			if cmp > 0 || cmp == 0 && after < c.Offset {
				lo, after = c.Key, c.Offset
			}
		}
		spans[i] = aggregateKeySpan{lo: lo, hi: hi, after: after}
		if after >= 0 {
			spans[i].keyEnd = append(bytes.Clone(lo), 0)
		}
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		s, t := spans[a], spans[b]
		if c := bytes.Compare(s.lo, t.lo); c != 0 {
			return c
		}
		if s.after < t.after {
			return -1
		}
		if s.after > t.after {
			return 1
		}
		if s.hi == nil {
			if t.hi == nil {
				return 0
			}
			return -1
		}
		if t.hi == nil {
			return 1
		}
		return -bytes.Compare(s.hi, t.hi)
	})
	var outer []aggregateKeySpan
	var selected []Selection
	for _, i := range order {
		s := spans[i]
		if s.hi != nil && bytes.Compare(s.lo, s.hi) >= 0 {
			continue
		}
		if len(outer) > 0 && outer[len(outer)-1].contains(s) {
			continue
		}
		outer = append(outer, s)
		sel := Selection{Span: Span{Start: s.lo, End: s.hi}}
		if s.after == math.MaxInt64 {
			sel.StartExclusive = true
		} else if s.after >= 0 {
			sel.Cursor = &Cursor{Key: s.lo, Offset: s.after}
		}
		selected = append(selected, sel)
	}
	return spans, order, selected, nil
}

// aggregateKeys is swath totals.sumSpans: plan only outer selections, distribute
// ordered key runs to active inputs, merge worker sums and check shard keys.
func (d *Dataset) aggregateKeys(ctx context.Context, selections []Selection, reductions []Reduction, opt AggregateOptions, columns []string, slots []int, st *ReadStats, located []RowSpan) ([][]AggregateResult, error) {
	spans, order, outer, err := aggregateKeySpans(selections)
	if err != nil {
		return nil, err
	}
	var planned ReadStats
	p, err := d.Plan(ctx, outer, columns, &planned)
	st.Add(planned)
	if err != nil {
		return nil, err
	}
	types, err := d.reductionTypes(p, reductions)
	if err != nil {
		return nil, err
	}
	m := len(reductions)
	totals := make([]reductionSum, len(spans)*m)
	var mu sync.Mutex
	err = p.scanParallel(ctx, opt.Workers, st, true, func() shardScan {
		sums := make([]reductionSum, len(totals))
		var positions []rowRange
		var seen []bool
		if located != nil {
			positions = make([]rowRange, len(spans))
			seen = make([]bool, len(spans))
		}
		run := make([]reductionSum, m)
		var keep []bool
		projected := ValueBatch{}
		filterSlots := aggregateFilterSlots(p.columns, opt.Filter)
		if opt.Filter != nil {
			projected.values = make([][]parquet.Value, len(opt.Filter.Columns))
			projected.kinds = make([]Kind, len(opt.Filter.Columns))
		}
		s := aggregateSpanScan{spans: spans, order: order, active: make([]int, 0, len(spans))}
		return func(ctx context.Context, shard *Plan, st *ReadStats, keys *shardKeys) error {
			clear(sums)
			clear(seen)
			s.next = 0
			s.active = s.active[:0]
			var failed error
			err := shard.Scan(ctx, st, func(b *Batch) error {
				keys.noteBatch(b)
				v := ValueBatch{ds: d.r.id, start: b.start, n: b.Len(), values: b.values, kinds: b.kinds}
				keep, failed = applyAggregateFilter(&v, opt.Filter, filterSlots, keep, &projected)
				if failed != nil {
					return failed
				}
				for i, n := 0, b.Len(); i < n; {
					key := b.Key(i)
					end := s.enter(key)
					j := n
					if end != nil {
						j = i + 1 + sort.Search(n-i-1, func(k int) bool { return bytes.Compare(b.Key(i+1+k), end) >= 0 })
					}
					// A row cursor is the only boundary inside an equal-key run. Partition
					// there too, so each row is still reduced once, including repeated keys.
					offset := b.Offset(i)
					for _, a := range s.active {
						sp := spans[a]
						if bytes.Equal(key, sp.lo) && sp.after >= offset {
							j = min(j, i+sort.Search(j-i, func(k int) bool { return !bytes.Equal(b.Key(i+k), key) || b.Offset(i+k) > sp.after }))
						}
					}
					if j == i {
						j = i + 1
					}
					for c := range run {
						run[c].lo, run[c].hi, run[c].maximum, run[c].seen = 0, 0, Value{}, false
					}
					for row := i; row < j; row++ {
						if opt.Filter != nil && !keep[row] {
							continue
						}
						for c, r := range reductions {
							value := Value{}
							if slots[c] >= 0 {
								value = b.Column(slots[c]).Value(row)
							}
							run[c].add(value, r, types[c])
						}
					}
					for _, a := range s.active {
						sp := spans[a]
						if bytes.Equal(key, sp.lo) && offset <= sp.after {
							continue
						}
						if located != nil {
							lo, hi := b.start+int64(i), b.start+int64(j)
							if !seen[a] {
								positions[a] = rowRange{lo, hi}
								seen[a] = true
							} else {
								positions[a].end = hi
							}
						}
						for c, r := range reductions {
							sums[a*m+c].merge(&run[c], r, types[c])
						}
					}
					i = j
				}
				return nil
			})
			if failed != nil {
				return &shardCallbackError{failed}
			}
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for i := range spans {
				if located != nil && seen[i] {
					lo, hi := positions[i].start, positions[i].end
					if located[i].Start.ds != 0 {
						lo = min(lo, located[i].Start.ordinal)
						hi = max(hi, located[i].End.ordinal)
					}
					located[i] = d.r.rowSpan(lo, hi)
				}
				for c, r := range reductions {
					totals[i*m+c].merge(&sums[i*m+c], r, types[c])
				}
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	if located != nil {
		for i := range located {
			if located[i].Start.ds == 0 {
				located[i] = d.r.rowSpan(0, 0)
			}
		}
	}
	out := aggregateResults(len(spans), m)
	for i := range spans {
		for c, r := range reductions {
			out[i][c] = totals[i*m+c].result(r, types[c])
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// aggregateSpanScan.enter is totals.spanScan.enter, with caller-defined bounds.
type aggregateSpanScan struct {
	spans  []aggregateKeySpan
	order  []int
	next   int
	active []int
}

func (s *aggregateSpanScan) enter(key []byte) []byte {
	for ; s.next < len(s.order) && bytes.Compare(s.spans[s.order[s.next]].lo, key) <= 0; s.next++ {
		s.active = append(s.active, s.order[s.next])
	}
	s.active = slices.DeleteFunc(s.active, func(i int) bool { return s.spans[i].hi != nil && bytes.Compare(key, s.spans[i].hi) >= 0 })
	var end []byte
	if s.next < len(s.order) {
		end = s.spans[s.order[s.next]].lo
	}
	for _, i := range s.active {
		sp := s.spans[i]
		if sp.hi != nil && (end == nil || bytes.Compare(sp.hi, end) < 0) {
			end = sp.hi
		}
		if sp.after >= 0 && bytes.Equal(key, sp.lo) {
			// Runs must end after this key before dropping its cursor condition.
			next := sp.keyEnd
			if end == nil || bytes.Compare(next, end) < 0 {
				end = next
			}
		}
	}
	return end
}
