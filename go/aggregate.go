// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"math"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// AggregateOp is a reduction over selected rows.
type AggregateOp uint8

const (
	// CountRows counts rows, including repeated keys and version rows.
	CountRows AggregateOp = iota
	// Sum adds present, non-null integers into an INT64 (unsigned for unsigned input).
	Sum
	// Max selects the greatest present, non-null integer, timestamp or byte string.
	Max
	// CountNulls counts nulls in a present column, excluding absent values.
	CountNulls
	// CountAbsent counts rows whose part lacks the column.
	CountAbsent
)

// Reduction names an operation and its column. Column is empty only for CountRows.
// Sum accepts unannotated integers and INT annotations. Max additionally accepts
// TIMESTAMP integers (in their declared unit), and unannotated or STRING byte
// arrays. Unsupported physical or logical types fail with ErrInvalidRequest
// once the contributing column types are known. Parts not yet checked can leave
// results AggregateUnknown without discovering unsupported types.
// Across contributing parts, a reduced column must keep its signedness and
// integer/timestamp meaning; timestamps must keep their unit and UTC adjustment.
// Incompatible annotations also fail with ErrInvalidRequest once known.
type Reduction struct {
	// Op is the requested reduction.
	Op AggregateOp
	// Column names the input column; empty only for CountRows.
	Column string
}

// AggregateStatus describes whether a result is complete.
type AggregateStatus uint8

const (
	// AggregateUnknown means established boundaries or checked row counts are unavailable.
	AggregateUnknown AggregateStatus = iota
	// AggregateExact means the complete requested reduction is in Value.
	AggregateExact
	// AggregateBudgetExceeded means the input's value work did not fit the budget.
	AggregateBudgetExceeded
	// AggregateUnpriced means the compressed value extents could not be proven.
	AggregateUnpriced
	// AggregateOverflow means the final sum does not fit its signed or unsigned INT64.
	AggregateOverflow
)

// AggregateResult is an owning value and its completeness status. Value is null
// unless Status is AggregateExact. Counts and signed sums are INT64; unsigned
// sums use UINT64 bits in INT64. Max retains the column's physical type and unit.
type AggregateResult struct {
	// Value owns the complete reduction when Status is AggregateExact.
	Value Value
	// Status describes completeness; inexact results carry a null Value.
	Status AggregateStatus
}

// BatchFilter selects rows from a scanned union batch. Only Columns, in order
// (duplicates included), are visible in batch. Apply may modify keep, initially
// all true. Both arguments are borrowed until Apply returns. Apply must be safe
// for concurrent use when Workers > 1. Every callback error, including Stop,
// fails aggregation without results. Predicate pages count against the budget.
type BatchFilter struct {
	// Columns is the predicate projection in callback order.
	Columns []string
	// Apply selects rows by modifying its initially true keep mask.
	Apply func(batch *ValueBatch, keep []bool) error
}

// AggregateOptions controls aggregation execution. Storage owns physical
// concurrency and caching; Workers bounds active scans and filter callbacks.
type AggregateOptions struct {
	// ValueBudgetBytes is the compressed page and dictionary extent budget:
	// zero permits no value I/O, -1 is unlimited, and positive values bound the
	// unique union extents. Planning metadata and implicit validation keys are free.
	ValueBudgetBytes int64
	// Workers <= 0 means one worker.
	Workers int
	// CheckKeys requests selected-key validation, including cross-shard order.
	// Count-only requests then scan keys. Exact value-only results do not certify unread keys.
	CheckKeys bool
	// Filter optionally selects rows using declared projected columns.
	Filter *BatchFilter
}

func aggregateColumns(reductions []Reduction, opt AggregateOptions) ([]string, []int, error) {
	if opt.ValueBudgetBytes < -1 {
		return nil, nil, invalidRequest("a value budget must be -1 or nonnegative")
	}
	var columns []string
	slots := make([]int, len(reductions))
	for i, r := range reductions {
		if r.Op > CountAbsent || (r.Op == CountRows) != (r.Column == "") {
			return nil, nil, invalidRequest("invalid reduction %d", i)
		}
		slots[i] = -1
		if r.Op != CountRows {
			c := slices.Index(columns, r.Column)
			if c < 0 {
				c = len(columns)
				columns = append(columns, r.Column)
			}
			slots[i] = c
		}
	}
	if f := opt.Filter; f != nil {
		if f.Apply == nil {
			return nil, nil, invalidRequest("a nil filter callback")
		}
		for _, name := range f.Columns {
			if !slices.Contains(columns, name) {
				columns = append(columns, name)
			}
		}
	}
	return columns, slots, nil
}

// Aggregate returns results indexed by input selection, then reduction, keeping
// duplicates and overlaps independent. It resolves selections once and scans
// their accepted union for values. Unfiltered counts from established spans
// require no value reads. Value work is admitted together per input in ascending
// row order; shared compressed pages and dictionaries are charged once. Unknown counts
// remain unknown. Sum and Max skip nulls and absent values; empty sums are zero
// and empty maxima null, both exact. All errors return no partial results.
// CheckKeys plans outer selections alone and distributes by key boundaries,
// avoiding endpoint seeks for nested descendants. Finite budgets validate keys
// first, then admit value-only work over the established row positions.
// st is reset and includes planning and execution work on every exit.
func (d *Dataset) Aggregate(ctx context.Context, selections []Selection, reductions []Reduction, opt AggregateOptions, st *ReadStats) (out [][]AggregateResult, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	columns, slots, err := aggregateColumns(reductions, opt)
	if err != nil {
		return nil, err
	}
	op, err := d.r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done(ctx, &err, st)
	if err := op.Err(); err != nil {
		return nil, err
	}
	if err := d.r.checkProjection(columns); err != nil {
		return nil, err
	}
	if len(reductions) == 0 {
		for _, sel := range selections {
			if err := sel.Cursor.check(false); err != nil {
				return nil, err
			}
		}
		return aggregateResults(len(selections), 0), nil
	}
	if opt.CheckKeys && (opt.ValueBudgetBytes == -1 || len(columns) == 0) {
		return d.aggregateKeys(op, selections, reductions, opt, columns, slots, st, nil)
	}
	if opt.CheckKeys {
		located := make([]RowSpan, len(selections))
		keyOpt := AggregateOptions{ValueBudgetBytes: -1, Workers: opt.Workers, CheckKeys: true}
		if _, err := d.aggregateKeys(op, selections, []Reduction{{Op: CountRows}}, keyOpt, nil, []int{-1}, st, located); err != nil {
			return nil, err
		}
		opt.CheckKeys = false
		return d.aggregateRows(op, located, reductions, opt, columns, slots, st)
	}
	unique, of, err := distinctAggregateSelections(selections)
	if err != nil {
		return nil, err
	}
	var planned ReadStats
	p, err := d.Plan(op, unique, nil, &planned)
	st.Add(planned)
	if err != nil {
		return nil, err
	}
	results, err := d.aggregateRows(op, p.InputRowSpans(), reductions, opt, columns, slots, st)
	if err != nil {
		return nil, err
	}
	if len(unique) == len(selections) {
		return results, nil
	}
	out = aggregateResults(len(selections), len(reductions))
	for i, u := range of {
		for j, result := range results[u] {
			out[i][j] = result
			out[i][j].Value = result.Value.Clone()
		}
	}
	return out, nil
}

// AggregateRows reduces established row spans, preserving input order, overlaps
// and empties. Boundaries must belong to d as PlanRows requires. Without a
// filter or CheckKeys, CountRows performs zero storage reads. Other semantics,
// budgets, statistics and error handling are those of Aggregate.
func (d *Dataset) AggregateRows(ctx context.Context, spans []RowSpan, reductions []Reduction, opt AggregateOptions, st *ReadStats) (out [][]AggregateResult, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	columns, slots, err := aggregateColumns(reductions, opt)
	if err != nil {
		return nil, err
	}
	op, err := d.r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done(ctx, &err, st)
	if err := op.Err(); err != nil {
		return nil, err
	}
	if err := d.r.checkProjection(columns); err != nil {
		return nil, err
	}
	if len(reductions) == 0 {
		if _, err := d.r.rowRanges(spans); err != nil {
			return nil, err
		}
		return aggregateResults(len(spans), 0), nil
	}
	return d.aggregateRows(op, spans, reductions, opt, columns, slots, st)
}

func aggregateResults(n, m int) [][]AggregateResult {
	out := make([][]AggregateResult, n)
	for i := range out {
		out[i] = make([]AggregateResult, m)
	}
	return out
}

func (d *Dataset) aggregateRows(ctx context.Context, spans []RowSpan, reductions []Reduction, opt AggregateOptions, columns []string, slots []int, st *ReadStats) ([][]AggregateResult, error) {
	out := aggregateResults(len(spans), len(reductions))
	// Like swath aggregate: free counts use one no-column PlanRows per input.
	var candidates []int
	for i, sp := range spans {
		p, err := d.PlanRows(ctx, []RowSpan{sp}, nil, nil)
		if err != nil {
			return nil, err
		}
		n, exact := p.Count()
		if !exact && !opt.CheckKeys {
			continue
		}
		if exact {
			if _, err := d.reductionTypes(p, reductions); err != nil {
				return nil, err
			}
		}
		for j, r := range reductions {
			if r.Op == CountRows && opt.Filter == nil {
				out[i][j] = AggregateResult{Int64Value(n), AggregateExact}
			}
		}
		if n == 0 {
			for j, r := range reductions {
				v := Int64Value(0)
				if r.Op == Max {
					v = Value{}
				}
				out[i][j] = AggregateResult{v, AggregateExact}
			}
		} else {
			candidates = append(candidates, i)
		}
	}
	slices.SortStableFunc(candidates, func(a, b int) int {
		if spans[a].Start.ordinal < spans[b].Start.ordinal {
			return -1
		}
		if spans[a].Start.ordinal > spans[b].Start.ordinal {
			return 1
		}
		return 0
	})
	// No value work is necessary for an unfiltered CountRows-only request.
	needValues := len(columns) > 0 || opt.Filter != nil
	if !needValues && !opt.CheckKeys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}
	selected := candidates
	var plan *Plan
	var err error
	if needValues && len(columns) > 0 && opt.ValueBudgetBytes >= 0 {
		var status []AggregateStatus
		plan, selected, status, err = d.budgetValues(ctx, spans, candidates, columns, opt.ValueBudgetBytes, st)
		if err != nil {
			return nil, err
		}
		for _, i := range candidates {
			for j, r := range reductions {
				if r.Op != CountRows || opt.Filter != nil {
					out[i][j] = AggregateResult{Status: status[i]}
				}
			}
		}
	} else {
		rows := make([]RowSpan, len(candidates))
		for k, i := range candidates {
			rows[k] = spans[i]
		}
		var planned ReadStats
		plan, err = d.PlanRows(ctx, rows, columns, &planned)
		st.Add(planned)
		if err != nil {
			return nil, err
		}
	}
	if len(selected) > 0 {
		sums, err := d.reduceRows(ctx, plan, reductions, opt, slots, st)
		if err != nil {
			return nil, err
		}
		for k, i := range selected {
			for j := range reductions {
				out[i][j] = sums[k][j]
			}
		}
	}
	if opt.CheckKeys && len(selected) != len(candidates) {
		// Budget refusal must not remove the requested key-validation obligation.
		var planned ReadStats
		p, err := d.PlanRows(ctx, spans, nil, &planned)
		st.Add(planned)
		if err != nil {
			return nil, err
		}
		var scanned ReadStats
		err = p.ScanParallel(ctx, opt.Workers, &scanned, func(*Batch) error { return nil })
		st.Add(scanned)
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// budgetValues is swath budgetSums: discover candidates part by part, abandon
// unaffordable candidates before opening later parts, and price the trial plan
// before SelectRows can discard transient locations. Metadata is uncharged.
const minAggregatePageBytes = 16

func (d *Dataset) budgetValues(ctx context.Context, spans []RowSpan, candidates []int, columns []string, budget int64, st *ReadStats) (*Plan, []int, []AggregateStatus, error) {
	parts := d.r.parts
	ends := make([]int64, len(parts))
	for i, p := range parts {
		ends[i] = p.start + p.rows
	}
	charged := make([]bool, len(parts))
	status := make([]AggregateStatus, len(spans))
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
				partFloors[j-first] = aggregatePartFloor(parts[j], columns)
				floor += partFloors[j-first]
			}
		}
		trial := slices.Clone(rows)
		from := t.Start
		cost := used
		var universe *Plan
		fits := true
		status[i] = AggregateBudgetExceeded
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
				status[i] = AggregateUnpriced
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
			status[i] = AggregateExact
			for j := first; j <= last; j++ {
				charged[j] = true
			}
		}
	}
	if accepted == nil {
		return nil, nil, status, nil
	}
	plan, err := accepted.SelectRows(rows)
	return plan, selected, status, err
}

// reductionType checks original footer annotations, even for all-null columns.
// COL-5 checks physical layout, not logical signedness or timestamp units.
type reductionType struct {
	kind     Kind
	unsigned bool
	logical  Logical
}

func (d *Dataset) reductionTypes(p *Plan, reductions []Reduction) ([]reductionType, error) {
	types := make([]reductionType, len(reductions))
	for _, sp := range p.spans {
		for i := d.r.partAt(sp.start); i < len(d.r.parts) && d.r.parts[i].start < sp.end; i++ {
			part := d.r.parts[i]
			for j, r := range reductions {
				if r.Op != Sum && r.Op != Max {
					continue
				}
				col, unknown, ok := aggregateColumn(part, r.Column)
				if !ok {
					continue
				}
				if unknown {
					return nil, invalidRequest("unrecognized logical type for reduction column %q", r.Column)
				}
				k, logical := col.Type, col.Logical
				integer := (k == KindInt32 || k == KindInt64) && (logical.Kind == LogicalNone || logical.Kind == LogicalInt)
				timestamp := k == KindInt64 && logical.Kind == LogicalTimestamp
				str := (k == KindByteArray || k == KindFixedLenByteArray) && (logical.Kind == LogicalNone || logical.Kind == LogicalString)
				if !(integer || r.Op == Max && (timestamp || str)) {
					return nil, invalidRequest("%v does not support column %q (%s %s)", r.Op, r.Column, k, logical)
				}
				current := reductionType{kind: k, unsigned: logical.Kind == LogicalInt && !logical.Signed, logical: logical}
				previous := types[j]
				timestampBefore := previous.logical.Kind == LogicalTimestamp
				if previous.kind != KindNone && (previous.kind != current.kind || previous.unsigned != current.unsigned || timestampBefore != timestamp || timestamp && (previous.logical.Unit != logical.Unit || previous.logical.AdjustedToUTC != logical.AdjustedToUTC)) {
					return nil, invalidRequest("incompatible logical types across parts for reduction column %q", r.Column)
				}
				types[j] = current
			}
		}
	}
	return types, nil
}

// reductionSum generalizes swath objectSum. A signed 128-bit accumulator holds
// any sum of at most MaxInt64 signed/unsigned 64-bit values, including temporary
// overflow followed by cancellation, without allocation in the row loop.
type reductionSum struct {
	lo, hi   uint64
	maximum  Value
	maxBytes []byte
	seen     bool
}

func (s *reductionSum) addInt(v uint64, negative bool) {
	hi := uint64(0)
	if negative {
		hi = math.MaxUint64
	}
	var carry uint64
	s.lo, carry = bits.Add64(s.lo, v, 0)
	s.hi, _ = bits.Add64(s.hi, hi, carry)
}
func (s *reductionSum) merge(t *reductionSum, r Reduction, typ reductionType) {
	var carry uint64
	s.lo, carry = bits.Add64(s.lo, t.lo, 0)
	s.hi, _ = bits.Add64(s.hi, t.hi, carry)
	if t.seen && (!s.seen || greater(t.maximum, s.maximum, typ)) {
		s.keepMax(t.maximum)
		s.seen = true
	}
}
func (s *reductionSum) keepMax(v Value) {
	if v.kind == KindByteArray || v.kind == KindFixedLenByteArray {
		s.maxBytes = append(s.maxBytes[:0], v.Bytes()...)
		v.ptr = unsafe.SliceData(s.maxBytes)
	}
	s.maximum = v
}
func greater(a, b Value, t reductionType) bool {
	switch t.kind {
	case KindInt32:
		if t.unsigned {
			return a.Uint32() > b.Uint32()
		}
		return a.Int32() > b.Int32()
	case KindInt64:
		if t.unsigned {
			return a.Uint64() > b.Uint64()
		}
		return a.Int64() > b.Int64()
	default:
		return bytes.Compare(a.Bytes(), b.Bytes()) > 0
	}
}
func (s *reductionSum) add(v Value, r Reduction, t reductionType) {
	switch r.Op {
	case CountRows:
		s.addInt(1, false)
	case CountNulls:
		if v.IsNull() {
			s.addInt(1, false)
		}
	case CountAbsent:
		if v.Absent() {
			s.addInt(1, false)
		}
	case Sum:
		if v.IsNull() || v.Absent() {
			return
		}
		if t.unsigned {
			n := v.Uint64()
			if t.kind == KindInt32 {
				n = uint64(v.Uint32())
			}
			s.addInt(n, false)
		} else {
			n := v.Int64()
			if t.kind == KindInt32 {
				n = int64(v.Int32())
			}
			s.addInt(uint64(n), n < 0)
		}
	case Max:
		if !v.IsNull() && !v.Absent() && (!s.seen || greater(v, s.maximum, t)) {
			s.keepMax(v)
			s.seen = true
		}
	}
}
func (s *reductionSum) result(r Reduction, t reductionType) AggregateResult {
	if r.Op == Max {
		return AggregateResult{s.maximum.Clone(), AggregateExact}
	}
	if r.Op == Sum {
		if t.unsigned {
			if s.hi != 0 {
				return AggregateResult{Status: AggregateOverflow}
			}
			return AggregateResult{Uint64Value(s.lo), AggregateExact}
		}
		if !(s.hi == 0 && s.lo <= math.MaxInt64 || s.hi == math.MaxUint64 && s.lo > math.MaxInt64) {
			return AggregateResult{Status: AggregateOverflow}
		}
	}
	return AggregateResult{Int64Value(int64(s.lo)), AggregateExact}
}

// reduceRows ports swath sumRows: sum each ordinal segment once, then merge it
// into every input holding that segment. Each worker owns segments and a mask.
func (d *Dataset) reduceRows(ctx context.Context, p *Plan, reductions []Reduction, opt AggregateOptions, slots []int, st *ReadStats) ([][]AggregateResult, error) {
	types, err := d.reductionTypes(p, reductions)
	if err != nil {
		return nil, err
	}
	var cuts []int64
	for _, sp := range p.inputs {
		cuts = append(cuts, sp.start, sp.end)
	}
	slices.Sort(cuts)
	cuts = slices.Compact(cuts)
	m := len(reductions)
	segments := make([]reductionSum, len(cuts)*m)
	var mu sync.Mutex
	err = p.scanParallel(ctx, opt.Workers, st, opt.CheckKeys, func() shardScan {
		sums := make([]reductionSum, len(segments))
		var keep []bool
		var projected ValueBatch
		filterSlots := aggregateFilterSlots(p.columns, opt.Filter)
		if opt.Filter != nil {
			projected.values = make([][]parquet.Value, len(opt.Filter.Columns))
			projected.kinds = make([]Kind, len(opt.Filter.Columns))
		}
		return func(ctx context.Context, shard *Plan, st *ReadStats, keys *shardKeys) error {
			clear(sums)
			k := 0
			apply := func(b *ValueBatch) error {
				var err error
				keep, err = applyAggregateFilter(b, opt.Filter, filterSlots, keep, &projected)
				if err != nil {
					return err
				}
				for row := range b.Len() {
					if opt.Filter != nil && !keep[row] {
						continue
					}
					ordinal := b.start + int64(row)
					for k+1 < len(cuts) && cuts[k+1] <= ordinal {
						k++
					}
					for j, r := range reductions {
						v := Value{}
						if slots[j] >= 0 {
							v = b.Column(slots[j]).Value(row)
						}
						sums[k*m+j].add(v, r, types[j])
					}
				}
				return nil
			}
			var failed, scannedErr error
			if opt.CheckKeys {
				scannedErr = shard.Scan(ctx, st, func(b *Batch) error {
					keys.noteBatch(b)
					v := ValueBatch{ds: d.r.id, start: b.start, n: b.Len(), values: b.values, kinds: b.kinds}
					failed = apply(&v)
					return failed
				})
			} else {
				scannedErr = shard.ScanValues(ctx, st, func(b *ValueBatch) error { failed = apply(b); return failed })
			}
			if failed != nil {
				return &shardCallbackError{failed}
			}
			if scannedErr != nil {
				return scannedErr
			}
			mu.Lock()
			defer mu.Unlock()
			for k := range cuts {
				for j, r := range reductions {
					segments[k*m+j].merge(&sums[k*m+j], r, types[j])
				}
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	out := aggregateResults(len(p.inputs), m)
	for i, sp := range p.inputs {
		for j, r := range reductions {
			var total reductionSum
			for k := sort.Search(len(cuts), func(k int) bool { return cuts[k] >= sp.start }); k < len(cuts) && cuts[k] < sp.end; k++ {
				total.merge(&segments[k*m+j], r, types[j])
			}
			out[i][j] = total.result(r, types[j])
		}
	}
	return out, nil
}

func applyAggregateFilter(b *ValueBatch, f *BatchFilter, slots []int, keep []bool, projected *ValueBatch) ([]bool, error) {
	if f == nil {
		return keep, nil
	}
	keep = resize(keep, b.Len())
	for i := range keep {
		keep[i] = true
	}
	projected.ds, projected.start, projected.n = b.ds, b.start, b.n
	for i, c := range slots {
		projected.values[i], projected.kinds[i] = b.values[c], b.kinds[c]
	}
	err := f.Apply(projected, keep)
	clear(projected.values)
	clear(projected.kinds)
	return keep, err
}

func aggregateFilterSlots(columns []string, f *BatchFilter) []int {
	if f == nil {
		return nil
	}
	slots := make([]int, len(f.Columns))
	for i, name := range f.Columns {
		slots[i] = slices.Index(columns, name)
	}
	return slots
}

// Unlike swath's mandatory size column, generic projected columns may all be
// absent in a checked part, whose value work has a zero lower bound.
func aggregatePartFloor(p *part, columns []string) int64 {
	if p.ready.Load() {
		for _, name := range columns {
			if _, ok := p.leaves[name]; ok {
				return minAggregatePageBytes
			}
		}
		return 0
	}
	return minAggregatePageBytes
}

// aggregateColumn reads the original top-level annotation. parquet-go's type
// can be the normalized physical fallback, which must not silently turn a
// logical measure (or an unknown annotation) into an unannotated integer.
func aggregateColumn(p *part, name string) (col Column, unknown, found bool) {
	schema := p.footer.Metadata().Schema
	for pos := 1; pos < len(schema); {
		se := &schema[pos]
		if se.Name == name {
			col = columnOf(se, "")
			unknown = se.LogicalType.Value != nil && col.Logical.Kind == LogicalNone
			if _, ok := se.ConvertedType.Get(); ok && col.Logical.Kind == LogicalNone {
				unknown = true
			}
			return col, unknown, true
		}
		for remaining := int64(1); remaining > 0; {
			remaining--
			remaining += int64(schema[pos].NumChildren.V)
			pos++
		}
	}
	return Column{}, false, false
}

// distinctAggregateSelections is swath distinct, generalized from prefixes to
// effective span bounds and row cursors. Resolve each repeated input only once.
func distinctAggregateSelections(in []Selection) (unique []Selection, of []int, err error) {
	type identity struct {
		start, end, cursorKey      string
		bounded, exclusive, cursor bool
		offset                     int64
	}
	seen := make(map[identity]int, len(in))
	of = make([]int, len(in))
	for i, sel := range in {
		if err := sel.Cursor.check(false); err != nil {
			return nil, nil, err
		}
		lo, exclusive, hi := sel.bounds()
		id := identity{start: string(lo), end: string(hi), bounded: hi != nil, exclusive: exclusive}
		if c := sel.Cursor; c != nil {
			id.cursor = true
			id.cursorKey = string(c.Key)
			id.offset = c.Offset
		}
		u, ok := seen[id]
		if !ok {
			u = len(unique)
			seen[id] = u
			unique = append(unique, sel)
		}
		of[i] = u
	}
	return unique, of, nil
}
