// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil/fakestore"
)

// The I/O budgets: every read operation of the remote-read plan (§6 of
// swath-notes 2026-11-11-seekparquet-remote-read-restore-plan), run on the
// budget fixture through a fake store that serves it as objfile serves a
// remote store today (one metadata request per open, one ranged request
// per ReadAt, 10 ms a request plus 128 MiB/s, at most 64 in flight), on
// the virtual clock of testing/synctest. Each operation's requests,
// rounds (the longest chain of dependent requests), bytes, repeated
// extents and most requests in flight are asserted against ioBudgets.
// SEEKPARQUET_IO_TRACE=text logs the full trace of every operation whose
// name (prefixed unique/ or repeated/) contains text.
//
// Every row asserts today's numbers exactly, so that any change, better or
// worse, is a diff a reviewer approves: update the row from the table the
// failing test prints. A row whose check is atTarget also asserts the
// plan's target, as an upper bound on requests and rounds and no repeated
// extent: when a change meets a target, flip the row.
//
// The stores: plain is a scalar remote store with metadata discovery
// (objfile today), read by this reader with every acceleration it has
// (retained tails, request memo); capable adds sized opens, whole-manifest
// reads and range batches. The fake merges nothing: one physical request
// per submitted range. OpenDir is the result oracle of the differential,
// not a scheduling baseline; results are also checked against the
// fixture's generated rows (TestReadBudgetsOracle), independently of the
// reader. Every operation's results (rows, entries,
// cursors, counts, errors) must also equal those of a plain local
// directory open: the differential check later optimisations keep.

// ioOptions is the store the budgets are measured on.
var ioOptions = fakestore.Options{Latency: 10 * time.Millisecond, BytesPerSecond: 128 << 20, MetadataOnOpen: true, MaxInFlight: 64}

// ioMetrics is what a budget row asserts.
type ioMetrics struct {
	Requests, Rounds int
	Bytes            int64
	Repeats          int
	MaxInFlight      int
}

// ioTarget is the plan's target for an operation: requests and rounds at
// most, and no repeated extent; -1 where the plan sets none.
type ioTarget struct {
	Requests, Rounds int
}

type budgetCheck uint8

const (
	current  budgetCheck = iota // assert now exactly
	atTarget                    // assert target as a bound
)

type ioBudget struct {
	op     string
	now    ioMetrics
	target ioTarget
	check  budgetCheck
}

// none marks an operation the plan sets no target for.
var none = ioTarget{-1, -1}

// ioBudgets is measured on ioSmall with ioOptions; an operation's name is
// its scenario and step ("cold" is the first read on a fresh handle after
// an unrecorded open, "warm" the same read again on it). Targets are the
// plan's §6 and the storage interface decision's acceptance budgets
// (storage-iface-astra.md, "Build order and acceptance budgets"): where
// the plan gives one figure for a cold mid-part lookup, the first and last
// keys carry it too; Get carries the Rows lookup's target plus one round,
// as that decision allows Get a separate value round; scans keep their
// cold target warm; Verify carries CheckSchema's admission target.
// Metadata-first Plan proves whole-part ends from manifest bounds before
// decoding keys. Thus plan-part/plan-cold and split/plan now read only the
// part check (plain: 3 requests/2 rounds; capable: 2/2, both 2,097,156 B),
// and plan-part/plan-warm makes no requests. These accepted improvements
// are recorded as exact current values in both tables below.
var ioBudgets = []ioBudget{
	{"open/cold", ioMetrics{2, 2, 1591, 0, 1}, ioTarget{1, 1}, current},
	{"open/again", ioMetrics{2, 2, 1591, 0, 1}, ioTarget{1, 1}, current},
	{"rows-key/first/cold", ioMetrics{6, 5, 2111161, 0, 2}, ioTarget{8, 4}, current},
	{"rows-key/first/warm", ioMetrics{3, 3, 14005, 0, 1}, ioTarget{3, 1}, current},
	{"rows-key/mid/cold", ioMetrics{8, 7, 2121071, 0, 2}, ioTarget{8, 4}, current},
	{"rows-key/mid/warm", ioMetrics{3, 3, 16685, 0, 1}, ioTarget{3, 1}, current},
	{"rows-key/other-group/warm", ioMetrics{3, 3, 14724, 0, 1}, ioTarget{3, 1}, current},
	{"rows-key/last/cold", ioMetrics{8, 7, 2103930, 0, 2}, ioTarget{8, 4}, current},
	{"rows-key/last/warm", ioMetrics{3, 3, 5467, 0, 1}, ioTarget{3, 1}, current},
	{"get/first/cold", ioMetrics{6, 5, 2111161, 0, 2}, ioTarget{8, 5}, atTarget},
	{"get/first/warm", ioMetrics{3, 3, 14005, 0, 1}, ioTarget{3, 2}, current},
	{"get/mid/cold", ioMetrics{8, 7, 2121071, 0, 2}, ioTarget{8, 5}, current},
	{"get/mid/warm", ioMetrics{3, 3, 16685, 0, 1}, ioTarget{3, 2}, current},
	{"get/other-group/warm", ioMetrics{3, 3, 14724, 0, 1}, ioTarget{3, 2}, current},
	{"get/last/cold", ioMetrics{8, 7, 2103930, 0, 2}, ioTarget{8, 5}, current},
	{"get/last/warm", ioMetrics{3, 3, 5467, 0, 1}, ioTarget{3, 2}, current},
	{"rows-100/2-cols/cold", ioMetrics{8, 7, 2121071, 0, 2}, ioTarget{8, 4}, current},
	{"rows-100/2-cols/warm", ioMetrics{3, 3, 16685, 0, 1}, ioTarget{3, 1}, current},
	{"rows-100/keys/cold", ioMetrics{6, 5, 2111735, 0, 2}, none, current},
	{"rows-100/keys/warm", ioMetrics{1, 1, 7349, 0, 1}, none, current},
	{"list-ra0/page1-cold", ioMetrics{16, 10, 4276461, 0, 3}, ioTarget{25, 8}, current},
	{"list-ra0/page2", ioMetrics{3, 1, 24484, 0, 3}, ioTarget{4, 2}, atTarget},
	{"list-ra0/page1-warm", ioMetrics{6, 2, 73570, 0, 3}, ioTarget{4, 2}, current},
	{"list-ra8/page1-cold", ioMetrics{22, 18, 4255737, 0, 2}, ioTarget{25, 8}, current},
	{"list-ra8/page2", ioMetrics{6, 5, 24484, 0, 2}, ioTarget{4, 2}, current},
	{"list-ra8/page1-warm", ioMetrics{12, 10, 52846, 0, 2}, ioTarget{4, 2}, current},
	{"build-list-index", ioMetrics{28, 4, 10389655, 0, 16}, ioTarget{40, 10}, atTarget},
	{"list-indexed-ra0/page1-cold", ioMetrics{7, 2, 364516, 0, 4}, ioTarget{25, 8}, atTarget},
	{"list-indexed-ra0/page2", ioMetrics{4, 1, 315430, 0, 4}, ioTarget{4, 2}, atTarget},
	{"list-indexed-ra0/page1-warm", ioMetrics{6, 2, 73570, 0, 3}, ioTarget{4, 2}, current},
	{"scan-part/cold", ioMetrics{7, 3, 2763754, 0, 4}, ioTarget{20, 5}, atTarget},
	{"scan-part/warm", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"plan-part/plan-cold", ioMetrics{3, 2, 2097156, 0, 2}, none, current},
	{"plan-part/scan-cold", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"plan-part/plan-warm", ioMetrics{0, 0, 0, 0, 0}, none, current},
	{"plan-part/scan-warm", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"plan-20/plan-cold", ioMetrics{44, 40, 8550725, 0, 2}, ioTarget{32, 4}, current},
	{"plan-20/scan-cold", ioMetrics{60, 1, 289574, 0, 60}, ioTarget{60, 2}, atTarget},
	{"plan-20/plan-warm", ioMetrics{20, 20, 103002, 0, 1}, none, current},
	{"plan-20/scan-warm", ioMetrics{60, 1, 289574, 0, 60}, ioTarget{60, 2}, atTarget},
	{"split/plan", ioMetrics{3, 2, 2097156, 0, 2}, none, current},
	{"split/split4", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
	{"split/shards-concurrent", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"verify/cold", ioMetrics{28, 3, 8477103, 0, 20}, ioTarget{24, 3}, current},
	{"verify/warm", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
	{"browse/count-prefix", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
	{"browse/sum-prefix", ioMetrics{1, 1, 6153, 0, 1}, ioTarget{1, 1}, atTarget},
	{"sum-all/scan", ioMetrics{16, 1, 1210103, 0, 16}, ioTarget{16, 1}, atTarget},
}

// ioCapableBudgets is measured on ioSmall through capableStore.
var ioCapableBudgets = []ioBudget{
	{"open/cold", ioMetrics{1, 1, 1591, 0, 1}, ioTarget{1, 1}, atTarget},
	{"open/again", ioMetrics{1, 1, 1591, 0, 1}, ioTarget{1, 1}, atTarget},
	{"rows-key/first/cold", ioMetrics{5, 4, 2111161, 0, 2}, ioTarget{8, 4}, atTarget},
	{"rows-key/first/warm", ioMetrics{3, 2, 14005, 0, 2}, ioTarget{3, 1}, current},
	{"rows-key/mid/cold", ioMetrics{8, 5, 2121731, 0, 3}, ioTarget{8, 4}, current},
	{"rows-key/mid/warm", ioMetrics{3, 2, 16685, 0, 2}, ioTarget{3, 1}, current},
	{"rows-key/other-group/warm", ioMetrics{3, 2, 14724, 0, 2}, ioTarget{3, 1}, current},
	{"rows-key/last/cold", ioMetrics{8, 5, 2110513, 0, 3}, ioTarget{8, 4}, current},
	{"rows-key/last/warm", ioMetrics{3, 2, 5467, 0, 2}, ioTarget{3, 1}, current},
	{"get/first/cold", ioMetrics{5, 4, 2111161, 0, 2}, ioTarget{8, 5}, atTarget},
	{"get/first/warm", ioMetrics{3, 2, 14005, 0, 2}, ioTarget{3, 2}, atTarget},
	{"get/mid/cold", ioMetrics{8, 5, 2121731, 0, 3}, ioTarget{8, 5}, atTarget},
	{"get/mid/warm", ioMetrics{3, 2, 16685, 0, 2}, ioTarget{3, 2}, atTarget},
	{"get/other-group/warm", ioMetrics{3, 2, 14724, 0, 2}, ioTarget{3, 2}, atTarget},
	{"get/last/cold", ioMetrics{8, 5, 2110513, 0, 3}, ioTarget{8, 5}, atTarget},
	{"get/last/warm", ioMetrics{3, 2, 5467, 0, 2}, ioTarget{3, 2}, atTarget},
	{"rows-100/2-cols/cold", ioMetrics{8, 5, 2121731, 0, 3}, ioTarget{8, 4}, current},
	{"rows-100/2-cols/warm", ioMetrics{3, 2, 16685, 0, 2}, ioTarget{3, 1}, current},
	{"rows-100/keys/cold", ioMetrics{6, 4, 2112395, 0, 3}, none, current},
	{"rows-100/keys/warm", ioMetrics{1, 1, 7349, 0, 1}, none, current},
	{"list-ra0/page1-cold", ioMetrics{16, 8, 4281282, 0, 3}, ioTarget{25, 8}, current},
	{"list-ra0/page2", ioMetrics{3, 1, 24484, 0, 3}, ioTarget{4, 2}, atTarget},
	{"list-ra0/page1-warm", ioMetrics{6, 2, 73570, 0, 3}, ioTarget{4, 2}, current},
	{"list-ra8/page1-cold", ioMetrics{22, 16, 4261907, 0, 3}, ioTarget{25, 8}, current},
	{"list-ra8/page2", ioMetrics{6, 5, 24484, 0, 2}, ioTarget{4, 2}, current},
	{"list-ra8/page1-warm", ioMetrics{12, 10, 52846, 0, 2}, ioTarget{4, 2}, current},
	{"build-list-index", ioMetrics{24, 3, 10389655, 0, 16}, ioTarget{40, 10}, atTarget},
	{"list-indexed-ra0/page1-cold", ioMetrics{7, 2, 364516, 0, 4}, ioTarget{25, 8}, atTarget},
	{"list-indexed-ra0/page2", ioMetrics{4, 1, 315430, 0, 4}, ioTarget{4, 2}, atTarget},
	{"list-indexed-ra0/page1-warm", ioMetrics{6, 2, 73570, 0, 3}, ioTarget{4, 2}, current},
	{"scan-part/cold", ioMetrics{6, 3, 2763754, 0, 4}, ioTarget{20, 5}, atTarget},
	{"scan-part/warm", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"plan-part/plan-cold", ioMetrics{2, 2, 2097156, 0, 1}, none, current},
	{"plan-part/scan-cold", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"plan-part/plan-warm", ioMetrics{0, 0, 0, 0, 0}, none, current},
	{"plan-part/scan-warm", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"plan-20/plan-cold", ioMetrics{40, 32, 8550725, 0, 3}, ioTarget{32, 4}, current},
	{"plan-20/scan-cold", ioMetrics{60, 1, 289574, 0, 60}, ioTarget{60, 2}, atTarget},
	{"plan-20/plan-warm", ioMetrics{20, 20, 103002, 0, 1}, none, current},
	{"plan-20/scan-warm", ioMetrics{60, 1, 289574, 0, 60}, ioTarget{60, 2}, atTarget},
	{"split/plan", ioMetrics{2, 2, 2097156, 0, 1}, none, current},
	{"split/split4", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
	{"split/shards-concurrent", ioMetrics{4, 1, 666598, 0, 4}, ioTarget{20, 5}, atTarget},
	{"verify/cold", ioMetrics{24, 2, 8477103, 0, 20}, ioTarget{24, 3}, atTarget},
	{"verify/warm", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
	{"browse/count-prefix", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
	{"browse/sum-prefix", ioMetrics{1, 1, 6153, 0, 1}, ioTarget{1, 1}, atTarget},
	{"sum-all/scan", ioMetrics{16, 1, 1210103, 0, 16}, ioTarget{16, 1}, atTarget},
}

// ioRepeatedBudgets is measured on the repeated-key variant of ioSmall,
// through capableStore.
var ioRepeatedBudgets = []ioBudget{
	{"open/cold", ioMetrics{1, 1, 1592, 0, 1}, ioTarget{1, 1}, atTarget},
	{"open/again", ioMetrics{1, 1, 1592, 0, 1}, ioTarget{1, 1}, atTarget},
	{"rows-key/mid/cold", ioMetrics{8, 5, 2119029, 0, 3}, ioTarget{8, 4}, current},
	{"rows-key/mid/warm", ioMetrics{3, 2, 13555, 0, 2}, ioTarget{3, 1}, current},
	{"rows-key/other-group/warm", ioMetrics{3, 2, 13015, 0, 2}, ioTarget{3, 1}, current},
	{"get/first/cold", ioMetrics{5, 4, 2110022, 0, 2}, ioTarget{8, 5}, atTarget},
	{"get/first/warm", ioMetrics{3, 2, 12866, 0, 2}, ioTarget{3, 2}, atTarget},
	{"get/mid/cold", ioMetrics{8, 5, 2119029, 0, 3}, ioTarget{8, 5}, atTarget},
	{"get/mid/warm", ioMetrics{3, 2, 13555, 0, 2}, ioTarget{3, 2}, atTarget},
	{"get/other-group/warm", ioMetrics{3, 2, 13015, 0, 2}, ioTarget{3, 2}, atTarget},
	{"get/last/cold", ioMetrics{8, 5, 2112377, 0, 3}, ioTarget{8, 5}, atTarget},
	{"get/last/warm", ioMetrics{3, 2, 6903, 0, 2}, ioTarget{3, 2}, atTarget},
	{"rows-100/2-cols/cold", ioMetrics{8, 5, 2119029, 0, 3}, ioTarget{8, 4}, current},
	{"rows-100/2-cols/warm", ioMetrics{3, 2, 13555, 0, 2}, ioTarget{3, 1}, current},
	{"list-ra0/page1-cold", ioMetrics{8, 4, 2176006, 0, 3}, ioTarget{25, 8}, atTarget},
	{"list-ra0/page2", ioMetrics{9, 5, 2198343, 0, 3}, ioTarget{4, 2}, current},
	{"list-ra0/page1-warm", ioMetrics{3, 1, 73448, 0, 3}, ioTarget{4, 2}, atTarget},
	{"scan-part/cold", ioMetrics{6, 3, 2697866, 0, 4}, ioTarget{20, 5}, atTarget},
	{"scan-part/warm", ioMetrics{4, 1, 600710, 0, 4}, ioTarget{20, 5}, atTarget},
	{"verify/cold", ioMetrics{24, 2, 8456109, 0, 20}, ioTarget{24, 3}, atTarget},
	{"verify/warm", ioMetrics{0, 0, 0, 0, 0}, ioTarget{0, 0}, atTarget},
}

// ioEnv runs one scenario: through the fake store in a synctest bubble,
// recording each step, or through OpenDir, for the differential check.
type ioEnv struct {
	t       *testing.T
	fx      *ioFixture
	src     *fakestore.Source // nil for the local directory
	store   ioStore
	reports map[string]fakestore.Report
	results map[string]string
}

// open opens the fixture: through the store, unrecorded, or as a local
// directory. The dataset is closed when the scenario ends.
func (e *ioEnv) open() *seekparquet.Dataset {
	e.t.Helper()
	var d *seekparquet.Dataset
	var err error
	if e.src != nil {
		d, err = seekparquet.OpenDataset(context.Background(), e.store.wrap(e.src), e.store.opt)
	} else {
		d, err = seekparquet.OpenDir(context.Background(), e.fx.dir, seekparquet.OpenOptions{})
	}
	must(e.t, err)
	e.t.Cleanup(func() { _ = d.Close() })
	return d
}

// step runs fn as the operation named name, recording its requests and
// its result.
func (e *ioEnv) step(name string, fn func() string) {
	e.t.Helper()
	var op *fakestore.Operation
	if e.src != nil {
		op = e.src.Begin(name)
	}
	res := fn()
	if op != nil {
		e.reports[name] = op.End()
		if n := e.src.InFlight(); n != 0 {
			e.t.Errorf("%s: %d requests in flight after it returned", name, n)
		}
	}
	if _, dup := e.results[name]; dup {
		e.t.Fatalf("step %s twice", name)
	}
	e.results[name] = res
}

// ioScenario is a sequence of steps on a fresh store.
type ioScenario struct {
	name string
	run  func(e *ioEnv)
}

func succ(k []byte) []byte { return append(bytes.Clone(k), 0) }

func ioScenarios(fx *ioFixture) []ioScenario {
	ctx := context.Background()
	partSpan := seekparquet.Span{Start: fx.first, End: succ(fx.last)}
	lookups := []struct {
		name string
		key  []byte
	}{{"first", fx.first}, {"mid", fx.mid}, {"last", fx.last}}
	var out []ioScenario
	out = append(out, ioScenario{"open", func(e *ioEnv) {
		if e.src == nil {
			e.step("open/cold", func() string { return fmt.Sprint(len(e.open().Metadata().Parts)) })
			e.step("open/again", func() string { return fmt.Sprint(len(e.open().Metadata().Parts)) })
			return
		}
		// The open is the step: open through the store inside it.
		for _, name := range []string{"open/cold", "open/again"} {
			op := e.src.Begin(name)
			d := e.open()
			e.reports[name] = op.End()
			e.results[name] = fmt.Sprint(len(d.Metadata().Parts))
		}
	}})
	for _, l := range lookups {
		out = append(out, ioScenario{"rows-key/" + l.name, func(e *ioEnv) {
			d := e.open()
			q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan(l.key)}, Limit: 1, Columns: ioColumns}
			for _, temp := range []string{"cold", "warm"} {
				e.step("rows-key/"+l.name+"/"+temp, func() string { return withErr(rowsText(d.Rows(ctx, q, nil))) })
			}
			if l.name == "mid" {
				q.Span = seekparquet.KeySpan(fx.other)
				e.step("rows-key/other-group/warm", func() string { return withErr(rowsText(d.Rows(ctx, q, nil))) })
			}
		}})
		out = append(out, ioScenario{"get/" + l.name, func(e *ioEnv) {
			d := e.open()
			for _, temp := range []string{"cold", "warm"} {
				e.step("get/"+l.name+"/"+temp, func() string { return withErr(rowsText(d.Get(ctx, l.key, nil, ioColumns...))) })
			}
			if l.name == "mid" {
				e.step("get/other-group/warm", func() string { return withErr(rowsText(d.Get(ctx, fx.other, nil, ioColumns...))) })
			}
		}})
	}
	for _, cols := range []struct {
		name string
		cols []string
	}{{"2-cols", ioColumns}, {"keys", nil}} {
		out = append(out, ioScenario{"rows-100/" + cols.name, func(e *ioEnv) {
			d := e.open()
			q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Start: fx.mid}}, Limit: 100, Columns: cols.cols}
			for _, temp := range []string{"cold", "warm"} {
				e.step("rows-100/"+cols.name+"/"+temp, func() string { return withErr(rowsText(d.Rows(ctx, q, nil))) })
			}
		}})
	}
	list := func(e *ioEnv, d *seekparquet.Dataset, tag string, ra int) {
		q := seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: 1000, ReadAhead: ra}
		var cur *seekparquet.Cursor
		e.step(tag+"/page1-cold", func() string {
			ents, err := d.List(ctx, q, nil)
			if len(ents) > 0 {
				cur = ents[len(ents)-1].Cursor()
			}
			return withErr(listText(ents, err))
		})
		q2 := q
		q2.Cursor = cur
		e.step(tag+"/page2", func() string { return withErr(listText(d.List(ctx, q2, nil))) })
		e.step(tag+"/page1-warm", func() string { return withErr(listText(d.List(ctx, q, nil))) })
	}
	for _, ra := range []int{0, 8} {
		tag := fmt.Sprintf("list-ra%d", ra)
		out = append(out, ioScenario{tag, func(e *ioEnv) { list(e, e.open(), tag, ra) }})
	}
	out = append(out, ioScenario{"list-index", func(e *ioEnv) {
		d := e.open()
		e.step("build-list-index", func() string {
			st, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{})
			st.Duration = 0
			return fmt.Sprintf("%+v %s", st, errResult(err))
		})
		list(e, d, "list-indexed-ra0", 0)
	}})
	out = append(out, ioScenario{"scan-part", func(e *ioEnv) {
		d := e.open()
		q := seekparquet.Query{Selection: seekparquet.Selection{Span: partSpan}, Columns: []string{"size"}}
		for _, temp := range []string{"cold", "warm"} {
			e.step("scan-part/"+temp, func() string {
				h := newBatchHash()
				return withErr(h.text(d.Scan(ctx, q, nil, h.add)))
			})
		}
	}})
	out = append(out, ioScenario{"plan-part", func(e *ioEnv) {
		d := e.open()
		for _, temp := range []string{"cold", "warm"} {
			var plan *seekparquet.Plan
			e.step("plan-part/plan-"+temp, func() string {
				var err error
				plan, err = d.Plan(ctx, []seekparquet.Selection{{Span: partSpan}}, []string{"size"}, nil)
				return withErr(planText(plan, err))
			})
			e.step("plan-part/scan-"+temp, func() string {
				h := newBatchHash()
				return withErr(h.text(plan.Scan(ctx, nil, h.add)))
			})
		}
	}})
	out = append(out, ioScenario{"plan-20", func(e *ioEnv) {
		d := e.open()
		var sels []seekparquet.Selection
		for _, k := range fx.twenty {
			sels = append(sels, seekparquet.Selection{Span: seekparquet.KeySpan(k)})
		}
		for _, temp := range []string{"cold", "warm"} {
			var plan *seekparquet.Plan
			e.step("plan-20/plan-"+temp, func() string {
				var err error
				plan, err = d.Plan(ctx, sels, ioColumns, nil)
				return withErr(planText(plan, err))
			})
			e.step("plan-20/scan-"+temp, func() string {
				h := newBatchHash()
				return withErr(h.text(plan.Scan(ctx, nil, h.add)))
			})
		}
	}})
	out = append(out, ioScenario{"split", func(e *ioEnv) {
		d := e.open()
		var plan *seekparquet.Plan
		e.step("split/plan", func() string {
			var err error
			plan, err = d.Plan(ctx, []seekparquet.Selection{{Span: partSpan}}, []string{"size"}, nil)
			return withErr(planText(plan, err))
		})
		var shards []*seekparquet.Plan
		e.step("split/split4", func() string {
			var err error
			shards, err = plan.Split(ctx, 4, nil)
			var counts []int64
			for _, s := range shards {
				n, _ := s.Count()
				counts = append(counts, n)
			}
			return fmt.Sprintf("%v %s", counts, errResult(err))
		})
		e.step("split/shards-concurrent", func() string { return withErr(shardsText(ctx, shards)) })
	}})
	out = append(out, ioScenario{"browse", func(e *ioEnv) {
		d := e.open()
		// The listing is the browser's page, unrecorded: its counts and sums
		// are the steps.
		ents, err := d.List(ctx, seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: 101}, nil)
		must(e.t, err)
		var prefix seekparquet.ListEntry
		for _, ent := range ents {
			if ent.IsPrefix() {
				prefix = ent
			}
		}
		e.step("browse/count-prefix", func() string {
			p, err := d.PlanRows(ctx, []seekparquet.RowSpan{prefix.Rows}, nil, nil)
			return withErr(planText(p, err))
		})
		e.step("browse/sum-prefix", func() string {
			p, err := d.PlanRows(ctx, []seekparquet.RowSpan{prefix.Rows}, []string{"size"}, nil)
			if err != nil {
				return errResult(err)
			}
			var sum, rows int64
			err = p.ScanValues(ctx, nil, func(b *seekparquet.ValueBatch) error {
				col := b.Column(0)
				for i := range b.Len() {
					sum += col.Int64(i)
				}
				rows += int64(b.Len())
				return nil
			})
			text, _ := planText(p, nil)
			return withErr(fmt.Sprintf("%q %s: %d rows, size %d", prefix.CommonPrefix, text, rows, sum), err)
		})
	}})
	out = append(out, ioScenario{"sum-all", func(e *ioEnv) {
		d := e.open()
		p, err := d.Plan(ctx, []seekparquet.Selection{{}}, []string{"size"}, nil)
		must(e.t, err)
		e.step("sum-all/scan", func() string {
			var sum, rows int64
			err := p.ScanValues(ctx, nil, func(b *seekparquet.ValueBatch) error {
				for i := range b.Len() {
					sum += b.Column(0).Int64(i)
				}
				rows += int64(b.Len())
				return nil
			})
			return withErr(fmt.Sprintf("%d rows, size %d", rows, sum), err)
		})
	}})
	out = append(out, ioScenario{"verify", func(e *ioEnv) {
		d := e.open()
		for _, temp := range []string{"cold", "warm"} {
			e.step("verify/"+temp, func() string { return errResult(d.Verify(ctx, nil)) })
		}
	}})
	return out
}

// ioRepeatedScenarios is the subset run on the repeated-key fixture.
func ioRepeatedScenarios(fx *ioFixture) []ioScenario {
	var out []ioScenario
	for _, s := range ioScenarios(fx) {
		switch s.name {
		case "open", "get/first", "get/mid", "get/last", "rows-key/mid", "rows-100/2-cols", "list-ra0", "scan-part", "verify":
			out = append(out, s)
		}
	}
	return out
}

func shardsText(ctx context.Context, shards []*seekparquet.Plan) (string, error) {
	res := make([]string, len(shards))
	errs := make([]error, len(shards))
	var wg sync.WaitGroup
	for i, s := range shards {
		wg.Go(func() {
			h := newBatchHash()
			res[i], errs[i] = h.text(s.Scan(ctx, nil, h.add))
		})
	}
	wg.Wait()
	return strings.Join(res, "; "), errors.Join(errs...)
}

// withErr joins a result and its error, for a step's result.
func withErr(text string, err error) string { return text + errResult(err) }

func errResult(err error) string {
	if err == nil {
		return "ok"
	}
	return fmt.Sprintf("error %q (refusal %q)", err, seekparquet.RefusalCodeOf(err))
}

func rowText(b *strings.Builder, r *seekparquet.Row) {
	fmt.Fprintf(b, "%q/%d", r.Key, r.Offset)
	for i, v := range r.Values {
		fmt.Fprintf(b, " %s=%s", r.Columns[i], v)
	}
	tok, err := r.Cursor().Encode()
	fmt.Fprintf(b, " cursor %s %v\n", tok, err)
}

func rowsText(rows []seekparquet.Row, err error) (string, error) {
	var b strings.Builder
	for i := range rows {
		rowText(&b, &rows[i])
	}
	return b.String(), err
}

func listText(ents []seekparquet.ListEntry, err error) (string, error) {
	var b strings.Builder
	for _, e := range ents {
		switch {
		case e.IsPrefix():
			fmt.Fprintf(&b, "prefix %q", e.CommonPrefix)
			if e.Exact != nil {
				b.WriteString(" exact ")
				rowText(&b, e.Exact)
			}
		default:
			rowText(&b, e.Row)
		}
		tok, err := e.Cursor().Encode()
		fmt.Fprintf(&b, " entry cursor %s %v\n", tok, err)
	}
	return b.String(), err
}

func planText(p *seekparquet.Plan, err error) (string, error) {
	if err != nil {
		return "", err
	}
	n, exact := p.Count()
	b, bexact := p.ProjectedBytes()
	return fmt.Sprintf("count %d %v bytes %d %v columns %q", n, exact, b, bexact, p.Columns()), nil
}

// batchHash digests a scan's batches: keys, offsets, values, cursors.
type batchHash struct {
	h    hash.Hash
	rows int
}

func newBatchHash() *batchHash { return &batchHash{h: sha256.New()} }

func (h *batchHash) add(b *seekparquet.Batch) error {
	cols := b.Columns()
	for i := range b.Len() {
		fmt.Fprintf(h.h, "%q/%d", b.Key(i), b.Offset(i))
		for c := range cols {
			fmt.Fprintf(h.h, " %s", b.Column(c).Value(i))
		}
		h.rows++
	}
	if n := b.Len(); n > 0 {
		tok, _ := b.Cursor(n - 1).Encode()
		fmt.Fprintf(h.h, " batch end %s\n", tok)
	}
	return nil
}

func (h *batchHash) text(err error) (string, error) {
	return fmt.Sprintf("%d rows sha256 %x ", h.rows, h.h.Sum(nil)), err
}

// ioStore is a way of serving the fixture: the Source the store is seen
// through, and the options the dataset is opened with.
type ioStore struct {
	name string
	wrap func(*fakestore.Source) seekparquet.Source
	opt  seekparquet.OpenOptions
}

var (
	// plainStore is a remote store as objfile's adapter serves it today:
	// Open with a metadata request, ReadAt and WithContext only.
	plainStore = ioStore{name: "plain", wrap: func(s *fakestore.Source) seekparquet.Source { return fakeSource{s} }}
	// capableStore offers every optional capability: sized opens (no
	// metadata request), a whole-manifest read and batched ranges.
	capableStore = ioStore{name: "capable", wrap: func(s *fakestore.Source) seekparquet.Source { return capableSource{fakeSource{s}} }}
	// noSpeculationStore is capableStore with ReadPolicy.DisableSpeculation:
	// no metadata retained. It has no budgets: it is a differential.
	noSpeculationStore = ioStore{name: "capable, no speculation", wrap: capableStore.wrap,
		opt: seekparquet.OpenOptions{Read: seekparquet.ReadPolicy{DisableSpeculation: true}}}
	// ioStores are the stores every scenario runs through.
	ioStores = []ioStore{plainStore, capableStore, noSpeculationStore}
)

// runIOScenarios runs every scenario through each store, each in its own
// bubble, and through the local directory, and reports each store's
// operations after checking that each step's result is the local one.
func runIOScenarios(t *testing.T, fx *ioFixture, scenarios []ioScenario, stores []ioStore) map[string]map[string]fakestore.Report {
	t.Helper()
	reports := map[string]map[string]fakestore.Report{}
	for _, store := range stores {
		reports[store.name] = map[string]fakestore.Report{}
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			want := map[string]string{}
			t.Run("local", func(t *testing.T) {
				sc.run(&ioEnv{t: t, fx: fx, results: want})
			})
			for _, store := range stores {
				got := map[string]string{}
				synctest.Test(t, func(t *testing.T) {
					src := fakestore.New(fx.objects, ioOptions)
					sc.run(&ioEnv{t: t, fx: fx, src: src, store: store, reports: reports[store.name], results: got})
				})
				for name, w := range want {
					if g := got[name]; g != w {
						t.Errorf("%s through the %s store:\n%.2000s\nfrom the local directory\n%.2000s", name, store.name, g, w)
					} else if strings.Contains(w, "error") {
						t.Errorf("%s: %s", name, w[max(0, len(w)-300):])
					}
				}
				if len(got) != len(want) {
					t.Errorf("steps: %d through the %s store, %d locally", len(got), store.name, len(want))
				}
			}
		})
	}
	return reports
}

func metricsOf(r fakestore.Report) ioMetrics {
	s := r.Summary()
	return ioMetrics{Requests: s.Requests, Rounds: s.Rounds, Bytes: s.Bytes, Repeats: s.Repeats, MaxInFlight: s.MaxInFlight}
}

// checkBudgets checks the reports against table, and prints the table
// with today's numbers when a current row differs or a row is missing.
func checkBudgets(t *testing.T, name string, table []ioBudget, reports map[string]fakestore.Report) {
	t.Helper()
	stale := false
	rows := map[string]ioBudget{}
	for _, b := range table {
		rows[b.op] = b
		r, ok := reports[b.op]
		if !ok {
			t.Errorf("%s: budget %s: no such operation", name, b.op)
			stale = true
			continue
		}
		got := metricsOf(r)
		// Always compare the measured snapshot with the reviewed current
		// values; for an achieved target, also enforce its acceptance
		// bounds.
		if got != b.now {
			t.Errorf("%s: %s: %+v, budget %+v\n%s", name, b.op, got, b.now, r.Trace(60))
			stale = true
		}
		if b.check == atTarget && (b.target.Requests >= 0 && got.Requests > b.target.Requests || b.target.Rounds >= 0 && got.Rounds > b.target.Rounds || got.Repeats != 0) {
			t.Errorf("%s: %s: %+v, over its target %+v (or repeating)\n%s", name, b.op, got, b.target, r.Trace(60))
		}
	}
	var ops []string
	for op := range reports {
		ops = append(ops, op)
		if pat := os.Getenv("SEEKPARQUET_IO_TRACE"); pat != "" && strings.Contains(name+"/"+op, pat) {
			t.Logf("%s: %s", name, reports[op].Trace(0))
		}
		if _, ok := rows[op]; !ok {
			t.Errorf("%s: operation %s has no budget", name, op)
			stale = true
		}
	}
	slices.Sort(ops)
	var b strings.Builder
	order := map[string]int{}
	for i, row := range table {
		order[row.op] = i
	}
	slices.SortStableFunc(ops, func(x, y string) int {
		ix, okx := order[x]
		iy, oky := order[y]
		switch {
		case okx && oky:
			return ix - iy
		case okx:
			return -1
		case oky:
			return 1
		}
		return 0
	})
	for _, op := range ops {
		m := metricsOf(reports[op])
		row, ok := rows[op]
		if !ok {
			row = ioBudget{op: op, target: none}
		}
		check := "current"
		if row.check == atTarget {
			check = "atTarget"
		}
		fmt.Fprintf(&b, "\t{%q, ioMetrics{%d, %d, %d, %d, %d}, ioTarget{%d, %d}, %s},\n", op, m.Requests, m.Rounds, m.Bytes, m.Repeats, m.MaxInFlight, row.target.Requests, row.target.Rounds, check)
	}
	if stale {
		t.Errorf("%s: today's table:\n%s", name, b.String())
	} else {
		t.Logf("%s:\n%s", name, b.String())
	}
}

// TestReadBudgets measures every operation on the budget fixture and
// checks it against ioBudgets, and its results against a local open.
func TestReadBudgets(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	fx := ioFixtureOf(t, ioSmall)
	t.Logf("fixture: %s", describeFixture(fx))
	reports := runIOScenarios(t, fx, ioScenarios(fx), ioStores)
	checkBudgets(t, "plain", ioBudgets, reports["plain"])
	checkBudgets(t, "capable", ioCapableBudgets, reports["capable"])
	// The part's end read when it is checked holds its whole page-index
	// region (fixture: about 28 KB, within 2 MiB): no two requests of an
	// operation read any byte of it twice, and no request reads in it
	// after the part's check.
	for _, store := range []string{"plain", "capable"} {
		for op, r := range reports[store] {
			for i, q := range r.Requests {
				region, ok := fx.indexRegion[q.Object]
				if !ok || q.Kind != fakestore.Range {
					continue
				}
				if q.Off >= region[0] && q.Off+q.Len <= region[1] {
					t.Errorf("%s %s: request %d+%d of %s reads the page-index region again", store, op, q.Off, q.Len, q.Object)
				}
				for _, e := range r.Requests[:i] {
					lo, hi := max(q.Off, e.Off, region[0]), min(q.Off+q.Len, e.Off+e.Len, region[1])
					if e.Object == q.Object && e.Kind == fakestore.Range && lo < hi {
						t.Errorf("%s %s: requests %d+%d and %d+%d of %s both read [%d, %d) of its page-index region", store, op, e.Off, e.Len, q.Off, q.Len, q.Object, lo, hi)
					}
				}
			}
		}
	}

	rfx := ioFixtureOf(t, ioRepeated)
	t.Logf("repeated fixture: %s", describeFixture(rfx))
	checkBudgets(t, "repeated", ioRepeatedBudgets, runIOScenarios(t, rfx, ioRepeatedScenarios(rfx), []ioStore{capableStore})["capable"])
}

// TestReadBudgetsAuditSize runs the operations on a fixture of the audit's
// size (4 parts of about 1M rows) and logs their numbers: unasserted
// evidence, compared with the audit by hand, not a budget. Set
// SEEKPARQUET_IO_BUDGET_FULL=1; it writes about 130 MB.
func TestReadBudgetsAuditSize(t *testing.T) {
	if os.Getenv("SEEKPARQUET_IO_BUDGET_FULL") == "" {
		t.Skip("set SEEKPARQUET_IO_BUDGET_FULL=1 to run the operations on a fixture of the audit's size")
	}
	seekparquet.NoTestReadAhead(t)
	fx := ioFixtureOf(t, ioAudit)
	t.Logf("fixture: %s", describeFixture(fx))
	reports := runIOScenarios(t, fx, ioScenarios(fx), []ioStore{capableStore})["capable"]
	var ops []string
	for op := range reports {
		ops = append(ops, op)
	}
	slices.Sort(ops)
	for _, op := range ops {
		t.Logf("unasserted %-32s %v", op, reports[op].Summary())
	}
}

// describeFixture reports a fixture's geometry.
func describeFixture(fx *ioFixture) string {
	var b strings.Builder
	d, err := seekparquet.OpenDir(context.Background(), fx.dir, seekparquet.OpenOptions{})
	if err != nil {
		return err.Error()
	}
	defer d.Close()
	m := d.Metadata()
	fmt.Fprintf(&b, "%d rows, %d parts, manifest %d B; part %d keys first %q mid %q other %q last %q", len(fx.keys), len(m.Parts), len(m.Manifest), fx.part, fx.first, fx.mid, fx.other, fx.last)
	for _, p := range m.Parts {
		fmt.Fprintf(&b, "\n  %s: %d rows, %d B, keys %q .. %q", p.Path, p.Rows, p.Bytes, p.MinKey, p.MaxKey)
	}
	return b.String()
}
