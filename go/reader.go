// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// reader serves bounded, ordered reads over a fixed set of parts in key
// order. It is safe for concurrent use: footers and page indexes are
// shared, and immutable once loaded; each request holds its own walkers.
//
// A part is checked the first time a read touches it (part), when every
// part has its key bounds and row count; otherwise every part is checked
// when the reader opens.
type reader struct {
	key    Key
	limits Limits
	policy ReadPolicy
	parts  []*part
	// rows is the dataset's row count.
	rows int64
	// bounded reports that every part has its key bounds, so reads are
	// routed to a part by its bounds. Otherwise flat lists every non-empty
	// row group in dataset order, and reads are routed by probing their
	// first keys.
	bounded bool
	// local reads as suits a local file (OpenDir with a zero ReadPolicy):
	// a part's ranges one at a time, no part metadata kept beyond the
	// footer, no request memo.
	local bool
	flat  []groupRef
	// fetch opens part i, checks it against its manifest entry and reads
	// its footer, the first time a read touches it; nil when the parts were
	// given open.
	fetch func(ctx context.Context, i int, st *ReadStats, deferHead bool) (fetchedInput, error)
	// root, when set, is the directory OpenDir opened, closed with the
	// reader.
	root *os.Root
	// columnsMu guards columns: the signature of every top-level column of
	// the checked parts, and the first checked part holding it (COL-5).
	columnsMu sync.Mutex
	columns   map[string]column

	listIndex atomic.Pointer[listIndex]
	listBuild sync.Mutex

	// meta accounts for the parts' retained metadata, once a part's check
	// first kept some (retainTail).
	meta atomic.Pointer[metaCache]

	indexMemory atomic.Int64
	// indexMu serializes retained-index admission and publication, including
	// first keys. No file I/O or page decoding runs under this lock.
	indexMu sync.Mutex
	// locsMemory is the memory charged to Limits.OffsetIndexMemory by the
	// parts' published page locations (part.locs), plus the reservations
	// of first loads still publishing, which a load that loses the race to
	// publish gives back.
	locsMemory atomic.Int64
	// readMemory bounds the bytes the operations hold read ahead
	// (Limits.DatasetReadMemory, windows).
	readMemory readBudget

	// mu guards the request leases: active counts the requests in
	// progress, and idle is signalled when it drops to zero; closed is set
	// as close begins, so that no request starts after it, and close then
	// waits for active. A request never waits on mu for another's lease,
	// so a request started inside another (a Rows in a Scan callback)
	// while close waits fails with ErrClosed instead of waiting for ever.
	mu        sync.Mutex
	idle      sync.Cond
	active    int
	closing   context.Context
	closeNow  context.CancelCauseFunc
	closeOnce sync.Once
	closed    bool // guarded by mu, beside closeOnce to fill its padding
	closeErr  error
}

// column is a top-level column's signature (COL-5) and the part it was
// first seen in.
type column struct{ signature, part string }

// part is one part: its manifest entry, and once checked, its parsed
// footer, located columns and the key page index of each row group,
// decoded once on first use.
type part struct {
	r    *reader
	name string
	// minKey and maxKey are the part's exact bounds, nil when not given.
	minKey, maxKey []byte
	// rows is the part's row count and start the dataset ordinal of its
	// first row: the rows of every part before it.
	rows, start int64
	// check is held while the part is checked, and err keeps its refusal
	// for the reader's lifetime. ready is set once the check has passed.
	check chan struct{}
	err   error
	ready atomic.Bool
	// flags holds partRefused and partLastChecked.
	flags atomic.Uint32

	// The fields below are set by the check, before ready; file and size
	// are set from the start when the part was given open, and footer when
	// it was given with its footer already read and checked (readFooter),
	// which the check then uses instead of reading it again.
	file   io.ReaderAt
	size   int64
	footer *parquet.File
	key    leaf
	// groups are the part's non-empty row groups.
	groups []*group
	// utf8Key reports a key column annotated STRING or UTF8 (KEY-6).
	utf8Key bool
	// reports is set when file reports its physical requests (IOReporter).
	reports bool
	// leaves are the top-level non-repeated leaf columns by name, key
	// included; unsupported are the top-level names that are groups or
	// repeated (COL-3).
	leaves      map[string]leaf
	unsupported map[string]bool
	index       []atomic.Pointer[groupIndex]
	// locs are the checked OffsetIndex page locations of projected column
	// chunks, by physical row group and column, published on first use
	// within Limits.OffsetIndexMemory (openColumn, reader.keepLocations).
	locs [][]atomic.Pointer[[]pageLoc]
	// meta holds the page-index bytes of the part's end that its check
	// read, nil when none are kept (reader.retainTail).
	meta atomic.Pointer[retained]
}

// The flags of a part.
const (
	// partRefused is set once the part's err is kept, for readers not
	// holding its check.
	partRefused uint32 = 1 << iota
	// partLastChecked is set once Verify decoded and checked the part's
	// last key page against its bounds.
	partLastChecked
)

// group is one non-empty row group.
type group struct {
	physical int
	rows     int64
	// start is the dataset ordinal of the group's first row.
	start int64
	// first is the group's decoded first key, once a read decoded it.
	first atomic.Pointer[[]byte]
}

// groupRef locates a row group in dataset order: its part, and its
// position among the part's non-empty row groups. A position past the
// part's last group stands for the next part's first.
type groupRef struct{ part, index int }

// less reports whether a precedes b in dataset order.
func (a groupRef) less(b groupRef) bool {
	return a.part < b.part || a.part == b.part && a.index < b.index
}

// next returns the position after g.
func (g groupRef) next() groupRef { return groupRef{g.part, g.index + 1} }

// group returns the row group at g, whose part is checked.
func (r *reader) group(g groupRef) *group { return r.parts[g.part].groups[g.index] }

// partInput is a part to open: its file, and its footer when the caller
// already read it. File must be safe for concurrent ReadAt calls, may
// implement ContextBinder and io.Closer, and must read the same immutable
// bytes for as long as the reader is open.
type partInput struct {
	name   string
	file   io.ReaderAt
	size   int64
	footer *parquet.File
	// minKey and maxKey, when set, are the part's exact first and last
	// keys. They route reads to the part without decoding its keys, and a
	// decoded key outside them, or a first or last key that differs from
	// them, is refused with bounds_mismatch. Parts without them are routed
	// by probing their row groups' first keys.
	minKey, maxKey []byte
	// rows, when positive, is the part's exact row count.
	rows int64
}

// fetchedInput is a part's input as a check opened it, with the end of its
// file that readFooter read, when it did.
type fetchedInput struct {
	partInput
	tail footerTail
}

// openParts opens parts in key order: OpenDataset after it has read the
// manifest, and tests over parts without one. It checks that parts'
// bounds, when given, ascend strictly, so that a group never crosses parts
// (PART-4), and that the dataset's row count fits a signed 64-bit ordinal.
// When every part has its bounds and row count, each part is checked the
// first time a read touches it; otherwise openParts checks every part
// (reader.part).
//
// openParts takes ownership of every file that implements io.Closer: when
// it fails it closes them all, and on success reader.close closes them.
func openParts(ctx context.Context, inputs []partInput, key Key, opt OpenOptions) (_ *reader, err error) {
	r := &reader{key: key, limits: opt.Limits.withDefaults(), policy: opt.Read.withDefaults(), bounded: true, columns: map[string]column{}}
	r.readMemory.free, r.readMemory.size = r.limits.DatasetReadMemory, r.limits.DatasetReadMemory
	r.closing, r.closeNow = context.WithCancelCause(context.Background())
	defer func() {
		if err != nil {
			for _, in := range inputs {
				closeFile(in.file)
			}
			r.closeNow(ErrClosed)
		}
	}()
	if key.Column == "" {
		return nil, invalidRequest("empty key column")
	}
	lazy := true
	for i, in := range inputs {
		if in.file == nil && in.rows <= 0 {
			return nil, fmt.Errorf("seekparquet: part %s has no file", in.name)
		}
		p := &part{r: r, name: in.name, file: in.file, size: in.size, footer: in.footer, rows: in.rows,
			minKey: bytes.Clone(in.minKey), maxKey: bytes.Clone(in.maxKey), check: make(chan struct{}, 1)}
		r.parts = append(r.parts, p)
		if p.minKey == nil || p.maxKey == nil {
			r.bounded = false
		}
		lazy = lazy && in.rows > 0
		if err := r.checkBounds(i); err != nil {
			return nil, err
		}
	}
	if lazy && r.bounded {
		for _, p := range r.parts {
			// Row-group positions and cursor arithmetic use signed dataset
			// ordinals. Per-part counts do not bound this cross-part sum.
			if p.rows > math.MaxInt64-r.rows {
				return nil, exceeded(LimitDatasetRows, -1, math.MaxInt64, "the dataset row count overflows at part %s", p.name)
			}
			p.start = r.rows
			r.rows += p.rows
		}
		return r, nil
	}
	var st ReadStats
	for i, p := range r.parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.start, p.rows = r.rows, 0
		if _, err := r.part(ctx, i, &st); err != nil {
			return nil, err
		}
		r.rows += p.rows
		for j := range p.groups {
			r.flat = append(r.flat, groupRef{i, j})
		}
	}
	return r, nil
}

// checkBounds requires part i's bounds, when given, to be ordered and to
// begin after the bounds of the bounded part before it: equal keys in two
// parts would be a group across parts (PART-4).
func (r *reader) checkBounds(i int) error {
	p := r.parts[i]
	if p.minKey == nil || p.maxKey == nil {
		return nil
	}
	if bytes.Compare(p.minKey, p.maxKey) > 0 {
		return refusePart(CodeBoundsMismatch, p.name, -1, "the part's minimum key is above its maximum key")
	}
	for j := i - 1; j >= 0; j-- {
		if q := r.parts[j]; q.maxKey != nil {
			if bytes.Compare(q.maxKey, p.minKey) >= 0 {
				return refusePart(CodeBoundsMismatch, p.name, -1, "the part does not begin after part %s ends", q.name)
			}
			break
		}
	}
	return nil
}

// part returns part i once it is checked, checking it if no read has yet
// (CHECK-1): its file opens with the manifest's size, its footer is
// readable, self-contained (FILE-1) and not encrypted (FILE-2), with
// row-group row counts that sum to its own and the manifest's row count,
// its stamps agree with the manifest (STAMP-2), its key column meets KEY-1
// and KEY-5, every non-empty row group meets RG-1, and its columns agree
// with those of every part checked before it (COL-5). Concurrent reads
// wait for one check. A refusal reached with a live context is kept: the
// part stays refused for the reader's lifetime. Other failures (the source,
// a limit), and any failure once ctx is done, are not kept, so a later read
// checks the part again.
func (r *reader) part(ctx context.Context, i int, st *ReadStats) (*part, error) {
	return r.partWith(ctx, i, st, nil)
}

// partWith is part completing the check from what Verify prefetched of
// the part, when pre is not nil: the outcome is the one fetching the part
// then would have reached. A prefetched part that a check by another read
// made unnecessary is closed.
func (r *reader) partWith(ctx context.Context, i int, st *ReadStats, pre *fetchedPart) (*part, error) {
	p := r.parts[i]
	if p.ready.Load() {
		pre.close()
		return p, nil
	}
	select {
	case p.check <- struct{}{}:
	case <-ctx.Done():
		pre.close()
		return nil, ctx.Err()
	}
	defer func() { <-p.check }()
	switch {
	case p.ready.Load():
		pre.close()
		return p, nil
	case p.err != nil:
		pre.close()
		return nil, detach(p.err)
	}
	if err := r.checkPart(ctx, i, st, pre); err != nil {
		if ctx.Err() != nil { // a check cut short is not an outcome: the part stays unchecked
			return nil, ctx.Err()
		}
		var refusal *RefusalError
		var unsupported *UnsupportedError
		switch {
		case errors.As(err, &refusal):
			p.err = detach(refusal)
			p.flags.Or(partRefused)
		case errors.As(err, &unsupported):
			p.err = detach(unsupported)
			p.flags.Or(partRefused)
		}
		return nil, err
	}
	p.ready.Store(true)
	return p, nil
}

// checkPart opens part i when the reader fetches its parts, checks it and
// fills it in. The caller holds p.check.
func (r *reader) checkPart(ctx context.Context, i int, st *ReadStats, pre *fetchedPart) error {
	p := r.parts[i]
	in := fetchedInput{partInput: partInput{name: p.name, file: p.file, size: p.size, footer: p.footer}}
	switch {
	case pre != nil: // fetched by Verify, which counted it
		if pre.err != nil {
			return pre.err
		}
		in = pre.in
	case r.fetch != nil: // counts the part opened once the Source opens it
		var err error
		if in, err = r.fetch(ctx, i, st, false); err != nil {
			return err
		}
	default:
		st.PartsOpened++
	}
	err := r.fill(ctx, p, in, st)
	if err != nil && (r.fetch != nil || pre != nil) {
		closeFile(in.file)
	}
	if err == nil && pre != nil {
		pre.adopted = true
	}
	return err
}

// fill checks the footer of a part's file and fills in the part.
func (r *reader) fill(ctx context.Context, p *part, in fetchedInput, st *ReadStats) error {
	footer := in.footer
	if footer == nil {
		var err error
		if footer, in.tail, err = readFooter(ctx, in.name, in.file, in.size, &r.limits, st, r.policy.tailWindow(), r.width(), false); err != nil {
			return err
		}
	}
	// A row count given with the part (the manifest's) is never rewritten:
	// reads route by it while other parts are checked.
	rows := p.rows
	switch {
	case rows == 0: // a part given without its row count, checked when the reader opens
		if footer.NumRows() > math.MaxInt64-p.start {
			return exceeded(LimitDatasetRows, -1, math.MaxInt64, "the dataset row count overflows at part %s", p.name)
		}
		rows = footer.NumRows()
	case footer.NumRows() != rows:
		return refusePart(CodePartMismatch, in.name, -1, "the part holds %d rows, its manifest entry %d", footer.NumRows(), rows)
	}
	meta := footer.Metadata()
	top, dup := topLevel(meta.Schema)
	utf8Key, err := checkKeyColumn(in.name, meta, top, dup, leafCount(meta.Schema), r.key.Column)
	if err != nil {
		return err
	}
	p.footer, p.utf8Key = footer, utf8Key
	p.leaves, p.unsupported = map[string]leaf{}, map[string]bool{}
	signatures := make([]column, 0, len(footer.Schema().Fields()))
	names := make([]string, 0, cap(signatures))
	for _, f := range footer.Schema().Fields() {
		name := f.Name()
		e := top[name]
		names, signatures = append(names, name), append(signatures, column{e.signature, p.name})
		rep, _ := e.se.RepetitionType.Get()
		if e.leaf < 0 || rep == format.Repeated || dup[name] {
			p.unsupported[name] = true
			continue
		}
		p.leaves[name] = leaf{index: e.leaf, typ: f.Type(), optional: rep == format.Optional}
	}
	p.key = p.leaves[r.key.Column]
	p.index = make([]atomic.Pointer[groupIndex], len(meta.RowGroups))
	p.locs = make([][]atomic.Pointer[[]pageLoc], len(meta.RowGroups))
	for physical := range meta.RowGroups {
		p.locs[physical] = make([]atomic.Pointer[[]pageLoc], len(meta.RowGroups[physical].Columns))
	}
	var groups []*group
	start := p.start
	for physical := range meta.RowGroups {
		rg := &meta.RowGroups[physical]
		if rg.NumRows == 0 {
			continue
		}
		if err := checkSorted(p, physical, rg); err != nil {
			return err
		}
		groups = append(groups, &group{physical: physical, rows: rg.NumRows, start: start})
		start += rg.NumRows
	}
	if len(groups) == 0 && (p.minKey != nil || p.maxKey != nil) {
		return refusePart(CodeBoundsMismatch, in.name, -1, "the part holds no rows but has key bounds")
	}
	// COL-5 last, so that only parts that passed every other check set the
	// signatures later parts are held to.
	r.columnsMu.Lock()
	defer r.columnsMu.Unlock()
	for i, name := range names {
		if want, ok := r.columns[name]; ok && want.signature != signatures[i].signature {
			return refusePart(CodeInconsistentColumn, in.name, -1,
				"column %s differs in physical type, repetition or nesting from part %s", name, want.part)
		}
	}
	for i, name := range names {
		if _, ok := r.columns[name]; !ok {
			r.columns[name] = signatures[i]
		}
	}
	if p.rows == 0 {
		p.rows = rows
	}
	p.file, p.size, p.groups, p.reports = in.file, in.size, groups, reportsIO(in.file)
	r.retainTail(p, in.tail)
	return nil
}

// checkParts checks every part that no read has checked, in manifest order.
func (r *reader) checkParts(ctx context.Context, st *ReadStats) error {
	for i := range r.parts {
		if _, err := r.part(ctx, i, st); err != nil {
			return err
		}
	}
	return nil
}

// verify checks every part, then decodes every row group's first key page
// and each bounded part's last key page, in dataset order, as one request
// (CHECK-3), counting into st. Parts are fetched, and the pages it needs
// read, by at most ReadPolicy.Concurrency parts at once (verifyParts); the
// checks that depend on other parts, the verdicts kept, and the decodes run
// in manifest order, the pages taken from the request's memo, so that
// Verify reports the first failure in the same part and check order as
// serial verification, independently of I/O completion order. A part whose
// last page it checked once is not checked again.
func (r *reader) verify(ctx context.Context, st *ReadStats) error {
	log := pageLog{unique: r.key.Unique}
	if err := r.verifyParts(ctx, log.pageMemo(), st); err != nil {
		return err
	}
	for i, p := range r.parts {
		for j := range p.groups {
			g := groupRef{i, j}
			if _, err := r.first(ctx, g, &log, st); err != nil {
				return err
			}
			if j == len(p.groups)-1 && p.maxKey != nil && p.flags.Load()&partLastChecked == 0 {
				k, err := r.keyPages(ctx, g, &log, st)
				if err != nil {
					return err
				}
				if err := k.decode(len(k.index.pages) - 1); err != nil {
					return err
				}
				p.flags.Or(partLastChecked)
			}
		}
	}
	return nil
}

// fetchedPart is what Verify fetched of a part ahead of its check: the part
// opened, its footer read and checked against its manifest entry and its
// head established, or the error doing so, with the key pages read with
// it in memo, and the counts of that work in st. Its pages join the
// request's memo only once the part's check adopts this very handle
// (adopted): another read may check the part first, with a handle of its
// own, and a Source's immutability holds per handle.
type fetchedPart struct {
	in      fetchedInput
	err     error
	memo    pageMemo
	st      ReadStats
	adopted bool
}

// close closes a prefetched part whose handle the check did not adopt.
func (pre *fetchedPart) close() {
	if pre != nil && pre.err == nil && !pre.adopted {
		closeFile(pre.in.file)
	}
}

// verifyParts checks every part no read has checked, in manifest order,
// fetching ahead what each check reads, and fetches into memo the key
// pages Verify will decode of the parts already checked. The fetches run
// in a window of at most Concurrency parts, from the part being checked
// on: opening a part, reading its end, head and key pages (prefetchPart,
// fetchProbes), work that depends on no other part. Every range they read
// is charged to the operation's limit as it is read. The rest of a check
// (key column, row groups, the consistency of columns with the parts
// before it), and keeping its verdict, runs in manifest order as its fetch
// completes, and the fetch is released then: the window bounds the tails
// and pages held. The first failure stops it.
func (r *reader) verifyParts(ctx context.Context, memo *pageMemo, st *ReadStats) error {
	n := len(r.parts)
	window := max(1, r.policy.Concurrency)
	fetched := make([]*fetchedPart, n)
	done := make([]chan struct{}, n)
	var wg sync.WaitGroup
	// The fetches still running when a check fails, or ctx ends, are
	// stopped (wctx) and waited for.
	wctx, stop := context.WithCancel(ctx)
	defer func() {
		stop()
		wg.Wait()
		for _, pre := range fetched { // fetched, never checked
			if pre != nil {
				st.Add(pre.st)
			}
			pre.close()
		}
	}()
	launch := func(i int) {
		p := r.parts[i]
		check := r.fetch != nil && !p.ready.Load() && p.flags.Load()&partRefused == 0
		probes := !r.local && p.ready.Load() && r.wantsProbes(p)
		if !check && !probes {
			return
		}
		pre := new(fetchedPart)
		fetched[i], done[i] = pre, make(chan struct{})
		wg.Go(func() {
			defer close(done[i])
			if check {
				r.prefetchPart(wctx, i, pre)
			} else { // a checked part's pages go to the request's memo
				_, _ = r.fetchProbes(wctx, p, p.file, p.size, p.footer, p.retainedBytes, false, memo, &pre.st)
			}
		})
	}
	next := 0
	for i := range n {
		for ; next < n && next < i+window && ctx.Err() == nil; next++ {
			launch(next)
		}
		pre := fetched[i]
		if pre != nil {
			<-done[i]
			fetched[i] = nil
			st.Add(pre.st)
			if pre.in.file == nil && pre.err == nil {
				pre = nil // the pages of a checked part
			}
		}
		if err := ctx.Err(); err != nil {
			pre.close()
			return err
		}
		_, err := r.partWith(ctx, i, st, pre)
		if pre != nil && pre.adopted {
			memo.adopt(&pre.memo)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// prefetchPart opens part i, reads its end and checks its footer against
// its manifest entry, then reads its head with the key pages Verify will
// decode (fetchProbes) and judges the head as readFooter would have,
// filling pre. A local read keeps nothing of the part's end.
func (r *reader) prefetchPart(ctx context.Context, i int, pre *fetchedPart) {
	p := r.parts[i]
	in, err := r.fetch(ctx, i, &pre.st, !r.local)
	if err != nil {
		pre.err = err
		return
	}
	if r.local {
		in.tail = footerTail{}
		pre.in = in
		return
	}
	tail := in.tail
	lookup := func(off, length int64) []byte {
		if length <= 0 || off < tail.off || off-tail.off > int64(len(tail.data))-length {
			return nil
		}
		return tail.data[off-tail.off : off-tail.off+length]
	}
	head, headErr := r.fetchProbes(ctx, p, in.file, in.size, in.footer, lookup, tail.headDeferred, &pre.memo, &pre.st)
	if tail.headDeferred && headErr == nil {
		at := storageReader{r: bind(ctx, in.file), op: operationOf(ctx), st: &pre.st, name: in.name, unreported: unreported(ctx, reportsIO(in.file))}
		headErr = headVerdict(ctx, in.name, at, in.size, head, tail, nil)
	}
	if headErr != nil {
		closeFile(in.file)
		pre.err = footerError(ctx, in.name, headErr)
		return
	}
	pre.in = in
}

// wantsProbes reports whether Verify would read a page of checked part p:
// a row group's first key page not yet known, or its last.
func (r *reader) wantsProbes(p *part) bool {
	if p.maxKey != nil && p.flags.Load()&partLastChecked == 0 && len(p.groups) > 0 {
		return true
	}
	for _, g := range p.groups {
		if g.first.Load() == nil {
			return true
		}
	}
	return false
}

// headMergeGap is how far into a part its first key page may begin for
// Verify to read the head with it in one range.
const headMergeGap = 64 << 10

// fetchProbes reads together, into memo, the key pages Verify will decode
// in part p whose footer is footer: the first page of each row group whose
// first key is not known and, when p has bounds and they were not checked,
// its last page; and, with wantHead, the part's 4-byte head, merged into
// the first group's page when it begins within headMergeGap. Page
// locations come from the key OffsetIndexes that lookup finds in retained
// bytes, decoded but not checked: a page whose location cannot be found
// this way, or lies outside the file or the page limits, is not fetched,
// and Verify reads it itself, checking its index first. Only the pages
// within the part's share of requestMemoBytes are fetched; each is kept in
// an allocation of its own. A page that fails is not kept, and fails when
// Verify reads it; a failed merged range is followed by an exact 4-byte
// head read, so that only a failure of the head itself is returned.
func (r *reader) fetchProbes(ctx context.Context, p *part, file io.ReaderAt, size int64, footer *parquet.File, lookup func(off, length int64) []byte, wantHead bool, memo *pageMemo, st *ReadStats) (head []byte, err error) {
	type probe struct{ off, size int64 }
	var probes []probe
	leaf, ok := footer.Schema().Lookup(r.key.Column)
	groups := footer.Metadata().RowGroups
	lastGroup := -1
	for g := range groups {
		if groups[g].NumRows > 0 {
			lastGroup = g
		}
	}
	located := func(g int, last bool) (probe, bool) {
		if !ok || leaf.ColumnIndex >= len(groups[g].Columns) {
			return probe{}, false
		}
		c := &groups[g].Columns[leaf.ColumnIndex]
		if c.OffsetIndexOffset <= 0 || c.OffsetIndexLength <= 0 || int64(c.OffsetIndexLength) > r.limits.PageIndexBytes ||
			c.OffsetIndexOffset > size-int64(c.OffsetIndexLength) {
			return probe{}, false
		}
		raw := lookup(c.OffsetIndexOffset, int64(c.OffsetIndexLength))
		if raw == nil {
			return probe{}, false
		}
		var index format.OffsetIndex
		if _, err := decodeBounded(raw, &index, r.limits.MetadataMemory); err != nil || len(index.PageLocations) == 0 {
			return probe{}, false
		}
		loc := index.PageLocations[0]
		if last {
			loc = index.PageLocations[len(index.PageLocations)-1]
		}
		n := int64(loc.CompressedPageSize)
		if n <= 0 || loc.Offset < 0 || loc.Offset > size-n || n > r.limits.PageBytes+r.limits.headerBytes() {
			return probe{}, false
		}
		return probe{loc.Offset, n}, true
	}
	known := func(physical int) bool {
		if !p.ready.Load() {
			return false
		}
		for _, grp := range p.groups {
			if grp.physical == physical {
				return grp.first.Load() != nil
			}
		}
		return true
	}
	for g := range groups {
		if groups[g].NumRows == 0 || known(g) {
			continue
		}
		if pr, ok := located(g, false); ok {
			probes = append(probes, pr)
		}
	}
	if p.maxKey != nil && p.flags.Load()&partLastChecked == 0 && lastGroup >= 0 {
		if pr, ok := located(lastGroup, true); ok {
			probes = append(probes, pr)
		}
	}
	budget := requestMemoBytes / max(1, int64(len(r.parts)))
	var ranges []ReadRange
	var kept []probe
	if wantHead {
		head = make([]byte, 4)
		ranges = append(ranges, ReadRange{Off: 0, Buf: head})
	}
	merged := false
	total := int64(0)
	for i, pr := range probes {
		if total+pr.size > budget || memo.get(p, pr.off, pr.size) != nil {
			continue
		}
		total += pr.size
		if i == 0 && wantHead && pr.off >= 4 && pr.off+pr.size <= headMergeGap {
			ranges[0].Buf, merged = make([]byte, pr.off+pr.size), true
		} else {
			ranges = append(ranges, ReadRange{Off: pr.off, Buf: make([]byte, pr.size)})
		}
		kept = append(kept, pr)
	}
	if len(ranges) == 0 {
		return head, nil
	}
	errs := readRanges(ctx, rangeFile{file: file, name: p.name, reports: reportsIO(file), concurrency: r.width()}, ranges, st)
	for _, pr := range kept {
		for j, rg := range ranges {
			if errs != nil && errs[j] != nil || rg.Off > pr.off || pr.off+pr.size > rg.Off+int64(len(rg.Buf)) {
				continue
			}
			page := rg.Buf[pr.off-rg.Off : pr.off-rg.Off+pr.size]
			if int64(len(rg.Buf)) != pr.size {
				page = ownCopy(page) // keep the page, not the head before it
			}
			memo.put(p, pr.off, page, false)
			break
		}
	}
	if !wantHead {
		return nil, nil
	}
	if errs == nil || errs[0] == nil {
		copy(head, ranges[0].Buf[:4])
		return head, nil
	}
	if !merged {
		return nil, errs[0]
	}
	// The page merged with the head failed: the head alone decides.
	at := storageReader{r: bind(ctx, file), op: operationOf(ctx), st: st, name: p.name, unreported: unreported(ctx, reportsIO(file))}
	head = make([]byte, 4)
	if err := at.readFull(head, 0); err != nil {
		return nil, err
	}
	return head, nil
}

// closeFile closes file when it implements io.Closer.
func closeFile(file io.ReaderAt) error {
	if c, ok := file.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// first returns the first key of row group g, decoding its first key page
// (with the group's key page index, CHECK-2) when no read has yet. The
// page joins the request's log.
func (r *reader) first(ctx context.Context, g groupRef, log *pageLog, st *ReadStats) ([]byte, error) {
	if _, err := r.part(ctx, g.part, st); err != nil {
		return nil, err
	}
	if x := r.listIndex.Load(); x != nil {
		if group := x.group(g); group != nil {
			return group.base, nil
		}
	}
	if k := r.group(g).first.Load(); k != nil {
		return *k, nil
	}
	k, err := r.keyPages(ctx, g, log, st)
	if err != nil {
		return nil, err
	}
	if err := k.decode(0); err != nil {
		return nil, err
	}
	return k.page.key(0), nil
}

// noteFirst records the decoded first key of row group g, after checking
// it against the first keys already known of the groups around it in
// checked parts: in the declared order, and equal only within one part
// (PART-4).
func (r *reader) noteFirst(g groupRef, key []byte) error {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()
	if r.group(g).first.Load() != nil {
		return nil
	}
	check := func(a, b groupRef, previous, next []byte) error {
		if r.outOfOrder(previous, next) || bytes.Equal(previous, next) && a.part != b.part {
			e := keyOrder(r.parts[b.part].name, r.group(b).physical, 0, next)
			e.Detail = "a row group's first key breaks the declared key order with the row group before it"
			return e
		}
		return nil
	}
	if prev, ok := r.adjacent(g, -1); ok {
		if k := r.group(prev).first.Load(); k != nil {
			if err := check(prev, g, *k, key); err != nil {
				return err
			}
		}
	}
	if next, ok := r.adjacent(g, 1); ok {
		if k := r.group(next).first.Load(); k != nil {
			if err := check(g, next, key, *k); err != nil {
				return err
			}
		}
	}
	if err := r.reserveIndex(int64(len(key))); err != nil {
		return err
	}
	k := bytes.Clone(key)
	r.group(g).first.Store(&k)
	return nil
}

// adjacent returns the row group before g (step -1) or after it (step 1)
// in dataset order, when it is in a checked part; parts with no non-empty
// row group are passed over.
func (r *reader) adjacent(g groupRef, step int) (groupRef, bool) {
	i, j := g.part, g.index+step
	for {
		if p := r.parts[i]; j >= 0 && j < len(p.groups) {
			return groupRef{i, j}, true
		}
		if i += step; i < 0 || i >= len(r.parts) || !r.parts[i].ready.Load() {
			return groupRef{}, false
		}
		if j = 0; step < 0 {
			j = len(r.parts[i].groups) - 1
		}
	}
}

// reserveIndex charges retained index data before publication. The caller
// holds indexMu, so a duplicate load must check for publication first.
func (r *reader) reserveIndex(size int64) error {
	used := r.indexMemory.Load()
	if size > r.limits.IndexMemory-used {
		return exceeded(LimitIndexMemory, used+size, r.limits.IndexMemory, "key page indexes and first keys")
	}
	r.indexMemory.Store(used + size)
	return nil
}

// end is the position after the dataset's last row group.
func (r *reader) end() groupRef { return groupRef{len(r.parts), 0} }

// groupFor returns the row group in which a read of the keys at or after
// from (after it unless inclusive) begins: the predecessor of the first
// group whose first key is above from, or at it when the read includes
// from and keys repeat, clamped to the first group of the part that can
// hold such a key; r.end() when no group can. Parts are chosen by their
// bounds when every part has them, and only the chosen part is checked;
// row groups by binary search over their first keys, decoding a group's
// first key page the first time a search needs it.
func (r *reader) groupFor(ctx context.Context, from []byte, inclusive bool, log *pageLog, st *ReadStats) (groupRef, error) {
	if len(r.parts) == 0 {
		return r.end(), nil
	}
	if from == nil {
		return groupRef{}, nil
	}
	var at func(int) groupRef
	var hi int
	if r.bounded {
		i := sort.Search(len(r.parts), func(i int) bool {
			cmp := bytes.Compare(r.parts[i].maxKey, from)
			return cmp > 0 || cmp == 0 && inclusive
		})
		if i == len(r.parts) {
			return r.end(), nil
		}
		if cmp := bytes.Compare(from, r.parts[i].minKey); cmp < 0 || cmp == 0 && inclusive {
			return groupRef{i, 0}, nil
		}
		p, err := r.part(ctx, i, st)
		if err != nil {
			return groupRef{}, err
		}
		at, hi = func(j int) groupRef { return groupRef{i, j} }, len(p.groups)
	} else {
		if len(r.flat) == 0 {
			return r.end(), nil
		}
		at, hi = func(j int) groupRef { return r.flat[j] }, len(r.flat)
	}
	// When keys repeat and the read includes from, search for the first
	// first key at or above from: its predecessor can hold earlier rows of
	// the same key (ROUTE-1). Otherwise a group whose first key is from
	// holds the first row the read can return, or precedes it.
	a, b := 1, hi
	for a < b {
		mid := int(uint(a+b) >> 1)
		key, err := r.first(ctx, at(mid), log, st)
		if err != nil {
			return groupRef{}, err
		}
		if cmp := bytes.Compare(key, from); cmp > 0 || cmp == 0 && inclusive && !r.key.Unique {
			b = mid
		} else {
			a = mid + 1
		}
	}
	return at(a - 1), nil
}

// partAt returns the part holding the dataset row ordinal, or the number
// of parts when the dataset has fewer rows.
func (r *reader) partAt(ordinal int64) int {
	return sort.Search(len(r.parts), func(i int) bool { return r.parts[i].start+r.parts[i].rows > ordinal })
}

// groupAt returns the row group holding the dataset row ordinal, checking
// its part, or r.end() when the dataset has fewer rows.
func (r *reader) groupAt(ctx context.Context, ordinal int64, st *ReadStats) (groupRef, error) {
	i := r.partAt(ordinal)
	if i == len(r.parts) {
		return r.end(), nil
	}
	p, err := r.part(ctx, i, st)
	if err != nil {
		return groupRef{}, err
	}
	return groupRef{i, sort.Search(len(p.groups), func(j int) bool { return p.groups[j].start+p.groups[j].rows > ordinal })}, nil
}

// startOf returns the dataset ordinal of row group g's first row: that of
// its part's first row for the part's first group, whether or not the part
// is checked, and the dataset's row count past the last part.
func (r *reader) startOf(g groupRef) int64 {
	if g.part >= len(r.parts) {
		return r.rows
	}
	p := r.parts[g.part]
	if g.index > 0 {
		return p.groups[g.index].start
	}
	return p.start
}

// startsAtOrAfter reports whether row group g is known, without reading
// it, to begin at or after key: its first key, or its part's minimum key
// when it is the part's first group, is at or after key.
func (r *reader) startsAtOrAfter(g groupRef, key []byte) bool {
	p := r.parts[g.part]
	if p.ready.Load() && g.index < len(p.groups) {
		if first := p.groups[g.index].first.Load(); first != nil {
			return bytes.Compare(*first, key) >= 0
		}
	}
	return g.index == 0 && p.minKey != nil && bytes.Compare(p.minKey, key) >= 0
}

// outOfOrder reports whether next may not follow previous in the declared
// key order (KEY-3): it is below previous, or equal when keys are unique.
func (r *reader) outOfOrder(previous, next []byte) bool {
	cmp := bytes.Compare(previous, next)
	return cmp > 0 || cmp == 0 && r.key.Unique
}

// bind returns file bound to ctx when it is a ContextBinder.
func bind(ctx context.Context, file io.ReaderAt) io.ReaderAt {
	if b, ok := file.(ContextBinder); ok {
		return b.WithContext(ctx)
	}
	return file
}

// at returns the part's file for reads under ctx, its errors marked as
// the source's own, its calls counted in st.
func (p *part) at(ctx context.Context, st *ReadStats) storageReader {
	op := operationOf(ctx)
	return storageReader{r: bind(ctx, p.file), op: op, st: st, name: p.name, unreported: !p.reports || op == nil}
}

// begin takes a read lease that close waits for, and returns the
// request's operation, which is its context: it ends with ctx, or with
// close (cause ErrClosed), observes the physical requests reporting
// receivers make, and limits the ranges in flight to the policy's
// Concurrency. An unknown cache mode (WithCacheMode) is an invalid
// request. Once close has begun (it cancels r.closing first), begin fails
// with ErrClosed at once. The caller ends the operation with end.
func (r *reader) begin(ctx context.Context) (*operation, error) {
	if err := checkCacheMode(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed || r.closing.Err() != nil {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	r.active++
	r.mu.Unlock()
	rctx, cancel := context.WithCancelCause(ctx)
	op := newOperation(rctx, r.policy.Concurrency)
	op.r, op.cancel = r, cancel
	op.stop = context.AfterFunc(r.closing, func() { cancel(ErrClosed) })
	return op, nil
}

// end ends an operation begun by reader.begin, adding the physical
// requests it observed to st, and releases its lease.
func (o *operation) end(st *ReadStats) {
	o.win.Load().close(st)
	o.stop()
	o.cancel(nil)
	if c := o.c.Load(); c != nil {
		c.addTo(st)
	}
	r := o.r
	r.mu.Lock()
	if r.active--; r.active == 0 {
		r.idle.Broadcast()
	}
	r.mu.Unlock()
}

// done ends a request's operation as end does, first mapping *err, of a
// request ended by close, to ErrClosed (finish); ctx is the request's.
func (o *operation) done(ctx context.Context, err *error, st *ReadStats) {
	*err = finish(ctx, o, *err)
	o.end(st)
}

// finish maps an error of a request ended by close to ErrClosed.
func finish(ctx, rctx context.Context, err error) error {
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(rctx), ErrClosed) && !errors.Is(err, ErrClosed) {
		return fmt.Errorf("%w (%w)", ErrClosed, err)
	}
	return err
}

// close ends the reads in progress as soon as they reach a cancellation
// point, waits for them to return, closes every part file that implements
// io.Closer and the release, and fails reads after it with ErrClosed. Later calls return
// the first call's result.
func (r *reader) close() error {
	r.closeOnce.Do(func() {
		r.closeNow(ErrClosed)
		r.mu.Lock()
		r.closed = true
		if r.idle.L == nil {
			r.idle.L = &r.mu
		}
		for r.active > 0 {
			r.idle.Wait()
		}
		r.mu.Unlock()
		var errs []error
		for _, p := range r.parts {
			if err := closeFile(p.file); err != nil {
				errs = append(errs, fmt.Errorf("close %s: %w", p.name, err))
			}
		}
		if r.root != nil {
			if err := r.root.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
