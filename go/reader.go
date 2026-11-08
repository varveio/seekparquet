// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// KeyColumn is the name of the column every part is sorted by (KEY-1).
const KeyColumn = "key"

// ContextBinder is implemented by a file whose reads can be bounded by a
// context. The reader calls WithContext once per operation (reading a
// footer, loading a page index, reading pages) and reads through the
// returned io.ReaderAt, so that a cancelled request, or Close, can end a
// read that waits on the source (for a descriptor, a connection, a retry).
//
// WithContext may be called concurrently, for operations that run at the
// same time. Each call must return an independent view bound to its own
// context, and must not change shared state such as a "current" context
// of the file.
//
// A file that does not implement ContextBinder is read as a plain
// io.ReaderAt: the reader checks its context between pages, but cannot
// interrupt a ReadAt call that is already blocked.
type ContextBinder interface {
	WithContext(ctx context.Context) io.ReaderAt
}

// PartSource is one part given to OpenParts.
type PartSource struct {
	// Name identifies the part in errors.
	Name string
	// File reads the part. It must be safe for concurrent ReadAt calls (an
	// *os.File is), and must read the same immutable bytes for as long as
	// the Reader is open: a remote file must stay bound to one stored
	// object (one version or generation) across requests and retries. It
	// may implement ContextBinder and io.Closer.
	File io.ReaderAt
	// Size is the part's length in bytes.
	Size int64
	// MinKey and MaxKey, when set, are the part's exact first and last
	// keys (a manifest records them). They route reads to the part without
	// decoding its keys, and a decoded key outside them, or a first or last
	// key that differs from them, is refused with bounds_mismatch. Parts
	// without them are routed by probing their row groups' first keys.
	MinKey, MaxKey []byte
}

// ColumnExpectation is a consumer's requirement on a value column, by
// top-level name, checked in every part on open. A part that breaks it
// fails the open with an *ExpectationError, which is the consumer's policy
// failing, not a refusal of the data.
//
// Physical types, repetition and nesting must already agree across parts
// (COL-5, checked on open without any expectation); an expectation adds the
// consumer's own requirements, such as presence in every part or a
// particular type.
type ColumnExpectation struct {
	// Name is the column's top-level name, matched exactly (COL-6).
	Name string
	// MustExist requires every part to have the column. Otherwise a part
	// without it is accepted, and reads report the column absent (COL-2)
	// for that part's rows. This is separate from Parquet nullability.
	MustExist bool
	// Kinds lists the physical kinds the column may have; empty accepts
	// any. Where the column exists it must be a top-level, non-repeated
	// leaf.
	Kinds []parquet.Kind
	// Check, when set, is called with the column's node in each part that
	// has it (to require a logical type, say); an error fails the open.
	Check func(node parquet.Node) error
}

// PartsOptions configures OpenParts.
type PartsOptions struct {
	// Expect lists column expectations.
	Expect []ColumnExpectation
	// Limits bounds the resources the parts can make the reader use.
	Limits Limits
	// VerifyOnOpen makes OpenParts check every non-empty row group's key
	// page index and decode its first key page, and decode each part's
	// last key page, before it returns (CHECK-3). Otherwise these are
	// loaded when a read first needs them.
	VerifyOnOpen bool
	// CheckFooter, when set, is called once per part with its parsed
	// footer, before the part's key column is checked. An error fails the
	// open and is returned as is. The footer must not be modified.
	CheckFooter func(part string, footer *parquet.File) error
	// CheckGroup, when set, is called for every non-empty row group in key
	// order with its part's footer. An error fails the open and is
	// returned as is.
	CheckGroup func(part string, footer *parquet.File, physical int, group *format.RowGroup) error
}

// Reader serves bounded, ordered reads over a fixed set of parts in key
// order. It is safe for concurrent use: footers and page indexes are
// shared, and immutable once loaded; each read holds its own page readers.
// Callers must not modify the sources, the slices of a request while it
// runs, or a Stats that another read is updating.
type Reader struct {
	parts   []*part
	groups  []*group
	columns []string
	rows    int64
	limits  Limits
	// bounded reports that every part holding rows has its key bounds, so
	// reads are routed to a part by its bounds; routed lists those parts.
	bounded     bool
	routed      []*part
	indexMemory atomic.Int64

	mu        sync.RWMutex
	closed    bool
	closing   context.Context
	closeNow  context.CancelCauseFunc
	closeOnce sync.Once
	closeErr  error
}

// part is one opened part: its parsed footer, located columns and the key
// page index of each row group, decoded once on first use.
type part struct {
	r      *Reader
	name   string
	file   io.ReaderAt
	size   int64
	footer *parquet.File
	key    int
	// minKey and maxKey are the part's exact bounds, nil when not given.
	minKey, maxKey []byte
	// firstGroup and endGroup delimit the part's non-empty row groups in
	// Reader.groups.
	firstGroup, endGroup int
	// utf8Key reports a key column annotated STRING or UTF8 (KEY-6).
	utf8Key bool
	// signatures spell each top-level column's physical type, repetition
	// and nesting (COL-5).
	signatures map[string]string
	// leaves are the top-level non-repeated leaf columns by name, key
	// included; unsupported are the top-level names that are groups or
	// repeated (COL-3).
	leaves      map[string]int
	unsupported map[string]bool
	index       []atomic.Pointer[groupIndex]
	// checked records the page headers already checked against the
	// limits, by column chunk and offset.
	checked sync.Map
}

// group is one non-empty row group in global key order.
type group struct {
	part     int
	physical int
	rows     int64
	// first is the group's decoded first key, once a read decoded it.
	first atomic.Pointer[[]byte]
}

// partInput is a source with its footer when the caller already read it.
type partInput struct {
	PartSource
	footer *parquet.File
}

// OpenParts opens parts given directly, in key order. It is the advanced
// path: unlike OpenDataset it reads no manifest and checks no footer
// stamps and no sorting_columns (RG-1); a caller adds such checks with
// CheckFooter and CheckGroup. It checks that each footer is readable,
// self-contained (FILE-1) and not encrypted (FILE-2), with row-group row
// counts that sum to its own; the key column (KEY-1, KEY-5); and that
// columns agree across parts (COL-5). With VerifyOnOpen it also checks every
// row group's first key page and each part's last key page.
//
// OpenParts takes ownership of every File that implements io.Closer from
// the call on: when it fails it closes them all before returning, and on
// success Reader.Close closes them. A File given twice is closed twice.
func OpenParts(ctx context.Context, parts []PartSource, opt PartsOptions) (*Reader, error) {
	inputs := make([]partInput, len(parts))
	for i, p := range parts {
		inputs[i] = partInput{PartSource: p}
	}
	return openParts(ctx, inputs, opt)
}

func openParts(ctx context.Context, inputs []partInput, opt PartsOptions) (_ *Reader, err error) {
	r := &Reader{limits: opt.Limits.withDefaults(), bounded: true}
	r.closing, r.closeNow = context.WithCancelCause(context.Background())
	defer func() {
		if err != nil {
			closeSources(inputs)
			r.closeNow(ErrClosed)
		}
	}()
	signatures := map[string]string{} // COL-5: column name -> signature
	owner := map[string]string{}      // column name -> first part holding it
	for i, in := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := r.openPart(ctx, in, opt)
		if err != nil {
			return nil, err
		}
		r.parts = append(r.parts, p)
		for name, sig := range p.signatures {
			if want, ok := signatures[name]; ok && want != sig {
				return nil, refusePart(CodeInconsistentColumn, in.Name, -1,
					"column %s differs in physical type, repetition or nesting from part %s", name, owner[name])
			} else if !ok {
				signatures[name], owner[name] = sig, in.Name
			}
		}
		r.columns = mergeColumns(r.columns, p)
		p.firstGroup = len(r.groups)
		for physical := range p.footer.Metadata().RowGroups {
			rg := &p.footer.Metadata().RowGroups[physical]
			if rg.NumRows == 0 {
				continue
			}
			if opt.CheckGroup != nil {
				if err := opt.CheckGroup(in.Name, p.footer, physical, rg); err != nil {
					return nil, err
				}
			}
			r.groups = append(r.groups, &group{part: i, physical: physical, rows: rg.NumRows})
			r.rows += rg.NumRows
		}
		p.endGroup = len(r.groups)
		if p.endGroup > p.firstGroup {
			r.routed = append(r.routed, p)
			if p.minKey == nil || p.maxKey == nil {
				r.bounded = false
			}
		}
		if p.endGroup == p.firstGroup && (p.minKey != nil || p.maxKey != nil) {
			return nil, refusePart(CodeBoundsMismatch, in.Name, -1, "the part holds no rows but has key bounds")
		}
	}
	if opt.VerifyOnOpen {
		if err := r.verify(ctx); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// verify decodes every row group's first key page and each bounded part's
// last key page, in dataset order, as one request.
func (r *Reader) verify(ctx context.Context) error {
	var log pageLog
	for g := range r.groups {
		if _, err := r.first(ctx, g, &log, nil); err != nil {
			return err
		}
		p := r.parts[r.groups[g].part]
		if g == p.endGroup-1 && p.maxKey != nil {
			if err := r.decodeLast(ctx, g, &log); err != nil {
				return err
			}
		}
	}
	return nil
}

// closeSources closes every source that implements io.Closer.
func closeSources(inputs []partInput) {
	for _, in := range inputs {
		if c, ok := in.File.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

func (r *Reader) openPart(ctx context.Context, in partInput, opt PartsOptions) (*part, error) {
	if in.File == nil {
		return nil, fmt.Errorf("seekparquet: part %s has no file", in.Name)
	}
	footer := in.footer
	if footer == nil {
		var err error
		if footer, err = readFooter(ctx, in.Name, in.File, in.Size, &r.limits); err != nil {
			return nil, err
		}
	}
	meta := footer.Metadata()
	if opt.CheckFooter != nil {
		if err := opt.CheckFooter(in.Name, footer); err != nil {
			return nil, err
		}
	}
	top, dup := topLevel(meta.Schema)
	utf8Key, err := checkKeyColumn(in.Name, meta, top, dup, leafCount(meta.Schema))
	if err != nil {
		return nil, err
	}
	p := &part{r: r, name: in.Name, file: in.File, size: in.Size, footer: footer, utf8Key: utf8Key,
		minKey: bytes.Clone(in.MinKey), maxKey: bytes.Clone(in.MaxKey),
		leaves: make(map[string]int), unsupported: make(map[string]bool), signatures: make(map[string]string)}
	for name, e := range top {
		p.signatures[name] = e.signature
		rep, _ := e.se.RepetitionType.Get()
		if e.leaf < 0 || rep == format.Repeated || dup[name] {
			p.unsupported[name] = true
			continue
		}
		p.leaves[name] = e.leaf
	}
	p.key = p.leaves[KeyColumn]
	for _, want := range opt.Expect {
		if err := checkExpectation(in.Name, footer.Schema(), want); err != nil {
			return nil, err
		}
	}
	p.index = make([]atomic.Pointer[groupIndex], len(meta.RowGroups))
	return p, nil
}

func topField(schema *parquet.Schema, name string) parquet.Field {
	for _, f := range schema.Fields() {
		if f.Name() == name {
			return f
		}
	}
	return nil
}

func checkExpectation(partName string, schema *parquet.Schema, want ColumnExpectation) error {
	fail := func(format string, args ...any) error {
		return &ExpectationError{Part: partName, Column: want.Name, Detail: fmt.Sprintf(format, args...)}
	}
	f := topField(schema, want.Name)
	if f == nil {
		if want.MustExist {
			return fail("the column is missing")
		}
		return nil
	}
	if !f.Leaf() || f.Repeated() {
		return fail("not a top-level, non-repeated leaf column")
	}
	if len(want.Kinds) > 0 && !slices.Contains(want.Kinds, f.Type().Kind()) {
		return fail("physical type %s is not one of %v", f.Type().Kind(), want.Kinds)
	}
	if want.Check != nil {
		if err := want.Check(f); err != nil {
			return fail("%v", err)
		}
	}
	return nil
}

// chunk returns the file-backed column chunk of leaf column in physical
// row group.
func (p *part) chunk(physical, column int) (*parquet.FileColumnChunk, error) {
	chunk, ok := p.footer.RowGroups()[physical].ColumnChunks()[column].(*parquet.FileColumnChunk)
	if !ok {
		return nil, errors.New("column chunk is not file-backed")
	}
	return chunk, nil
}

// first returns the first key of row group g, decoding its first key page
// (with the group's key page index, CHECK-2) when no read has yet. The
// page joins the request's log.
func (r *Reader) first(ctx context.Context, g int, log *pageLog, stats *Stats) ([]byte, error) {
	if k := r.groups[g].first.Load(); k != nil {
		return *k, nil
	}
	c, err := r.cursor(ctx, g, log, stats)
	if err != nil {
		return nil, err
	}
	defer c.close()
	if err := c.decode(0); err != nil {
		return nil, err
	}
	return *r.groups[g].first.Load(), nil
}

// decodeLast decodes the last key page of row group g.
func (r *Reader) decodeLast(ctx context.Context, g int, log *pageLog) error {
	c, err := r.cursor(ctx, g, log, nil)
	if err != nil {
		return err
	}
	defer c.close()
	return c.decode(len(c.index.pages) - 1)
}

// noteFirst records the decoded first key of row group g, after checking
// it against the first keys already known of the groups around it.
func (r *Reader) noteFirst(g int, key []byte) error {
	if r.groups[g].first.Load() != nil {
		return nil
	}
	grp := r.groups[g]
	name := r.parts[grp.part].name
	if g > 0 {
		if prev := r.groups[g-1].first.Load(); prev != nil && bytes.Compare(*prev, key) >= 0 {
			e := keyOrder(name, grp.physical, 0, key)
			e.Detail = "the row group's first key is not above the previous row group's"
			return e
		}
	}
	if g+1 < len(r.groups) {
		if next := r.groups[g+1].first.Load(); next != nil && bytes.Compare(key, *next) >= 0 {
			n := r.groups[g+1]
			e := keyOrder(r.parts[n.part].name, n.physical, 0, *next)
			e.Detail = "the row group's first key is not above the previous row group's"
			return e
		}
	}
	k := bytes.Clone(key)
	r.groups[g].first.CompareAndSwap(nil, &k)
	return nil
}

// groupFor returns the row group in which a read from from begins: the
// last group whose first key is at or before from, or the first group of
// the part that can hold from; len(groups) when no group can. Parts are
// chosen by their bounds when every part has them; row groups by binary
// search over their first keys, decoding a group's first key page the first
// time a search needs it.
func (r *Reader) groupFor(ctx context.Context, from []byte, log *pageLog, stats *Stats) (int, error) {
	if from == nil || len(r.groups) == 0 {
		return 0, nil
	}
	lo, hi := 0, len(r.groups)
	if r.bounded {
		parts := r.routed
		i := sort.Search(len(parts), func(i int) bool { return bytes.Compare(parts[i].maxKey, from) >= 0 })
		if i == len(parts) {
			return len(r.groups), nil
		}
		lo, hi = parts[i].firstGroup, parts[i].endGroup
		if bytes.Compare(from, parts[i].minKey) <= 0 {
			return lo, nil
		}
	}
	// The answer is the last group in [lo, hi) whose first key is at or
	// before from, or lo: search (lo, hi) for the first group above from.
	a, b := lo+1, hi
	for a < b {
		mid := int(uint(a+b) >> 1)
		key, err := r.first(ctx, mid, log, stats)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(key, from) > 0 {
			b = mid
		} else {
			a = mid + 1
		}
	}
	return a - 1, nil
}

// bind returns file bound to ctx when it is a ContextBinder.
func bind(ctx context.Context, file io.ReaderAt) io.ReaderAt {
	if b, ok := file.(ContextBinder); ok {
		return b.WithContext(ctx)
	}
	return file
}

// at returns the part's file for reads under ctx, its errors marked as
// the source's own.
func (p *part) at(ctx context.Context) io.ReaderAt { return storageReader{bind(ctx, p.file)} }

// Parts returns the number of parts.
func (r *Reader) Parts() int { return len(r.parts) }

// RowGroups returns the number of non-empty row groups.
func (r *Reader) RowGroups() int { return len(r.groups) }

// Rows returns the number of rows in the non-empty row groups.
func (r *Reader) Rows() int64 { return r.rows }

// Columns returns the names of the top-level, non-repeated value columns
// of the parts (the key column excluded), in the first part's order and
// then any later part's additions. The slice is a copy.
func (r *Reader) Columns() []string { return slices.Clone(r.columns) }

// mergeColumns adds the top-level non-repeated value columns of p that
// names lacks, in p's schema order.
func mergeColumns(names []string, p *part) []string {
	for _, f := range p.footer.Schema().Fields() {
		if _, ok := p.leaves[f.Name()]; !ok || f.Name() == KeyColumn || slices.Contains(names, f.Name()) {
			continue
		}
		names = append(names, f.Name())
	}
	return names
}

// begin takes a read lease that Close waits for, and returns the request's
// context: it ends with ctx, or with Close (cause ErrClosed).
func (r *Reader) begin(ctx context.Context) (context.Context, func(), error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	rctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(r.closing, func() { cancel(ErrClosed) })
	return rctx, func() {
		stop()
		cancel(nil)
		r.mu.RUnlock()
	}, nil
}

// finish maps an error of a request ended by Close to ErrClosed.
func finish(ctx, rctx context.Context, err error) error {
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(rctx), ErrClosed) && !errors.Is(err, ErrClosed) {
		return fmt.Errorf("%w (%w)", ErrClosed, err)
	}
	return err
}

// Close ends the reads in progress as soon as they reach a cancellation
// point (a context-bound source read, or a page boundary), waits for them
// to return, closes every part file that implements io.Closer, and fails
// reads after it with ErrClosed. It is safe to call more than once; later
// calls return the first call's result.
//
// Close must not be called from a ColumnSink callback: the scan calling the
// sink holds the read lease that Close waits for.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.closeNow(ErrClosed)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closed = true
		var errs []error
		for _, p := range r.parts {
			if closer, ok := p.file.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					errs = append(errs, fmt.Errorf("close %s: %w", p.name, err))
				}
			}
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
