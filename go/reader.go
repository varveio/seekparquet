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

// ContextBinder is implemented by a part file whose reads can be bounded by
// a context. The reader calls WithContext once per operation (opening a
// footer, loading a page index, reading pages) and reads through the
// returned io.ReaderAt, so a cancelled request, or Close, can end a read
// that waits on the source (for a descriptor, a connection, a retry).
//
// A part file that does not implement it is read as a plain io.ReaderAt:
// the reader checks its context between pages, but cannot interrupt a
// ReadAt call that is already blocked.
type ContextBinder interface {
	WithContext(ctx context.Context) io.ReaderAt
}

// PartSource is one part given to OpenParts.
type PartSource struct {
	// Name identifies the part in errors.
	Name string
	// File reads the part. It must be safe for concurrent ReadAt calls (an
	// *os.File is), and its bytes must not change while the Reader is
	// open. It may implement ContextBinder and io.Closer.
	File io.ReaderAt
	// Size is the part's length in bytes.
	Size int64
	// MinKey and MaxKey, when set, are the part's exact first and last
	// keys (a manifest records them); OpenParts decodes both and refuses
	// the parts with bounds_mismatch when they differ. Nil checks nothing.
	MinKey, MaxKey []byte
}

// ColumnExpectation is a consumer's requirement on a value column, by
// top-level name, checked in every part on open. A part that breaks it
// fails the open with an *ExpectationError, which is the consumer's policy
// failing, not a refusal of the data.
//
// Without an expectation, any column may still be projected: its values
// come back as each part stores them, and may differ in type from part to
// part.
type ColumnExpectation struct {
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
	// CheckFooter, when set, is called once per part with its parsed
	// footer, before the part's key column is checked. An error fails the
	// open and is returned as is. The footer must not be modified.
	CheckFooter func(part string, footer *parquet.File) error
	// CheckGroup, when set, is called for every non-empty row group in key
	// order with its part's footer, before the group's first key is
	// decoded. An error fails the open and is returned as is.
	CheckGroup func(part string, footer *parquet.File, physical int, group *format.RowGroup) error
}

// Reader serves bounded, ordered reads over a fixed set of parts in key
// order. It is safe for concurrent use: footers and page indexes are
// shared, and immutable once loaded; each read holds its own page readers.
// Callers must not modify the sources, the slices of a request while it
// runs, or a Stats that another read is updating.
type Reader struct {
	parts   []*part
	groups  []group
	columns []string
	rows    int64

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
	name   string
	file   io.ReaderAt
	size   int64
	footer *parquet.File
	key    int
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
}

// group is one non-empty row group in global key order.
type group struct {
	part     int
	physical int
	first    []byte
	rows     int64
}

// partInput is a source with its footer when the caller already read it.
type partInput struct {
	PartSource
	footer *parquet.File
}

// OpenParts opens parts given directly, in key order, decoding the first
// key of every non-empty row group to route reads. It is the advanced
// path: unlike OpenDataset it reads no manifest and checks no footer
// stamps and no sorting_columns (RG-1); a caller adds such checks with
// CheckFooter and CheckGroup. It checks the key column (KEY-1), that every
// non-empty row group's key column has both page indexes, that the decoded
// row-group first keys ascend strictly, and each part's MinKey and MaxKey
// when given.
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
	r := &Reader{}
	r.closing, r.closeNow = context.WithCancelCause(context.Background())
	defer func() {
		if err != nil {
			closeSources(inputs)
			r.closeNow(ErrClosed)
		}
	}()
	signatures := map[string]string{} // COL-5: column name -> signature
	owner := map[string]string{}      // column name -> first part holding it
	var log pageLog
	for i, in := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := openPart(ctx, in, opt)
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
		first, last := len(r.groups), -1
		for physical := range p.footer.Metadata().RowGroups {
			rg := &p.footer.Metadata().RowGroups[physical]
			if rg.NumRows == 0 {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if opt.CheckGroup != nil {
				if err := opt.CheckGroup(in.Name, p.footer, physical, rg); err != nil {
					return nil, err
				}
			}
			g := len(r.groups)
			key, err := p.firstKey(ctx, g, physical, &log)
			if err != nil {
				return nil, err
			}
			r.groups = append(r.groups, group{part: i, physical: physical, first: key, rows: rg.NumRows})
			r.rows += rg.NumRows
			last = g
		}
		if err := r.checkBounds(ctx, p, in.PartSource, first, last, &log); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// closeSources closes every source that implements io.Closer.
func closeSources(inputs []partInput) {
	for _, in := range inputs {
		if c, ok := in.File.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

// checkBounds compares a part's decoded first and last keys with the exact
// bounds given for it.
func (r *Reader) checkBounds(ctx context.Context, p *part, src PartSource, first, last int, log *pageLog) error {
	if src.MinKey == nil && src.MaxKey == nil {
		return nil
	}
	if last < 0 {
		return refusePart(CodeBoundsMismatch, src.Name, -1, "the part holds no rows but has key bounds")
	}
	if src.MinKey != nil && !bytes.Equal(r.groups[first].first, src.MinKey) {
		return refusePart(CodeBoundsMismatch, src.Name, r.groups[first].physical, "the decoded first key differs from the part's minimum key")
	}
	if src.MaxKey != nil {
		key, err := p.lastKey(ctx, last, r.groups[last].physical, log)
		if err != nil {
			return err
		}
		if !bytes.Equal(key, src.MaxKey) {
			return refusePart(CodeBoundsMismatch, src.Name, r.groups[last].physical, "the decoded last key differs from the part's maximum key")
		}
	}
	return nil
}

func openPart(ctx context.Context, in partInput, opt PartsOptions) (*part, error) {
	if in.File == nil {
		return nil, fmt.Errorf("seekparquet: part %s has no file", in.Name)
	}
	footer := in.footer
	if footer == nil {
		var err error
		if footer, err = readFooter(ctx, in.File, in.Size); err != nil {
			return nil, footerError(ctx, in.Name, err)
		}
	}
	meta := footer.Metadata()
	if err := checkSelfContained(in.Name, meta); err != nil {
		return nil, err
	}
	if err := checkRowCounts(footer); err != nil {
		return nil, refusePart(CodeInvalidFooter, in.Name, -1, "%v", err)
	}
	top, dup, err := topLevel(meta.Schema)
	if err != nil {
		return nil, refusePart(CodeInvalidFooter, in.Name, -1, "%v", err)
	}
	ncols := len(footer.Schema().Columns())
	for g := range meta.RowGroups {
		if len(meta.RowGroups[g].Columns) != ncols {
			return nil, refusePart(CodeInvalidFooter, in.Name, g, "the row group's column chunks do not match the schema")
		}
	}
	if opt.CheckFooter != nil {
		if err := opt.CheckFooter(in.Name, footer); err != nil {
			return nil, err
		}
	}
	utf8Key, err := checkKeyColumn(in.Name, meta, top, dup)
	if err != nil {
		return nil, err
	}
	p := &part{name: in.Name, file: in.File, size: in.Size, footer: footer, utf8Key: utf8Key,
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

// checkRowCounts checks that the row groups hold non-negative counts that
// sum to the footer's row count.
func checkRowCounts(footer *parquet.File) error {
	metadata := footer.Metadata()
	var total int64
	for _, rg := range metadata.RowGroups {
		if rg.NumRows < 0 || total+rg.NumRows < total {
			return errors.New("row group row counts are invalid")
		}
		total += rg.NumRows
	}
	if total != metadata.NumRows {
		return errors.New("the row groups do not hold the footer's row count")
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

// firstKey decodes the first key of a non-empty row group, global index
// g, from its first key page, after checking the group's key page index
// (CHECK-2 applies to every row group a reader reads).
func (p *part) firstKey(ctx context.Context, g, physical int, log *pageLog) ([]byte, error) {
	index, err := p.keyIndex(ctx, physical)
	if err != nil {
		return nil, err
	}
	cursor := p.keyCursor(ctx, g, physical, index, nil, log)
	defer cursor.close()
	if err := cursor.decode(0); err != nil {
		return nil, err
	}
	return bytes.Clone(cursor.values[0].ByteArray()), nil
}

// lastKey decodes the last key of a non-empty row group from its last key
// page, located through the OffsetIndex.
func (p *part) lastKey(ctx context.Context, g, physical int, log *pageLog) ([]byte, error) {
	index, err := p.keyIndex(ctx, physical)
	if err != nil {
		return nil, err
	}
	cursor := p.keyCursor(ctx, g, physical, index, nil, log)
	defer cursor.close()
	if err := cursor.decode(len(index.pages) - 1); err != nil {
		return nil, err
	}
	return bytes.Clone(cursor.values[len(cursor.values)-1].ByteArray()), nil
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

// groupFor returns the row group that holds from: the last one whose first
// key is at or before it, or the first group.
func (r *Reader) groupFor(from []byte) int {
	if from == nil {
		return 0
	}
	n := sort.Search(len(r.groups), func(i int) bool { return bytes.Compare(r.groups[i].first, from) > 0 })
	return max(n-1, 0)
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
