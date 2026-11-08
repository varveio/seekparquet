// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"encoding/binary"
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

func openParts(ctx context.Context, inputs []partInput, opt PartsOptions) (r *Reader, err error) {
	r = &Reader{}
	r.closing, r.closeNow = context.WithCancelCause(context.Background())
	defer func() {
		if err != nil {
			closeSources(inputs)
			r.closeNow(ErrClosed)
			r = nil
		}
	}()
	var previous []byte
	for i, in := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := openPart(ctx, in, opt)
		if err != nil {
			return nil, err
		}
		r.parts = append(r.parts, p)
		r.columns = mergeColumns(r.columns, p.footer.Schema())
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
			chunk := rg.Columns[p.key]
			if chunk.ColumnIndexOffset == 0 || chunk.ColumnIndexLength == 0 || chunk.OffsetIndexOffset == 0 || chunk.OffsetIndexLength == 0 {
				return nil, refusePart(CodeMissingPageIndex, in.Name, physical, "the key column lacks a ColumnIndex or an OffsetIndex")
			}
			key, err := p.firstKey(ctx, physical)
			if err != nil {
				return nil, err
			}
			if previous != nil && bytes.Compare(previous, key) >= 0 {
				e := keyOrder(in.Name, physical, 0, key)
				e.Detail = "the row group's first key is not above the keys before it"
				return nil, e
			}
			r.groups = append(r.groups, group{part: i, physical: physical, first: key, rows: rg.NumRows})
			r.rows += rg.NumRows
			previous, last = key, len(r.groups)-1
		}
		if err := r.checkBounds(ctx, p, in.PartSource, first, last); err != nil {
			return nil, err
		}
		if last >= 0 && in.MaxKey != nil {
			// The next part's first key must follow this part's last.
			previous = in.MaxKey
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
func (r *Reader) checkBounds(ctx context.Context, p *part, src PartSource, first, last int) error {
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
		key, err := p.lastKey(ctx, r.groups[last].physical)
		if err != nil {
			return err
		}
		if !bytes.Equal(key, src.MaxKey) {
			return refusePart(CodeBoundsMismatch, src.Name, r.groups[last].physical, "the decoded last key differs from the part's maximum key")
		}
	}
	return nil
}

// readFooter parses a part's footer, without its page indexes or bloom
// filters, after checking that the footer length the file declares fits
// in the file (so a hostile length cannot make it allocate gigabytes).
func readFooter(ctx context.Context, file io.ReaderAt, size int64) (*parquet.File, error) {
	at := bind(ctx, file)
	if size < 12 {
		return nil, errors.New("the file is too short to be Parquet")
	}
	var tail [8]byte
	if _, err := at.ReadAt(tail[:], size-8); err != nil && !(errors.Is(err, io.EOF)) {
		return nil, fmt.Errorf("read the footer length: %w", err)
	}
	if string(tail[4:]) != "PAR1" {
		return nil, errors.New("the file does not end with PAR1")
	}
	if n := int64(binary.LittleEndian.Uint32(tail[:4])); n > size-12 {
		return nil, fmt.Errorf("the footer length %d does not fit in the file", n)
	}
	f, err := parquet.OpenFile(at, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true), parquet.ReadBufferSize(64<<10))
	if err != nil {
		return nil, err
	}
	return f, nil
}

// footerError classifies an error reading a footer: the context's own
// error as is, anything else invalid_footer.
func footerError(ctx context.Context, name string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return refusePart(CodeInvalidFooter, name, -1, "%v", err)
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
	if err := checkRowCounts(footer); err != nil {
		return nil, refusePart(CodeInvalidFooter, in.Name, -1, "%v", err)
	}
	if opt.CheckFooter != nil {
		if err := opt.CheckFooter(in.Name, footer); err != nil {
			return nil, err
		}
	}
	p := &part{name: in.Name, file: in.File, size: in.Size, footer: footer,
		leaves: make(map[string]int), unsupported: make(map[string]bool)}
	schema := footer.Schema()
	keyField := topField(schema, KeyColumn)
	if keyField == nil || !keyField.Leaf() || !keyField.Required() || keyField.Type().Kind() != parquet.ByteArray {
		return nil, refusePart(CodeInvalidKeyColumn, in.Name, -1, "no top-level REQUIRED BYTE_ARRAY column named %s", KeyColumn)
	}
	for _, f := range schema.Fields() {
		if !f.Leaf() || f.Repeated() {
			p.unsupported[f.Name()] = true
			continue
		}
		leaf, ok := schema.Lookup(f.Name())
		if !ok {
			return nil, refusePart(CodeInvalidFooter, in.Name, -1, "column %s is not in the schema", f.Name())
		}
		p.leaves[f.Name()] = leaf.ColumnIndex
	}
	p.key = p.leaves[KeyColumn]
	for _, want := range opt.Expect {
		if err := checkExpectation(in.Name, schema, want); err != nil {
			return nil, err
		}
	}
	for g := range footer.Metadata().RowGroups {
		if len(footer.Metadata().RowGroups[g].Columns) != len(schema.Columns()) {
			return nil, refusePart(CodeInvalidFooter, in.Name, g, "the row group's column chunks do not match the schema")
		}
	}
	p.index = make([]atomic.Pointer[groupIndex], len(footer.Metadata().RowGroups))
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

// firstKey decodes the first key of a non-empty row group from its first
// key page.
func (p *part) firstKey(ctx context.Context, physical int) ([]byte, error) {
	chunk, err := p.chunk(physical, p.key)
	if err != nil {
		return nil, err
	}
	pages := chunk.PagesFrom(p.at(ctx))
	defer func() { _ = pages.Close() }()
	page, err := pages.ReadPage()
	if err != nil {
		return nil, p.readError(ctx, physical, fmt.Errorf("read the first key page: %w", err))
	}
	defer parquet.Release(page)
	values := make([]parquet.Value, 1)
	n, err := page.Values().ReadValues(values)
	if n == 0 {
		if err == nil || errors.Is(err, io.EOF) {
			err = errors.New("the first key page holds no values")
		}
		return nil, p.readError(ctx, physical, fmt.Errorf("read the first key: %w", err))
	}
	if values[0].IsNull() || len(values[0].ByteArray()) == 0 {
		e := refusePart(CodeInvalidKey, p.name, physical, "the first key is empty")
		e.Row = 0
		return nil, e
	}
	return bytes.Clone(values[0].ByteArray()), nil
}

// readError wraps an error reading a part's pages with its location.
func (p *part) readError(ctx context.Context, physical int, err error) error {
	if ctx.Err() != nil {
		return err
	}
	return fmt.Errorf("seekparquet: part %s row group %d: %w", p.name, physical, err)
}

// lastKey decodes the last key of a non-empty row group from its last key
// page, located through the OffsetIndex.
func (p *part) lastKey(ctx context.Context, physical int) ([]byte, error) {
	index, err := p.keyIndex(ctx, physical)
	if err != nil {
		return nil, err
	}
	var seq []byte
	cursor := p.keyCursor(ctx, physical, index, nil, &seq)
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

// at returns the part's file for reads under ctx.
func (p *part) at(ctx context.Context) io.ReaderAt { return bind(ctx, p.file) }

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

// mergeColumns adds the top-level non-repeated value columns of schema that
// names lacks.
func mergeColumns(names []string, schema *parquet.Schema) []string {
	for _, f := range schema.Fields() {
		if f.Name() == KeyColumn || !f.Leaf() || f.Repeated() || slices.Contains(names, f.Name()) {
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
