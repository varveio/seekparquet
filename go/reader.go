// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// reader serves bounded, ordered reads over a fixed set of parts in key
// order. It is safe for concurrent use: footers and page indexes are
// shared, and immutable once loaded; each request holds its own walkers.
type reader struct {
	key    Key
	limits Limits
	parts  []*part
	groups []*group
	rows   int64
	// bounded reports that every part has its key bounds, so reads are
	// routed to a part by its bounds.
	bounded     bool
	indexMemory atomic.Int64
	// indexMu serializes retained-index admission and publication, including
	// first keys. No file I/O or page decoding runs under this lock.
	indexMu sync.Mutex

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
	r      *reader
	name   string
	file   io.ReaderAt
	size   int64
	footer *parquet.File
	key    leaf
	// minKey and maxKey are the part's exact bounds, nil when not given.
	minKey, maxKey []byte
	// firstGroup and endGroup delimit the part's non-empty row groups in
	// reader.groups.
	firstGroup, endGroup int
	// utf8Key reports a key column annotated STRING or UTF8 (KEY-6).
	utf8Key bool
	// signatures spell each top-level column's physical type, repetition
	// and nesting (COL-5).
	signatures map[string]string
	// leaves are the top-level non-repeated leaf columns by name, key
	// included; unsupported are the top-level names that are groups or
	// repeated (COL-3).
	leaves      map[string]leaf
	unsupported map[string]bool
	index       []atomic.Pointer[groupIndex]
}

// group is one non-empty row group in dataset order.
type group struct {
	part     int
	physical int
	rows     int64
	// start is the dataset ordinal of the group's first row: the rows
	// before it in the non-empty row groups of every part.
	start int64
	// first is the group's decoded first key, once a read decoded it.
	first atomic.Pointer[[]byte]
}

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
}

// openParts opens parts in key order: OpenDataset after it has read the
// manifest, and tests over parts without one. It checks that each footer
// is readable, self-contained (FILE-1) and not encrypted (FILE-2), with
// row-group row counts that sum to its own; the key column (KEY-1, KEY-5);
// that columns agree across parts (COL-5); RG-1 for every non-empty row
// group; and that parts' bounds, when given, ascend strictly, so that a
// group never crosses parts (PART-4). With VerifyOnOpen it also checks
// every row group's first key page and each part's last key page.
//
// openParts takes ownership of every file that implements io.Closer: when
// it fails it closes them all, and on success reader.close closes them.
func openParts(ctx context.Context, inputs []partInput, key Key, opt Options) (_ *reader, err error) {
	r := &reader{key: key, limits: opt.Limits.withDefaults(), bounded: true}
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
	signatures := map[string]string{} // COL-5: column name -> signature
	owner := map[string]string{}      // column name -> first part holding it
	for i, in := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := r.openPart(ctx, in)
		if err != nil {
			return nil, err
		}
		r.parts = append(r.parts, p)
		for name, sig := range p.signatures {
			if want, ok := signatures[name]; ok && want != sig {
				return nil, refusePart(CodeInconsistentColumn, in.name, -1,
					"column %s differs in physical type, repetition or nesting from part %s", name, owner[name])
			} else if !ok {
				signatures[name], owner[name] = sig, in.name
			}
		}
		if err := r.checkBounds(i); err != nil {
			return nil, err
		}
		p.firstGroup = len(r.groups)
		for physical := range p.footer.Metadata().RowGroups {
			rg := &p.footer.Metadata().RowGroups[physical]
			if rg.NumRows == 0 {
				continue
			}
			if err := checkSorted(p, physical, rg); err != nil {
				return nil, err
			}
			r.groups = append(r.groups, &group{part: i, physical: physical, rows: rg.NumRows, start: r.rows})
			r.rows += rg.NumRows
		}
		p.endGroup = len(r.groups)
		if p.endGroup == p.firstGroup && (p.minKey != nil || p.maxKey != nil) {
			return nil, refusePart(CodeBoundsMismatch, in.name, -1, "the part holds no rows but has key bounds")
		}
		if p.minKey == nil || p.maxKey == nil {
			r.bounded = false
		}
	}
	if opt.VerifyOnOpen {
		if err := r.verify(ctx); err != nil {
			return nil, err
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

// verify decodes every row group's first key page and each bounded part's
// last key page, in dataset order, as one request.
func (r *reader) verify(ctx context.Context) error {
	log := pageLog{unique: r.key.Unique}
	var st stats
	for g := range r.groups {
		if _, err := r.first(ctx, g, &log, &st); err != nil {
			return err
		}
		p := r.parts[r.groups[g].part]
		if g == p.endGroup-1 && p.maxKey != nil {
			k, err := r.keyPages(ctx, g, &log, &st)
			if err != nil {
				return err
			}
			if err := k.decode(len(k.index.pages) - 1); err != nil {
				return err
			}
		}
	}
	return nil
}

// closeFile closes file when it implements io.Closer.
func closeFile(file io.ReaderAt) error {
	if c, ok := file.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (r *reader) openPart(ctx context.Context, in partInput) (*part, error) {
	if in.file == nil {
		return nil, fmt.Errorf("seekparquet: part %s has no file", in.name)
	}
	footer := in.footer
	if footer == nil {
		var err error
		if footer, err = readFooter(ctx, in.name, in.file, in.size, &r.limits); err != nil {
			return nil, err
		}
	}
	meta := footer.Metadata()
	top, dup := topLevel(meta.Schema)
	utf8Key, err := checkKeyColumn(in.name, meta, top, dup, leafCount(meta.Schema), r.key.Column)
	if err != nil {
		return nil, err
	}
	p := &part{r: r, name: in.name, file: in.file, size: in.size, footer: footer, utf8Key: utf8Key,
		minKey: bytes.Clone(in.minKey), maxKey: bytes.Clone(in.maxKey),
		leaves: map[string]leaf{}, unsupported: map[string]bool{}, signatures: map[string]string{}}
	for _, f := range footer.Schema().Fields() {
		name := f.Name()
		e := top[name]
		p.signatures[name] = e.signature
		rep, _ := e.se.RepetitionType.Get()
		if e.leaf < 0 || rep == format.Repeated || dup[name] {
			p.unsupported[name] = true
			continue
		}
		p.leaves[name] = leaf{index: e.leaf, typ: f.Type(), optional: rep == format.Optional}
	}
	p.key = p.leaves[r.key.Column]
	p.index = make([]atomic.Pointer[groupIndex], len(meta.RowGroups))
	return p, nil
}

// first returns the first key of row group g, decoding its first key page
// (with the group's key page index, CHECK-2) when no read has yet. The
// page joins the request's log.
func (r *reader) first(ctx context.Context, g int, log *pageLog, st *stats) ([]byte, error) {
	if k := r.groups[g].first.Load(); k != nil {
		return *k, nil
	}
	k, err := r.keyPages(ctx, g, log, st)
	if err != nil {
		return nil, err
	}
	if err := k.decode(0); err != nil {
		return nil, err
	}
	return *r.groups[g].first.Load(), nil
}

// noteFirst records the decoded first key of row group g, after checking
// it against the first keys already known of the groups around it: in the
// declared order, and equal only within one part (PART-4).
func (r *reader) noteFirst(g int, key []byte) error {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()
	if r.groups[g].first.Load() != nil {
		return nil
	}
	check := func(a, b int, previous, next []byte) error {
		if r.outOfOrder(previous, next) || bytes.Equal(previous, next) && r.groups[a].part != r.groups[b].part {
			later := r.groups[b]
			e := keyOrder(r.parts[later.part].name, later.physical, 0, next)
			e.Detail = "a row group's first key breaks the declared key order with the row group before it"
			return e
		}
		return nil
	}
	if g > 0 {
		if prev := r.groups[g-1].first.Load(); prev != nil {
			if err := check(g-1, g, *prev, key); err != nil {
				return err
			}
		}
	}
	if g+1 < len(r.groups) {
		if next := r.groups[g+1].first.Load(); next != nil {
			if err := check(g, g+1, key, *next); err != nil {
				return err
			}
		}
	}
	if err := r.reserveIndex(int64(len(key))); err != nil {
		return err
	}
	k := bytes.Clone(key)
	r.groups[g].first.Store(&k)
	return nil
}

// reserveIndex charges retained index data before publication. The caller
// holds indexMu, so a duplicate load must check for publication first.
func (r *reader) reserveIndex(size int64) error {
	used := r.indexMemory.Load()
	if size > r.limits.IndexMemory-used {
		return exceeded("key page indexes and first keys need more than %d bytes (Limits.IndexMemory)", r.limits.IndexMemory)
	}
	r.indexMemory.Store(used + size)
	return nil
}

// groupFor returns the row group in which a read of the keys at or after
// from (after it unless inclusive) begins: the predecessor of the first
// group whose first key is above from, or at it when the read includes
// from and keys repeat, clamped to the first group of the part that can
// hold such a key; len(groups) when no group can. Parts are chosen by
// their bounds when every part has them; row groups by binary search over
// their first keys, decoding a group's first key page the first time a
// search needs it.
func (r *reader) groupFor(ctx context.Context, from []byte, inclusive bool, log *pageLog, st *stats) (int, error) {
	if from == nil || len(r.groups) == 0 {
		return 0, nil
	}
	lo, hi := 0, len(r.groups)
	if r.bounded {
		i := sort.Search(len(r.parts), func(i int) bool {
			cmp := bytes.Compare(r.parts[i].maxKey, from)
			return cmp > 0 || cmp == 0 && inclusive
		})
		if i == len(r.parts) {
			return len(r.groups), nil
		}
		lo, hi = r.parts[i].firstGroup, r.parts[i].endGroup
		if cmp := bytes.Compare(from, r.parts[i].minKey); cmp < 0 || cmp == 0 && inclusive {
			return lo, nil
		}
	}
	// When keys repeat and the read includes from, search for the first
	// first key at or above from: its predecessor can hold earlier rows of
	// the same key (ROUTE-1). Otherwise a group whose first key is from
	// holds the first row the read can return, or precedes it.
	a, b := lo+1, hi
	for a < b {
		mid := int(uint(a+b) >> 1)
		key, err := r.first(ctx, mid, log, st)
		if err != nil {
			return 0, err
		}
		if cmp := bytes.Compare(key, from); cmp > 0 || cmp == 0 && inclusive && !r.key.Unique {
			b = mid
		} else {
			a = mid + 1
		}
	}
	return a - 1, nil
}

// groupAt returns the row group holding the dataset row ordinal, or
// len(groups) when the dataset has fewer rows.
func (r *reader) groupAt(ordinal int64) int {
	return sort.Search(len(r.groups), func(i int) bool { return r.groups[i].start+r.groups[i].rows > ordinal })
}

// startsAtOrAfter reports whether row group g is known, without reading
// it, to begin at or after key: its first key, or its part's minimum key
// when it is the part's first group, is at or after key.
func (r *reader) startsAtOrAfter(g int, key []byte) bool {
	if first := r.groups[g].first.Load(); first != nil {
		return bytes.Compare(*first, key) >= 0
	}
	p := r.parts[r.groups[g].part]
	return g == p.firstGroup && p.minKey != nil && bytes.Compare(p.minKey, key) >= 0
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
// the source's own.
func (p *part) at(ctx context.Context) io.ReaderAt { return storageReader{bind(ctx, p.file)} }

// begin takes a read lease that close waits for, and returns the request's
// context: it ends with ctx, or with close (cause ErrClosed).
func (r *reader) begin(ctx context.Context) (context.Context, func(), error) {
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

// finish maps an error of a request ended by close to ErrClosed.
func finish(ctx, rctx context.Context, err error) error {
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(rctx), ErrClosed) && !errors.Is(err, ErrClosed) {
		return fmt.Errorf("%w (%w)", ErrClosed, err)
	}
	return err
}

// close ends the reads in progress as soon as they reach a cancellation
// point, waits for them to return, closes every part file that implements
// io.Closer, and fails reads after it with ErrClosed. Later calls return
// the first call's result.
func (r *reader) close() error {
	r.closeOnce.Do(func() {
		r.closeNow(ErrClosed)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closed = true
		var errs []error
		for _, p := range r.parts {
			if err := closeFile(p.file); err != nil {
				errs = append(errs, fmt.Errorf("close %s: %w", p.name, err))
			}
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
