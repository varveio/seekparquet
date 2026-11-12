// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/manifest"
)

// Version is a (format_version, draft_revision) pair. DraftRevision is 0
// for a final format version, which has none.
type Version struct {
	FormatVersion int64
	DraftRevision int64
}

// SupportedVersions returns the version pairs this package reads (VER-1):
// format_version 0 with draft_revision 1. Every other pair is refused with
// unsupported_version.
func SupportedVersions() []Version {
	out := make([]Version, len(manifest.Supported))
	for i, v := range manifest.Supported {
		out[i] = Version(v)
	}
	return out
}

// ContextBinder is implemented by a file whose reads can be bounded by a
// context. The reader calls WithContext once per operation (reading a
// footer, loading a page index, reading pages) and reads through the
// returned io.ReaderAt, so that a cancelled request, or Close, can end a
// read that waits on the source (for a connection, a retry).
//
// WithContext may be called concurrently, for operations that run at the
// same time. Each call must return an independent view bound to its own
// context, and must not change shared state such as a "current" context
// of the file.
//
// The view's reads must carry ctx's values, the operation's cache mode
// (WithCacheMode) and observer (WithIOObserver) among them, to the source.
//
// A file that does not implement ContextBinder is read as a plain
// io.ReaderAt: the reader checks its context between pages, but cannot
// interrupt a ReadAt call that is already blocked.
type ContextBinder interface {
	WithContext(ctx context.Context) io.ReaderAt
}

// SourceFile is one opened file of a dataset root: its manifest or a part.
//
// SourceFile.Size is stable for the handle's lifetime. Reads must never
// successfully return bytes from different versions. Dataset cancels and
// joins its reads before calling Close. Close need not interrupt
// concurrent reads.
type SourceFile interface {
	io.ReaderAt
	// Size returns the file's length in bytes.
	Size() int64
	Close() error
}

// Source is a dataset root: it opens files by their slash-separated paths
// relative to the root. OpenDataset opens the root's manifest.json; the
// Dataset opens each part path the manifest lists the first time a read,
// or Verify, touches the part, after checking every path against PART-1
// on open, so a Source is never asked for an absolute path or one with a
// "." or ".." segment; it must still confine reads to the root. The Source
// must stay usable until the Dataset is closed.
//
// Open returns an error that wraps fs.ErrNotExist when the file does not
// exist, and should honour ctx. The returned file must be safe for
// concurrent ReadAt calls and must read the same bytes until it is closed:
// a remote source binds it to one stored object (one version or
// generation) across all its range requests and retries. It may implement
// ContextBinder, so that every read of it is bounded by the operation's
// context.
//
// Source implementations must be safe for concurrent calls (Verify opens
// parts concurrently). Open establishes the actual size and an immutable
// file view. Its context governs opening, not the returned handle's
// lifetime. The caller owns Source; Dataset closes opened files but never
// Source.
type Source interface {
	Open(ctx context.Context, path string) (SourceFile, error)
}

// OpenOptions configures OpenDataset.
type OpenOptions struct {
	// Limits bounds the resources the dataset can make the reader use.
	Limits Limits
	// Read controls how the reader schedules its reads.
	Read ReadPolicy
	// OpenStats, when non-nil, is reset on entry to OpenDataset or
	// OpenDir and filled on every exit. It includes manifest I/O.
	// The Dataset does not retain it.
	OpenStats *ReadStats
}

// Key declares the exact key column and whether keys must be unique.
type Key struct {
	Column string
	Unique bool
}

// Metadata is a read-only view of a dataset's manifest. Metadata returns a
// copy each time, so a caller may keep and change it freely.
type Metadata struct {
	Key      Key
	Version  Version
	SchemaID string
	Parts    []PartInfo
	// Extensions is the manifest's extensions object exactly as written
	// (EXT-2), nil when the manifest has none; an empty object is "{}".
	Extensions json.RawMessage
	// Manifest is the manifest exactly as read.
	Manifest []byte
	// ManifestIdentity is the opaque identity a ManifestReader gave the
	// manifest's bytes, "" when the Source has none. It is diagnostic: it
	// grants no authority over the parts.
	ManifestIdentity string
}

// PartInfo is one part's manifest entry.
type PartInfo struct {
	Path   string
	Rows   int64
	Bytes  int64
	MinKey []byte
	MaxKey []byte
	// SHA256 is the declared lowercase hex digest, "" when none. Neither
	// opening nor Verify checks it (PART-5).
	SHA256 string
}

// Extension returns the value of one extensions namespace exactly as
// written, and whether the manifest has it.
func (m Metadata) Extension(namespace string) (json.RawMessage, bool) {
	return extension(m.Extensions, namespace)
}

func (m Metadata) clone() Metadata {
	out := m
	out.Parts = make([]PartInfo, len(m.Parts))
	for i, p := range m.Parts {
		p.MinKey, p.MaxKey = bytes.Clone(p.MinKey), bytes.Clone(p.MaxKey)
		out.Parts[i] = p
	}
	out.Extensions = bytes.Clone(m.Extensions)
	out.Manifest = bytes.Clone(m.Manifest)
	return out
}

// Dataset is an opened SeekParquet dataset: its manifest's metadata, and
// bounded, ordered reads over its parts. It is safe for concurrent use:
// part checks, footers and page indexes are shared, and immutable once
// loaded.
type Dataset struct {
	meta Metadata
	r    *reader
}

// OpenDataset opens the dataset at src (CHECK-1). It reads and validates
// the manifest (§3), and nothing else: no part is opened. The manifest
// carries each part's exact key bounds, row count and size, so reads are
// routed to a part by its bounds, and within a part by row groups' first
// keys, decoded when a read first needs them.
//
// A part is checked the first time a read routes to it: it must exist and
// have the manifest's size; its footer must be readable, self-contained and
// unencrypted (FILE-1, FILE-2), with the seekparquet.* stamps agreeing with
// the manifest (STAMP-2), the manifest's row count, a valid key column
// (KEY-1, KEY-5), the key column ascending as the first sorting column of
// every non-empty row group (RG-1), and columns consistent with every part
// checked before it (COL-5). A part that fails is refused by the read that
// touched it, and by every later read that touches it. Verify
// checks every part at once. Rows a read does not decode are not certified
// (CHECK-4).
//
// A dataset whose manifest breaks the specification is refused with a
// *RefusalError (see RefusalCodeOf). Other errors report I/O, a limit or the
// context. The reader uses signed 64-bit dataset ordinals: a total row
// count above 2^63-1 fails with ErrLimitExceeded, even when each part's
// count fits.
//
// A negative field of opt.Limits is ErrInvalidRequest.
//
// Dataset.Close closes every part file the dataset opened.
func OpenDataset(ctx context.Context, src Source, opt OpenOptions) (*Dataset, error) {
	st, spare := takeStats(opt.OpenStats)
	defer dropStats(spare)
	if err := prepareOpen(ctx, &opt); err != nil {
		return nil, err
	}
	return openDataset(ctx, src, opt, st, false)
}

// prepareOpen checks opt and the context's cache mode, before any I/O,
// and sets opt's defaults.
func prepareOpen(ctx context.Context, opt *OpenOptions) error {
	if err := opt.Limits.check(); err != nil {
		return err
	}
	if err := opt.Read.check(); err != nil {
		return err
	}
	if err := checkCacheMode(ctx); err != nil {
		return err
	}
	opt.Limits, opt.Read = opt.Limits.withDefaults(), opt.Read.withDefaults()
	return nil
}

// openDataset opens the dataset at src with checked options; local reads
// as OpenDir's zero policy says (reader.local).
func openDataset(ctx context.Context, src Source, opt OpenOptions, st *ReadStats, local bool) (*Dataset, error) {
	// The manifest is read under an operation that observes the physical
	// requests of whichever of the Source and the manifest's file report
	// them; a local directory's files report none.
	mctx := ctx
	if _, ok := src.(dirSource); !ok {
		op := newOperation(ctx, opt.Read.Concurrency)
		mctx = op
		defer func() {
			if c := op.c.Load(); c != nil {
				c.addTo(st)
			}
		}()
	}
	raw, identity, err := readManifest(mctx, src, opt.Limits.ManifestBytes, reportsIO(src), st)
	if err != nil {
		return nil, err
	}
	m, err := parseManifest(raw, &opt.Limits)
	if err != nil {
		return nil, err
	}
	meta := Metadata{Key: Key{Column: m.KeyColumn, Unique: m.KeyUnique}, Version: Version(m.Version), SchemaID: m.SchemaID,
		Extensions: m.Extensions, Manifest: raw, ManifestIdentity: identity}
	inputs := make([]partInput, 0, len(m.Parts))
	if len(m.Parts) > 0 {
		meta.Parts = make([]PartInfo, 0, len(m.Parts))
	}
	for _, entry := range m.Parts {
		meta.Parts = append(meta.Parts, PartInfo(entry))
		inputs = append(inputs, partInput{name: entry.Path, minKey: entry.MinKey, maxKey: entry.MaxKey, rows: entry.Rows})
	}
	r, err := openParts(ctx, inputs, meta.Key, opt)
	if err != nil {
		return nil, err
	}
	r.local = local
	r.fetch = func(ctx context.Context, i int, st *ReadStats, deferHead bool) (fetchedInput, error) {
		return openDatasetPart(ctx, src, m, m.Parts[i], r, st, deferHead)
	}
	return &Dataset{meta: meta, r: r}, nil
}

// readManifest reads the manifest whole within limit: through
// ManifestReader when the Source has it, otherwise by opening it and
// reading it through ReadAt.
func readManifest(ctx context.Context, src Source, limit int64, reports bool, st *ReadStats) ([]byte, string, error) {
	if !reports {
		st.UnreportedIOCalls++
	}
	if mr, ok := src.(ManifestReader); ok {
		raw, identity, err := mr.ReadManifest(ctx, limit)
		switch {
		case errors.Is(err, ErrObjectChanged): // a broken pin, whatever else it wraps
			return nil, "", fmt.Errorf("read %s: %w", ManifestName, err)
		case errors.Is(err, ErrObjectTooLarge):
			return nil, "", exceeded(LimitManifestBytes, -1, limit, "%s", ManifestName)
		case err != nil && errors.Is(err, fs.ErrNotExist):
			return nil, "", refuse(CodeNoManifest, "the dataset root has no %s", ManifestName)
		case err != nil:
			return nil, "", fmt.Errorf("read %s: %w", ManifestName, err)
		case int64(len(raw)) > limit:
			return nil, "", exceeded(LimitManifestBytes, int64(len(raw)), limit, "%s", ManifestName)
		}
		return raw, identity, nil
	}
	f, err := src.Open(ctx, ManifestName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) && !errors.Is(err, ErrObjectChanged) {
			return nil, "", refuse(CodeNoManifest, "the dataset root has no %s", ManifestName)
		}
		return nil, "", err
	}
	defer func() { _ = f.Close() }()
	size := f.Size()
	if size > limit {
		return nil, "", exceeded(LimitManifestBytes, size, limit, "%s", ManifestName)
	}
	if size < 0 {
		return nil, "", fmt.Errorf("read %s: the source gives a negative size %d", ManifestName, size)
	}
	if uint64(size) > uint64(math.MaxInt) {
		return nil, "", exceeded(LimitManifestBytes, size, int64(math.MaxInt), "%s: beyond a slice's length", ManifestName)
	}
	raw := make([]byte, size)
	// Manifest reads are not part reads: they count in no Source counter.
	if unreported(ctx, reportsIO(f)) {
		st.UnreportedIOCalls++
	}
	at := storageReader{r: bind(ctx, f), op: operationOf(ctx), name: ManifestName}
	if err := at.readFull(raw, 0); err != nil {
		return nil, "", fmt.Errorf("read %s: %w", ManifestName, err)
	}
	return raw, "", nil
}

// openPartFile opens a part's file: with its manifest size through
// SizedSource when the Source has it, the size then proven by the first
// read (sourceError), and otherwise through Open, its size compared with
// the manifest's.
func openPartFile(ctx context.Context, src Source, entry manifest.Part, reports bool, st *ReadStats) (SourceFile, error) {
	if !reports {
		st.UnreportedIOCalls++
	}
	var f SourceFile
	var err error
	// A part of fewer than 12 bytes is refused before any read could prove
	// its size: it is opened by Open, which establishes it, so that a
	// missing or resized object is refused as such.
	if sized, ok := src.(SizedSource); ok && entry.Bytes >= 12 {
		f, err = sized.OpenSized(ctx, entry.Path, entry.Bytes)
	} else {
		f, err = src.Open(ctx, entry.Path)
	}
	if err != nil {
		var mismatch *SizeMismatchError
		switch {
		case errors.Is(err, ErrObjectChanged):
		case errors.As(err, &mismatch), errors.Is(err, fs.ErrNotExist):
			return nil, sourceError(entry.Path, err)
		}
		return nil, err
	}
	return f, nil
}

// openDatasetPart opens one part, checks its size, footer, stamps and row
// count against its manifest entry, and returns it with its footer. It
// closes the file when it fails. With deferHead the part's head is not
// read with its end when the window does not reach it (readFooter): the
// caller establishes it (fetchedInput.tail.headDeferred), and a verdict
// reached before is judged again with the real head (magicFirst).
func openDatasetPart(ctx context.Context, src Source, m *manifest.Manifest, entry manifest.Part, r *reader, st *ReadStats, deferHead bool) (_ fetchedInput, err error) {
	f, err := openPartFile(ctx, src, entry, reportsIO(src), st)
	if err != nil {
		return fetchedInput{}, err
	}
	st.PartsOpened++
	in := fetchedInput{partInput: partInput{name: entry.Path, file: f, size: f.Size(), minKey: entry.MinKey, maxKey: entry.MaxKey, rows: entry.Rows}}
	defer func() {
		if err != nil {
			if in.tail.headDeferred {
				err = magicFirst(ctx, entry.Path, f, in.size, in.tail, st, err)
			}
			_ = f.Close()
		}
	}()
	if in.size != entry.Bytes {
		return fetchedInput{}, refusePart(CodePartMismatch, entry.Path, -1, "the file is %d bytes, its manifest entry %d", in.size, entry.Bytes)
	}
	footer, tail, err := readFooter(ctx, entry.Path, f, in.size, &r.limits, st, r.policy.tailWindow(), r.width(), deferHead)
	in.tail = tail
	if err != nil {
		return fetchedInput{}, err
	}
	if err := checkStamps(entry.Path, footer, m); err != nil {
		return fetchedInput{}, err
	}
	if footer.NumRows() != entry.Rows {
		return fetchedInput{}, refusePart(CodePartMismatch, entry.Path, -1, "the part holds %d rows, its manifest entry %d", footer.NumRows(), entry.Rows)
	}
	in.footer = footer
	return in, nil
}

// checkStamps applies STAMP-1 and STAMP-2: each stamp exactly once, no
// draft_revision stamp unless format_version is 0, values compared as exact
// strings with the manifest's canonical decimal and schema_id.
func checkStamps(name string, footer *parquet.File, m *manifest.Manifest) error {
	want := []string{
		strconv.FormatInt(m.Version.FormatVersion, 10),
		strconv.FormatInt(m.Version.DraftRevision, 10),
		m.SchemaID,
		m.KeyColumn,
		strconv.FormatBool(m.KeyUnique),
	}
	got := make([][]string, len(stampKeys))
	for _, kv := range footer.Metadata().KeyValueMetadata {
		for i, key := range stampKeys {
			if kv.Key == key {
				got[i] = append(got[i], kv.Value)
			}
		}
	}
	for i, key := range stampKeys {
		values := got[i]
		required := i != 1 || m.Version.FormatVersion == 0 // draft_revision only on a draft
		switch {
		case !required && len(values) > 0:
			return refusePart(CodeStampMismatch, name, -1, "%s is present on a final format version", key)
		case !required:
		case len(values) == 0:
			return refusePart(CodeStampMismatch, name, -1, "the footer has no %s", key)
		case len(values) > 1:
			return refusePart(CodeStampMismatch, name, -1, "the footer has %s %d times", key, len(values))
		case values[0] != want[i]:
			return refusePart(CodeStampMismatch, name, -1, "%s is %q, the manifest's %q", key, values[0], want[i])
		}
	}
	return nil
}

// checkSorted applies RG-1: a non-empty row group declares the key column
// ascending as its first sorting column.
func checkSorted(p *part, physical int, group *format.RowGroup) error {
	sorting := group.SortingColumns
	if len(sorting) == 0 || int(sorting[0].ColumnIdx) != p.key.index || sorting[0].Descending {
		return refusePart(CodeMissingSortingColumns, p.name, physical, "the row group does not declare %s ascending as its first sorting column", p.r.key.Column)
	}
	return nil
}

// Metadata returns a copy of the dataset's manifest metadata.
func (d *Dataset) Metadata() Metadata { return d.meta.clone() }

// Verify checks every part now, as the first read to touch each would
// (CHECK-1), then checks every non-empty row group's key page index and
// decodes its first key page, and decodes each part's last key page
// (CHECK-3): row-group first keys must be in the declared order and each
// part's first and last keys equal its manifest bounds. It reads one
// footer per part not yet checked and a few pages per row group, and suits
// a long-lived reader of local files, or a caller that wants every part's
// fault before its first query. A part it refuses stays refused for later
// reads. It does not read every row (CHECK-4) or check digests (PART-5).
//
// Parts are checked, and the pages Verify decodes fetched, by up to
// ReadPolicy.Concurrency parts at once: a part's end first, then its row
// groups' first key pages and its last key page together, the part's head
// read with its first page when that page begins near it. The checks and
// decodes themselves run in manifest and row-group order, so the fault
// Verify reports is the first in that order, whichever read ends first. A
// row group's first key and a part's last key page, once checked, are not
// checked again by a later Verify of the same Dataset.
//
// When st is not nil, Verify resets it on entry and fills it on every
// exit, including failure; Verify does not retain it.
func (d *Dataset) Verify(ctx context.Context, st *ReadStats) (err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	rctx, err := d.r.begin(ctx)
	if err != nil {
		return err
	}
	defer rctx.end(st)
	return finish(ctx, rctx, d.r.verify(rctx, st))
}

// Columns returns the top-level columns of part i, an index into
// Metadata().Parts, in schema order, from the footer the dataset checked
// and holds for that part: if no read has checked the part yet, Columns
// checks it as the first read to touch it would (CHECK-1), so the answer
// always comes from a checked part, and every later read of the part reads
// that same file. COL-5 holds a column's physical type, repetition and
// nesting equal in every checked part that has it; a part may lack a
// column (COL-2), and logical annotations are not compared, so a caller
// that needs a column in every part asks for each part, after Verify or
// not. An index outside the parts is ErrInvalidRequest.
func (d *Dataset) Columns(ctx context.Context, i int) ([]Column, error) {
	if i < 0 || i >= len(d.r.parts) {
		return nil, invalidRequest("part %d of %d", i, len(d.r.parts))
	}
	rctx, err := d.r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rctx.end(nil)
	p, err := d.r.part(rctx, i, new(ReadStats))
	if err != nil {
		return nil, finish(ctx, rctx, err)
	}
	return columnsOf(p.footer.Metadata().Schema, d.r.key.Column), nil
}

// Rows returns the rows of q, owning their keys and values. q.Limit must be
// positive and fit a slice's length. It decodes key pages only from the
// page that can hold its start, or the cursor's row, and finds every
// selected row's key before it reads any value page, then value pages only
// for the rows it returns. A limit that is not positive, or a malformed or
// prefix cursor, is ErrInvalidRequest; projecting a nested or repeated
// column is an *UnsupportedError; a page that breaks the specification ends
// the read with a *RefusalError. On an error it returns no rows. st, when
// not nil, is reset on entry and receives the read statistics, including on
// error; do not share it between concurrent calls.
func (d *Dataset) Rows(ctx context.Context, q Query, st *ReadStats) ([]Row, error) {
	return d.r.readRange(ctx, q, st)
}

// Get returns every row of key's group of equal keys, owning their keys and
// the values of columns: none when no row has key, at most one when keys
// are unique. It finds the group's keys before it reads any value page,
// and a group of more than Limits.GetRows rows fails, with no rows and
// before any value page is read, with a *LimitError naming GetRows: read
// such a group with Rows over KeySpan(key), page by page, continuing after
// each page's last row. The limit bounds rows, not bytes. Errors are
// otherwise those of Rows; st is as Rows' st.
func (d *Dataset) Get(ctx context.Context, key []byte, st *ReadStats, columns ...string) ([]Row, error) {
	return d.r.readGet(ctx, key, st, columns)
}

// Scan visits q's rows in order, in serial callbacks, as borrowed batches:
// the rows Rows returns. Bounds, Cursor and Columns mean exactly what they
// mean to Rows, and its errors are Rows'; a nil fn is ErrInvalidRequest
// too. A positive Limit stops it after that many rows without advancing to
// the next row; a zero Limit reads to the end of the span, a streaming
// extension of this library (the specification's range has a positive
// limit, QUERY-1); a negative Limit is ErrInvalidRequest. It opens the
// projected chunks of each row group as it reaches it, with no plan: it
// does not position at the upper bound or read the OffsetIndexes of the
// rest of the span first, and, as Rows, reads a dictionary only for a page
// that uses it. Decoding and checking still cover the whole pages holding
// the rows it returns.
//
// Batch documents the boundaries (never crossing a row group, a key page or
// a projected page); the last batch ends at the limit or the upper bound.
// The batch, its keys, column views and bytes are read-only and borrowed
// until fn returns; Batch.Row and Batch.Cursor make owning copies, the rows
// and cursors Rows would return. A span with no rows calls no fn. st, when
// not nil, is reset on entry and records the reads made, including on
// error; do not share it between concurrent calls.
//
// An error fn returns stops the scan and is returned unchanged, unless Close
// ended the scan, when ErrClosed wraps it as it wraps every request's error;
// Stop stops it with a nil error. Read errors and refusals end the scan;
// batches already handed over stand. Cancellation is checked as Rows checks
// it, before each part, key page and value page read, and also before every
// callback; a scan that reaches its limit or its end returns nil. Close
// cancels the scan and waits for fn to return: fn must not call Close, nor
// wait for a goroutine that does. Concurrent and reentrant calls have
// independent scratch. It asks a Source's byte cache to be bypassed
// (CacheBypass), unless ctx sets a cache mode.
func (d *Dataset) Scan(ctx context.Context, q Query, st *ReadStats, fn func(*Batch) error) error {
	return d.r.rangeBatches(ctx, q, st, keyReuse{}, fn)
}

// List returns the entries of q. After a common prefix it continues at
// the prefix's successor through the routing index and the page index
// instead of decoding the keys under it, and a row group whose first key
// and the next group's first key share a common prefix is skipped without
// decoding its keys; when that first key is the prefix's exact row
// (ListEntry.Exact), only the row's projected value pages are read. Keys
// under a prefix that share a page with the keys around them are decoded.
// Value pages are read only for row entries and exact rows. BuildListIndex
// optionally replaces slash-prefix resumes with exact directory-run
// anchors, usually reducing a wide listing to one key-page decode. Errors
// are those of Rows. st, when not nil, is reset on entry and receives the
// read statistics, including on error.
func (d *Dataset) List(ctx context.Context, q ListQuery, st *ReadStats) ([]ListEntry, error) {
	return d.r.readList(ctx, q, st)
}

// Close ends the reads in progress as soon as they reach a cancellation
// point (a context-bound source read, or a page boundary), waits for them
// to return, closes every part file, and fails reads after it with
// ErrClosed. It is safe to call more than once; later calls return the
// first call's result.
func (d *Dataset) Close() error { return d.r.close() }
