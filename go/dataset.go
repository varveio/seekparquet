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
// A file that does not implement ContextBinder is read as a plain
// io.ReaderAt: the reader checks its context between pages, but cannot
// interrupt a ReadAt call that is already blocked.
type ContextBinder interface {
	WithContext(ctx context.Context) io.ReaderAt
}

// SourceFile is one opened file of a dataset root: its manifest or a part.
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
type Source interface {
	Open(ctx context.Context, path string) (SourceFile, error)
}

// Options configures OpenDataset.
type Options struct {
	// Limits bounds the resources the dataset can make the reader use.
	Limits Limits
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
// checked before it (COL-5). A part that fails is refused by the Range or
// List that touched it, and by every later read that touches it. Verify
// checks every part at once. Rows a read does not decode are not certified
// (CHECK-4).
//
// A dataset whose manifest breaks the specification is refused with a
// *RefusalError (see CodeOf). Other errors report I/O, a limit or the
// context. The reader uses signed 64-bit dataset ordinals: a total row
// count above 2^63-1 fails with ErrLimitExceeded, even when each part's
// count fits.
//
// Dataset.Close closes every part file the dataset opened.
func OpenDataset(ctx context.Context, src Source, opt Options) (*Dataset, error) {
	opt.Limits = opt.Limits.withDefaults()
	raw, err := readManifest(ctx, src, opt.Limits.ManifestBytes)
	if err != nil {
		return nil, err
	}
	m, err := parseManifest(raw, &opt.Limits)
	if err != nil {
		return nil, err
	}
	meta := Metadata{Key: Key{Column: m.KeyColumn, Unique: m.KeyUnique}, Version: Version(m.Version), SchemaID: m.SchemaID,
		Extensions: m.Extensions, Manifest: raw}
	inputs := make([]partInput, 0, len(m.Parts))
	for _, entry := range m.Parts {
		meta.Parts = append(meta.Parts, PartInfo(entry))
		inputs = append(inputs, partInput{name: entry.Path, minKey: entry.MinKey, maxKey: entry.MaxKey, rows: entry.Rows})
	}
	r, err := openParts(ctx, inputs, meta.Key, opt)
	if err != nil {
		return nil, err
	}
	r.fetch = func(ctx context.Context, i int, st *ReadStats) (partInput, error) {
		return openDatasetPart(ctx, src, m, m.Parts[i], &r.limits, st)
	}
	return &Dataset{meta: meta, r: r}, nil
}

func readManifest(ctx context.Context, src Source, limit int64) ([]byte, error) {
	f, err := src.Open(ctx, ManifestName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, refuse(CodeNoManifest, "the dataset root has no %s", ManifestName)
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	size := f.Size()
	if size > limit {
		return nil, exceeded("%s is %d bytes (Limits.ManifestBytes %d)", ManifestName, size, limit)
	}
	raw := make([]byte, size)
	if err := readFull(bind(ctx, f), raw, 0); err != nil {
		return nil, fmt.Errorf("read %s: %w", ManifestName, err)
	}
	return raw, nil
}

// openDatasetPart opens one part, checks its size, footer, stamps and row
// count against its manifest entry, and returns it with its footer. It
// closes the file when it fails.
func openDatasetPart(ctx context.Context, src Source, m *manifest.Manifest, entry manifest.Part, lim *Limits, st *ReadStats) (_ partInput, err error) {
	f, err := src.Open(ctx, entry.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return partInput{}, refusePart(CodeMissingPart, entry.Path, -1, "%v", err)
		}
		return partInput{}, err
	}
	st.PartsOpened++
	defer func() {
		if err != nil {
			_ = f.Close()
		}
	}()
	in := partInput{name: entry.Path, file: f, size: f.Size(), minKey: entry.MinKey, maxKey: entry.MaxKey, rows: entry.Rows}
	if in.size != entry.Bytes {
		return partInput{}, refusePart(CodePartMismatch, entry.Path, -1, "the file is %d bytes, its manifest entry %d", in.size, entry.Bytes)
	}
	footer, err := readFooter(ctx, entry.Path, f, in.size, lim, st)
	if err != nil {
		return partInput{}, err
	}
	if err := checkStamps(entry.Path, footer, m); err != nil {
		return partInput{}, err
	}
	if footer.NumRows() != entry.Rows {
		return partInput{}, refusePart(CodePartMismatch, entry.Path, -1, "the part holds %d rows, its manifest entry %d", footer.NumRows(), entry.Rows)
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
func (d *Dataset) Verify(ctx context.Context) (err error) {
	rctx, end, err := d.r.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	return finish(ctx, rctx, d.r.verify(rctx))
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
	rctx, end, err := d.r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	p, err := d.r.part(rctx, i, new(ReadStats))
	if err != nil {
		return nil, finish(ctx, rctx, err)
	}
	return columnsOf(p.footer.Metadata().Schema, d.r.key.Column), nil
}

// Range returns the rows of rng. It decodes key pages only from the page
// that can hold its start, or the cursor's row, and value pages only for
// the rows it returns. A limit that is not positive, or a malformed or
// prefix cursor, is ErrInvalidRequest; projecting a nested or repeated
// column is an *UnsupportedError; a page that breaks the specification
// ends the read with a *RefusalError.
func (d *Dataset) Range(ctx context.Context, rng Range) ([]Row, error) {
	return d.r.readRange(ctx, rng, rng.Stats)
}

// List returns the entries of req. After a common prefix it continues at
// the prefix's successor through the routing index and the page index
// instead of decoding the keys under it, and a row group whose first key
// and the next group's first key share a common prefix is skipped without
// a read. Keys under a prefix that share a page with the keys around them
// are decoded. Errors are those of Range.
func (d *Dataset) List(ctx context.Context, req List) ([]Entry, error) {
	return d.r.readList(ctx, req, req.Stats)
}

// Close ends the reads in progress as soon as they reach a cancellation
// point (a context-bound source read, or a page boundary), waits for them
// to return, closes every part file, and fails reads after it with
// ErrClosed. It is safe to call more than once; later calls return the
// first call's result.
func (d *Dataset) Close() error { return d.r.close() }
