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

// ManifestName is the name of the manifest file in a dataset root.
const ManifestName = "manifest.json"

// Footer stamps of a part (STAMP-1).
const (
	StampFormatVersion = "seekparquet.format_version"
	StampDraftRevision = "seekparquet.draft_revision"
	StampSchemaID      = "seekparquet.schema_id"
	StampKeyColumn     = "seekparquet.key_column"
	StampKeyUnique     = "seekparquet.key_unique"
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

// SourceFile is one opened file of a dataset root: its manifest or a part.
type SourceFile interface {
	io.ReaderAt
	// Size returns the file's length in bytes.
	Size() int64
	Close() error
}

// Source is a dataset root: it opens files by their slash-separated paths
// relative to the root. OpenDataset opens ManifestName and each part path
// the manifest lists, after checking that path against PART-1, so a Source
// is never asked for an absolute path or one with a "." or ".." segment;
// it must still confine reads to the root.
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

// Options configures OpenDataset, and OpenParts through PartsOptions.
type Options struct {
	// Expect lists column expectations, checked in every part on open.
	Expect []ColumnExpectation
	// Limits bounds the resources the dataset can make the reader use.
	Limits Limits
	// VerifyOnOpen makes OpenDataset check every non-empty row group's key
	// page index and decode its first key page, and decode each part's
	// last key page, before it returns (CHECK-3): row-group first keys must
	// then ascend and each part's first and last keys equal its manifest
	// bounds. It suits a long-lived reader of local files. Without it,
	// opening reads only the manifest and the footers (CHECK-1), and the
	// rest is loaded, and checked, when a read first needs it.
	VerifyOnOpen bool
}

// Dataset is an opened SeekParquet dataset: its manifest's metadata and a
// Reader over its parts. It is safe for concurrent use.
type Dataset struct {
	reader *Reader
	meta   Metadata
}

// Metadata is a read-only view of a dataset's manifest. Metadata returns a
// copy each time, so a caller may keep and change it freely.
// Key declares the exact key column and whether keys must be unique.
type Key struct {
	Column string
	Unique bool
}

// Metadata is a copy of a dataset's manifest metadata.
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
	// SHA256 is the declared lowercase hex digest, "" when none. Opening
	// does not check it (PART-5).
	SHA256 string
}

// Extension returns the value of one extensions namespace exactly as
// written, and whether the manifest has it.
func (m Metadata) Extension(namespace string) (json.RawMessage, bool) {
	if m.Extensions == nil {
		return nil, false
	}
	var ext map[string]json.RawMessage
	if err := json.Unmarshal(m.Extensions, &ext); err != nil {
		return nil, false
	}
	v, ok := ext[namespace]
	return v, ok
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

// OpenDataset opens the dataset at src (CHECK-1). It reads and validates
// the manifest (§3); opens every part, requiring it to exist and have the
// manifest's size; reads every footer, requiring it to be self-contained
// and unencrypted (FILE-1, FILE-2), the seekparquet.* stamps to agree with
// the manifest (STAMP-2), the row count to equal the manifest's, a valid
// key column (KEY-1, KEY-5), consistent columns across parts (COL-5), and
// key ascending as the first sorting column of every non-empty row group
// (RG-1). Beyond CHECK-1 (as CHECK-3 allows) it checks every non-empty row
// (RG-1). It reads the manifest and every part's footer, and nothing more
// unless Options.VerifyOnOpen asks for the first and last key pages too.
// Reads are routed to a part by its manifest bounds, and within a part by
// row groups' first keys, decoded when a read first needs them. Opening
// does not read every row: rows a read does not decode are not certified
// (CHECK-4).
//
// A dataset that breaks the specification is refused with a
// *RefusalError (see CodeOf); a part that fails an expectation, with an
// *ExpectationError. Other errors report I/O or the context.
//
// OpenDataset closes every file it opened when it fails; on success,
// Dataset.Close closes them. It never closes src.
func OpenDataset(ctx context.Context, src Source, opt Options) (*Dataset, error) {
	lim := opt.Limits.withDefaults()
	raw, err := readManifest(ctx, src, lim.ManifestBytes)
	if err != nil {
		return nil, err
	}
	m, err := manifest.Parse(raw, lim.Parts)
	if err != nil {
		var e *manifest.Error
		switch {
		case errors.As(err, &e):
			return nil, refuse(Code(e.Code), "%s: %s", ManifestName, e.Detail)
		case errors.Is(err, manifest.ErrTooManyParts):
			return nil, exceeded("%s lists more than %d parts (Limits.Parts)", ManifestName, lim.Parts)
		}
		return nil, err
	}
	meta := Metadata{Version: Version(m.Version), SchemaID: m.SchemaID, Key: Key{Column: m.KeyColumn, Unique: m.KeyUnique}, Extensions: m.Extensions, Manifest: raw}
	for _, p := range m.Parts {
		meta.Parts = append(meta.Parts, PartInfo(p))
	}

	inputs := make([]partInput, 0, len(m.Parts))
	handed := false
	defer func() {
		if !handed {
			closeSources(inputs)
		}
	}()
	for _, entry := range m.Parts {
		in, err := openDatasetPart(ctx, src, m, entry, &lim)
		if in.File != nil {
			inputs = append(inputs, in)
		}
		if err != nil {
			return nil, err
		}
	}
	handed = true
	opt.Limits = lim
	reader, err := openParts(ctx, inputs, PartsOptions{Options: opt, Key: &meta.Key, CheckGroup: func(name string, footer *parquet.File, physical int, group *format.RowGroup) error {
		return checkSortedColumn(name, footer, physical, group, m.KeyColumn)
	}})
	if err != nil {
		return nil, err
	}
	return &Dataset{reader: reader, meta: meta}, nil
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
	n, err := bind(ctx, f).ReadAt(raw, 0)
	if n < len(raw) {
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("read %s: %w", ManifestName, err)
	}
	return raw, nil
}

// openDatasetPart opens one part, checks its size, footer, stamps and row
// count, and returns it with its footer. The returned File is set whenever
// the part was opened, even with an error, so that the caller closes it.
func openDatasetPart(ctx context.Context, src Source, m *manifest.Manifest, entry manifest.Part, lim *Limits) (partInput, error) {
	f, err := src.Open(ctx, entry.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return partInput{}, refusePart(CodeMissingPart, entry.Path, -1, "%v", err)
		}
		return partInput{}, err
	}
	in := partInput{PartSource: PartSource{Name: entry.Path, File: f, Size: f.Size(), MinKey: entry.MinKey, MaxKey: entry.MaxKey}}
	if in.Size != entry.Bytes {
		return in, refusePart(CodePartMismatch, entry.Path, -1, "the file is %d bytes, its manifest entry %d", in.Size, entry.Bytes)
	}
	footer, err := readFooter(ctx, entry.Path, f, in.Size, lim)
	if err != nil {
		return in, err
	}
	if err := checkStamps(entry.Path, footer, m); err != nil {
		return in, err
	}
	if footer.NumRows() != entry.Rows {
		return in, refusePart(CodePartMismatch, entry.Path, -1, "the part holds %d rows, its manifest entry %d", footer.NumRows(), entry.Rows)
	}
	in.footer = footer
	return in, nil
}

// checkStamps applies STAMP-1 and STAMP-2: each stamp exactly once, no
// draft_revision stamp unless format_version is 0, values compared as exact
// strings with the manifest's canonical decimal and schema_id.
func checkStamps(name string, footer *parquet.File, m *manifest.Manifest) error {
	want := map[string]string{
		StampFormatVersion: strconv.FormatInt(m.Version.FormatVersion, 10),
		StampSchemaID:      m.SchemaID,
		StampKeyColumn:     m.KeyColumn,
		StampKeyUnique:     strconv.FormatBool(m.KeyUnique),
	}
	if m.Version.FormatVersion == 0 {
		want[StampDraftRevision] = strconv.FormatInt(m.Version.DraftRevision, 10)
	}
	got := map[string][]string{}
	for _, kv := range footer.Metadata().KeyValueMetadata {
		switch kv.Key {
		case StampFormatVersion, StampDraftRevision, StampSchemaID, StampKeyColumn, StampKeyUnique:
			got[kv.Key] = append(got[kv.Key], kv.Value)
		}
	}
	for _, key := range []string{StampFormatVersion, StampDraftRevision, StampSchemaID, StampKeyColumn, StampKeyUnique} {
		values := got[key]
		expected, required := want[key]
		switch {
		case !required && len(values) > 0:
			return refusePart(CodeStampMismatch, name, -1, "%s is present on a final format version", key)
		case !required:
		case len(values) == 0:
			return refusePart(CodeStampMismatch, name, -1, "the footer has no %s", key)
		case len(values) > 1:
			return refusePart(CodeStampMismatch, name, -1, "the footer has %s %d times", key, len(values))
		case values[0] != expected:
			return refusePart(CodeStampMismatch, name, -1, "%s is %q, the manifest's %q", key, values[0], expected)
		}
	}
	return nil
}

// checkSorted applies RG-1: a non-empty row group declares the key column
// ascending as its first sorting column.
func checkSorted(name string, footer *parquet.File, physical int, group *format.RowGroup) error {
	return checkSortedColumn(name, footer, physical, group, KeyColumn)
}

func checkSortedColumn(name string, footer *parquet.File, physical int, group *format.RowGroup, keyColumn string) error {
	key, _ := footer.Schema().Lookup(keyColumn)
	sorting := group.SortingColumns
	if len(sorting) == 0 || int(sorting[0].ColumnIdx) != key.ColumnIndex || sorting[0].Descending {
		return refusePart(CodeMissingSortingColumns, name, physical, "the row group does not declare %s ascending as its first sorting column", keyColumn)
	}
	return nil
}

// Metadata returns a copy of the dataset's manifest metadata.
func (d *Dataset) Metadata() Metadata { return d.meta.clone() }

// Reader returns the dataset's Reader. Closing it closes the dataset.
func (d *Dataset) Reader() *Reader { return d.reader }

// Range returns the rows of rng; see Reader.Range.
func (d *Dataset) Range(ctx context.Context, rng Range, stats *Stats) ([]Row, error) {
	return d.reader.Range(ctx, rng, stats)
}

// List returns the entries of req; see Reader.List.
func (d *Dataset) List(ctx context.Context, req List, stats *Stats) ([]Entry, error) {
	return d.reader.List(ctx, req, stats)
}

// Scan hands the rows of rng to sink; see Reader.Scan.
func (d *Dataset) Scan(ctx context.Context, rng Range, stats *Stats, sink ColumnSink) error {
	return d.reader.Scan(ctx, rng, stats, sink)
}

// Close closes the dataset's parts once the reads in progress end; see
// Reader.Close.
func (d *Dataset) Close() error { return d.reader.Close() }
