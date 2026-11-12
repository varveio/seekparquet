// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"io"
	"strconv"

	"github.com/parquet-go/parquet-go"
)

// Footer is what one part's footer says: its row count, its top-level
// columns in schema order, by name (Columns) and as declared (Schema), its
// created_by, and the version, schema id and key declaration of its
// seekparquet.* stamps (STAMP-1).
type Footer struct {
	Rows      int64
	Columns   []string
	Schema    []Column
	CreatedBy string
	Version   Version
	SchemaID  string
	Key       Key
}

// Column is one top-level column of a part as its footer's schema element
// declares it.
type Column struct {
	Name string
	// Group reports a column with nested fields, which has no physical
	// type: Type is then KindNone and Length zero.
	Group bool
	// Type is the physical type, and Length a FIXED_LEN_BYTE_ARRAY's
	// type_length (0 for every other type).
	Type   Kind
	Length int32
	// Logical is the logical annotation, with its parameters, the zero
	// Logical when the column has none: the logical_type field, or else the
	// legacy converted_type normalized into it as Parquet's LogicalTypes.md
	// maps it (time and timestamp adjusted to UTC; INTERVAL has no logical
	// type).
	Logical    Logical
	Repetition Repetition
	// Key reports the dataset's key column.
	Key bool
}

// String describes the column for people, such as
// "mtime INT64 OPTIONAL TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)".
func (c Column) String() string {
	s := c.Name + " "
	switch {
	case c.Group:
		s += "group"
	case c.Type == KindFixedLenByteArray:
		s += c.Type.String() + "(" + strconv.Itoa(int(c.Length)) + ")"
	default:
		s += c.Type.String()
	}
	s += " " + c.Repetition.String()
	if c.Logical.Kind != LogicalNone {
		s += " " + c.Logical.String()
	}
	if c.Key {
		s += " key"
	}
	return s
}

// KeyPage is one data page of a part's key column, as the row group's
// page indexes describe it (RG-2 to RG-4).
type KeyPage struct {
	// RowGroup is the page's physical row group in the part.
	RowGroup int
	// FirstRow is the page's first row, counted from the start of the part,
	// and Rows its row count.
	FirstRow, Rows int64
	// Min and Max are the page's ColumnIndex bounds: at or below its first
	// key and at or above its last (RG-5). They are exactly its first and
	// last keys when the writer wrote exact bounds, as package write does;
	// a writer may also have shortened them, and nothing in the file says
	// which (RG-6).
	Min, Max []byte
}

// InspectFooter reads one part file of size bytes through r and returns
// its footer, checked as a read checks a part's footer the first time it
// touches the part (CHECK-1), but against the part's own stamps instead of a manifest:
// readable within the default Limits, self-contained and unencrypted
// (FILE-1, FILE-2), the five stamps each present once in canonical form
// (STAMP-2) naming a version this package reads (VER-1), the stamped key
// column valid (KEY-1, KEY-5), and the key ascending as the first sorting
// column of every non-empty row group (RG-1). It reads the file's first
// four bytes and its footer, nothing else. A fault is a *RefusalError with
// an empty Part.
func InspectFooter(r io.ReaderAt, size int64) (Footer, error) {
	rd, ft, err := inspect(r, size)
	if err != nil {
		return Footer{}, err
	}
	_ = rd.close()
	return ft, nil
}

// InspectKeyPages reads one part file of size bytes through r and returns
// every page of its key column, which must be keyColumn, in file order:
// it checks the footer as InspectFooter does (stamp_mismatch when the
// stamps name another key column), then each non-empty row group's key
// ColumnIndex and OffsetIndex as a read does before it uses the group
// (RG-2 to RG-4). It reads the footer and those two indexes per row group,
// never a data page, so the keys themselves are not checked (CHECK-4).
func InspectKeyPages(r io.ReaderAt, size int64, keyColumn string) ([]KeyPage, error) {
	rd, ft, err := inspect(r, size)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rd.close() }()
	if ft.Key.Column != keyColumn {
		return nil, refusePart(CodeStampMismatch, "", -1, "%s is %q, not %q", StampKeyColumn, ft.Key.Column, keyColumn)
	}
	var pages []KeyPage
	p := rd.parts[0]
	for _, g := range p.groups {
		index, err := p.keyIndex(context.Background(), g.physical, new(ReadStats))
		if err != nil {
			return nil, err
		}
		for _, page := range index.pages {
			pages = append(pages, KeyPage{RowGroup: g.physical, FirstRow: g.start + page.first, Rows: page.rows, Min: page.lo, Max: page.hi})
		}
	}
	return pages, nil
}

// readerOnly hides a caller's Close from the reader, which closes the
// files it was given.
type readerOnly struct{ io.ReaderAt }

// inspect opens one part, without a manifest, as a reader over its own
// stamps.
func inspect(r io.ReaderAt, size int64) (*reader, Footer, error) {
	ctx := context.Background()
	lim := Limits{}.withDefaults()
	footer, _, err := readFooter(ctx, "", r, size, &lim, nil, 0, 1, false)
	if err != nil {
		return nil, Footer{}, err
	}
	ft, err := readStamps(footer)
	if err != nil {
		return nil, Footer{}, err
	}
	rd, err := openParts(ctx, []partInput{{file: readerOnly{r}, size: size, footer: footer}}, ft.Key, OpenOptions{Limits: lim})
	if err != nil {
		return nil, Footer{}, err
	}
	ft.Rows, ft.CreatedBy = footer.NumRows(), footer.Metadata().CreatedBy
	for _, f := range footer.Schema().Fields() {
		ft.Columns = append(ft.Columns, f.Name())
	}
	ft.Schema = columnsOf(footer.Metadata().Schema, ft.Key.Column)
	return rd, ft, nil
}

// readStamps reads a footer's seekparquet.* stamps (STAMP-1, STAMP-2):
// each once, draft_revision only on format_version 0, versions in
// canonical decimal naming a supported pair (VER-1), key_unique "true" or
// "false", and a non-empty schema id and key column.
func readStamps(footer *parquet.File) (Footer, error) {
	values := map[string][]string{}
	for _, kv := range footer.Metadata().KeyValueMetadata {
		values[kv.Key] = append(values[kv.Key], kv.Value)
	}
	bad := func(format string, args ...any) error {
		return refusePart(CodeStampMismatch, "", -1, format, args...)
	}
	one := func(key string) (string, error) {
		if n := len(values[key]); n != 1 {
			return "", bad("the footer has %s %d times", key, n)
		}
		return values[key][0], nil
	}
	decimal := func(key string) (int64, error) {
		s, err := one(key)
		if err != nil {
			return 0, err
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 || strconv.FormatInt(n, 10) != s {
			return 0, bad("%s is %q, not a canonical decimal", key, s)
		}
		return n, nil
	}
	var ft Footer
	var err error
	if ft.Version.FormatVersion, err = decimal(StampFormatVersion); err != nil {
		return Footer{}, err
	}
	if ft.Version.FormatVersion == 0 {
		if ft.Version.DraftRevision, err = decimal(StampDraftRevision); err != nil {
			return Footer{}, err
		}
	} else if len(values[StampDraftRevision]) > 0 {
		return Footer{}, bad("%s is present on format version %d", StampDraftRevision, ft.Version.FormatVersion)
	}
	supported := false
	for _, v := range SupportedVersions() {
		supported = supported || v == ft.Version
	}
	if !supported {
		return Footer{}, refusePart(CodeUnsupportedVersion, "", -1, "the part is stamped format_version %d, draft_revision %d", ft.Version.FormatVersion, ft.Version.DraftRevision)
	}
	if ft.SchemaID, err = one(StampSchemaID); err != nil {
		return Footer{}, err
	}
	if ft.Key.Column, err = one(StampKeyColumn); err != nil {
		return Footer{}, err
	}
	unique, err := one(StampKeyUnique)
	if err != nil {
		return Footer{}, err
	}
	switch {
	case unique != "true" && unique != "false":
		return Footer{}, bad("%s is %q, not true or false", StampKeyUnique, unique)
	case ft.SchemaID == "" || ft.Key.Column == "":
		return Footer{}, bad("the stamped schema id or key column is empty")
	}
	ft.Key.Unique = unique == "true"
	return ft, nil
}
