// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package seekparquettest writes small SeekParquet datasets for tests: a
// manifest and parts carrying the seekparquet.* stamps, sorting_columns,
// page indexes and small pages, from rows the test gives, with knobs that
// produce invalid variants. The parts are written by parquet-go, with one
// correction: parquet-go v0.32.0 shortens a page maximum whose retained
// bytes are all 0xFF to a value below the real maximum, which is not a
// bound (RG-5), so this writer keeps such a page's full maximum (and, for a
// STRING column, a full bound wherever a shortened one would not be valid
// UTF-8) unless Part.KeepShortenedMax asks for the broken one.
//
// It is a test helper, not a production writer: rows are written in the
// order given, unsorted rows included.
package seekparquettest

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// SchemaID is the default schema id of a written dataset.
const SchemaID = "seekparquettest/v1"

// Stamp keys, spelled out here so that tests do not take them from the
// code under test.
const (
	StampFormatVersion = "seekparquet.format_version"
	StampDraftRevision = "seekparquet.draft_revision"
	StampSchemaID      = "seekparquet.schema_id"
)

// Row is the default row: an unannotated BYTE_ARRAY key and two optional
// value columns.
type Row struct {
	Key  []byte  `parquet:"key"`
	Size *int64  `parquet:"size,optional"`
	Name *string `parquet:"name,optional"`
}

// Rows returns a Row for each key: row i has size i and name "name-i",
// except that every third row (i%3 == 2) has a null name.
func Rows(keys ...string) []Row {
	rows := make([]Row, len(keys))
	for i, k := range keys {
		size := int64(i)
		rows[i] = Row{Key: []byte(k), Size: &size}
		if i%3 != 2 {
			name := "name-" + strconv.Itoa(i)
			rows[i].Name = &name
		}
	}
	return rows
}

// Part describes one part file.
type Part struct {
	// Rows is a slice of structs whose parquet tags give the schema, with
	// a top-level column named key; nil is []Row{}. Rows are written in
	// the order given.
	Rows any
	// PageBytes is the page buffer size (pages are cut by bytes); 0 is 512.
	PageBytes int
	// GroupRows is the rows per row group; 0 is one group.
	GroupRows int
	// Groups, when set, are the row counts of the row groups in order,
	// overriding GroupRows; a zero writes an empty row group. They must
	// add up to the number of rows.
	Groups []int
	// IndexSizeLimit is the length to which ColumnIndex bounds are
	// shortened; 0 is 16, parquet-go's default.
	IndexSizeLimit int
	// KeepShortenedMax keeps parquet-go's shortened maximum even where it
	// is below the page's last key (an invalid page index, RG-5).
	KeepShortenedMax bool
	// NoKeyPageIndex leaves the key column without a ColumnIndex and an
	// OffsetIndex.
	NoKeyPageIndex bool
	// Unsorted leaves out the sorting_columns metadata (RG-1).
	Unsorted bool
	// Stamps change the footer stamps: a value replaces the default, ""
	// removes the stamp.
	Stamps map[string]string
	// DataPageVersion is 1 or 2; 0 is parquet-go's default.
	DataPageVersion int
}

// Dataset describes a dataset.
type Dataset struct {
	// SchemaID is the manifest's and the stamps' schema id; "" is
	// SchemaID.
	SchemaID string
	// Parts are written in order at PartPath(i).
	Parts []Part
	// Extensions, when set, is written as the manifest's extensions.
	Extensions map[string]any
}

// PartPath returns the manifest path of part i.
func PartPath(i int) string { return fmt.Sprintf("data/part-%05d.parquet", i) }

// Write writes d into dir (created if needed): its parts, then its
// manifest, whose bounds are each part's first and last keys as written.
func Write(dir string, d Dataset) error {
	schemaID := d.SchemaID
	if schemaID == "" {
		schemaID = SchemaID
	}
	m := map[string]any{"format": "seekparquet", "format_version": 0, "draft_revision": 1, "schema_id": schemaID}
	if d.Extensions != nil {
		m["extensions"] = d.Extensions
	}
	parts := []any{}
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		return err
	}
	for i, part := range d.Parts {
		stamps := map[string]string{StampFormatVersion: "0", StampDraftRevision: "1", StampSchemaID: schemaID}
		for k, v := range part.Stamps {
			if v == "" {
				delete(stamps, k)
			} else {
				stamps[k] = v
			}
		}
		rel := PartPath(i)
		path := filepath.Join(dir, filepath.FromSlash(rel))
		first, last, rows, err := WritePart(path, part, stamps)
		if err != nil {
			return fmt.Errorf("part %d: %w", i, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		entry := map[string]any{"path": rel, "rows": rows, "bytes": info.Size()}
		if rows > 0 {
			entry["min_key_base64"] = base64.StdEncoding.EncodeToString(first)
			entry["max_key_base64"] = base64.StdEncoding.EncodeToString(last)
		}
		parts = append(parts, entry)
	}
	m["parts"] = parts
	return writeManifest(dir, m)
}

func writeManifest(dir string, m map[string]any) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644)
}

// EditManifest decodes dir's manifest, applies edit and writes it back.
func EditManifest(dir string, edit func(m map[string]any)) error {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return err
	}
	edit(m)
	return writeManifest(dir, m)
}

// ManifestPart returns part i of a decoded manifest, for EditManifest.
func ManifestPart(m map[string]any, i int) map[string]any {
	return m["parts"].([]any)[i].(map[string]any)
}

// SyncSizes sets every part's bytes in dir's manifest to its file's size,
// after a test rewrote a part.
func SyncSizes(dir string) error {
	var err error
	editErr := EditManifest(dir, func(m map[string]any) {
		for _, p := range m["parts"].([]any) {
			entry := p.(map[string]any)
			info, statErr := os.Stat(filepath.Join(dir, filepath.FromSlash(entry["path"].(string))))
			if statErr != nil {
				err = statErr
				return
			}
			entry["bytes"] = info.Size()
		}
	})
	return errors.Join(editErr, err)
}

// WritePart writes part to path with the footer key-value metadata stamps
// and returns its first and last keys and row count.
func WritePart(path string, part Part, stamps map[string]string) (first, last []byte, rows int, err error) {
	rowsValue := reflect.ValueOf(part.Rows)
	if part.Rows == nil {
		rowsValue = reflect.ValueOf([]Row{})
	}
	if rowsValue.Kind() != reflect.Slice {
		return nil, nil, 0, errors.New("Rows is not a slice")
	}
	rows = rowsValue.Len()
	base := parquet.SchemaOf(reflect.New(rowsValue.Type().Elem()).Elem().Interface())
	keyLeaf, ok := base.Lookup("key")
	if !ok {
		return nil, nil, 0, errors.New("the rows have no key column")
	}
	schema := parquet.NewSchema("seekparquettest", boundsNode{Node: base, keep: part.KeepShortenedMax})
	pageBytes := part.PageBytes
	if pageBytes <= 0 {
		pageBytes = 512
	}
	limit := part.IndexSizeLimit
	if limit <= 0 {
		limit = 16
	}
	options := []parquet.WriterOption{
		schema,
		parquet.PageBufferSize(pageBytes),
		parquet.ColumnIndexSizeLimit(func([]string) int { return limit }),
		parquet.DataPageStatistics(true),
	}
	if part.DataPageVersion > 0 {
		options = append(options, parquet.DataPageVersion(part.DataPageVersion))
	}
	if !part.Unsorted {
		options = append(options, parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("key"))))
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range stamps {
			if !yield(k) {
				return
			}
		}
	})
	for _, k := range keys {
		options = append(options, parquet.KeyValueMetadata(k, stamps[k]))
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, 0, err
	}
	w := parquet.NewWriter(f, options...)
	groups := part.Groups
	if groups == nil {
		size := part.GroupRows
		if size <= 0 {
			size = rows + 1
		}
		for i := 0; i < rows; i += size {
			groups = append(groups, min(size, rows-i))
		}
	}
	keyOf := func(i int) []byte {
		row := schema.Deconstruct(nil, rowsValue.Index(i).Interface())
		for _, v := range row {
			if v.Column() == keyLeaf.ColumnIndex && !v.IsNull() {
				return bytes.Clone(v.Bytes())
			}
		}
		return []byte("null")
	}
	fail := func(err error) ([]byte, []byte, int, error) {
		_ = f.Close()
		return nil, nil, 0, err
	}
	next := 0
	for _, n := range groups {
		if next+n > rows {
			return fail(fmt.Errorf("the row groups hold more than the part's %d rows", rows))
		}
		if n == 0 {
			continue // parquet-go writes no empty group; insertEmptyGroups adds them
		}
		for i := next; i < next+n; i++ {
			if err := w.Write(rowsValue.Index(i).Interface()); err != nil {
				return fail(err)
			}
		}
		if err := w.Flush(); err != nil {
			return fail(err)
		}
		next += n
	}
	if next != rows {
		return fail(fmt.Errorf("the row groups hold %d of the part's %d rows", next, rows))
	}
	if err := w.Close(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return nil, nil, 0, err
	}
	if slices.Contains(groups, 0) {
		if err := insertEmptyGroups(path, groups); err != nil {
			return nil, nil, 0, err
		}
	}
	if part.NoKeyPageIndex {
		if err := dropPageIndex(path, keyLeaf.ColumnIndex); err != nil {
			return nil, nil, 0, err
		}
	}
	if rows > 0 {
		first, last = keyOf(0), keyOf(rows-1)
	}
	return first, last, rows, nil
}

// insertEmptyGroups rewrites the footer of the part at path so that its row
// groups have the row counts groups gives, adding a row group of no rows
// (its column chunks empty, without page indexes) at each zero: parquet-go
// skips empty groups when it writes, but other writers leave them.
func insertEmptyGroups(path string, groups []int) error {
	return RewriteFooter(path, func(meta *format.FileMetaData) error {
		if len(meta.RowGroups) == 0 {
			return errors.New("a part of empty groups only is not supported")
		}
		var out []format.RowGroup
		written := meta.RowGroups
		for _, rows := range groups {
			if rows > 0 {
				out = append(out, written[0])
				written = written[1:]
				continue
			}
			empty := format.RowGroup{SortingColumns: meta.RowGroups[0].SortingColumns}
			for _, chunk := range meta.RowGroups[0].Columns {
				md := chunk.MetaData
				md.NumValues, md.TotalUncompressedSize, md.TotalCompressedSize = 0, 0, 0
				md.Statistics, md.DictionaryPageOffset, md.EncodingStats = format.Statistics{}, 0, nil
				empty.Columns = append(empty.Columns, format.ColumnChunk{FileOffset: md.DataPageOffset, MetaData: md})
			}
			out = append(out, empty)
		}
		for i := range out {
			out[i].Ordinal = int16(i)
		}
		meta.RowGroups = out
		return nil
	})
}

// dropPageIndex rewrites the footer of the part at path so that leaf
// column's chunks reference no ColumnIndex and no OffsetIndex.
func dropPageIndex(path string, column int) error {
	return RewriteFooter(path, func(meta *format.FileMetaData) error {
		for g := range meta.RowGroups {
			chunk := &meta.RowGroups[g].Columns[column]
			chunk.ColumnIndexOffset, chunk.ColumnIndexLength = 0, 0
			chunk.OffsetIndexOffset, chunk.OffsetIndexLength = 0, 0
		}
		return nil
	})
}

// RewriteFooter applies change to the footer of the Parquet file at path
// and writes the file back with the new footer: for tests of footers no
// writer produces. The manifest's bytes then need SyncSizes.
func RewriteFooter(path string, change func(*format.FileMetaData) error) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	n := len(raw)
	footerLen := int(binary.LittleEndian.Uint32(raw[n-8 : n-4]))
	var protocol thrift.CompactProtocol
	var meta format.FileMetaData
	if err := thrift.Unmarshal(&protocol, raw[n-8-footerLen:n-8], &meta); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}
	if err := change(&meta); err != nil {
		return err
	}
	footer, err := thrift.Marshal(&protocol, &meta)
	if err != nil {
		return err
	}
	body := append(raw[:n-8-footerLen:n-8-footerLen], footer...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(footer)))
	body = append(body, "PAR1"...)
	return os.WriteFile(path, body, 0o644)
}

// ReadFooter returns the footer of the Parquet file at path.
func ReadFooter(path string) (*format.FileMetaData, error) {
	var meta *format.FileMetaData
	err := rewriteNothing(path, func(m *format.FileMetaData) { meta = m })
	return meta, err
}

func rewriteNothing(path string, see func(*format.FileMetaData)) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	n := len(raw)
	footerLen := int(binary.LittleEndian.Uint32(raw[n-8 : n-4]))
	var meta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), raw[n-8-footerLen:n-8], &meta); err != nil {
		return err
	}
	see(&meta)
	return nil
}

// boundsNode wraps a schema so that its top-level byte-array leaves are
// indexed by boundsIndexer.
type boundsNode struct {
	parquet.Node
	keep bool
}

func (n boundsNode) Fields() []parquet.Field {
	fields := slices.Clone(n.Node.Fields())
	for i, f := range fields {
		if f.Leaf() && f.Type().Kind() == parquet.ByteArray {
			isString := false
			if lt := f.Type().LogicalType(); lt != nil {
				_, isString = lt.Value.(*format.StringType)
			}
			fields[i] = boundsField{Field: f, keep: n.keep, key: f.Name() == "key", utf8: isString}
		}
	}
	return fields
}

type boundsField struct {
	parquet.Field
	keep, key, utf8 bool
}

func (f boundsField) Type() parquet.Type {
	return boundsType{Type: f.Field.Type(), keep: f.keep, key: f.key, utf8: f.utf8}
}

type boundsType struct {
	parquet.Type
	keep, key, utf8 bool
}

func (t boundsType) NewColumnIndexer(sizeLimit int) parquet.ColumnIndexer {
	return &boundsIndexer{ColumnIndexer: t.Type.NewColumnIndexer(sizeLimit), t: t}
}

// boundsIndexer records each page's real minimum and maximum and corrects
// the shortened bounds parquet-go computes: a maximum below the real one
// (all retained bytes 0xFF) is replaced by the real maximum (RG-5), unless
// keep is set, and a bound of a STRING column that is not valid UTF-8 by
// the real value (KEY-6). For the key column it declares boundary_order
// ASCENDING when both bound sequences are non-decreasing, UNORDERED
// otherwise (RG-6).
type boundsIndexer struct {
	parquet.ColumnIndexer
	t          boundsType
	mins, maxs [][]byte
}

func (ix *boundsIndexer) Reset() {
	ix.ColumnIndexer.Reset()
	ix.mins, ix.maxs = nil, nil
}

func (ix *boundsIndexer) IndexPage(numValues, numNulls int64, min, max parquet.Value) {
	if numValues > numNulls {
		ix.mins = append(ix.mins, bytes.Clone(min.ByteArray()))
		ix.maxs = append(ix.maxs, bytes.Clone(max.ByteArray()))
	} else {
		ix.mins, ix.maxs = append(ix.mins, nil), append(ix.maxs, nil)
	}
	ix.ColumnIndexer.IndexPage(numValues, numNulls, min, max)
}

func (ix *boundsIndexer) ColumnIndex() format.ColumnIndex {
	index := ix.ColumnIndexer.ColumnIndex()
	index.MinValues = slices.Clone(index.MinValues)
	index.MaxValues = slices.Clone(index.MaxValues)
	for i := range index.MaxValues {
		if i >= len(ix.maxs) || ix.maxs[i] == nil {
			continue
		}
		if !ix.t.keep && bytes.Compare(index.MaxValues[i], ix.maxs[i]) < 0 {
			index.MaxValues[i] = ix.maxs[i]
		}
		if ix.t.utf8 && !utf8.Valid(index.MaxValues[i]) {
			index.MaxValues[i] = ix.maxs[i]
		}
		if ix.t.utf8 && !utf8.Valid(index.MinValues[i]) {
			index.MinValues[i] = ix.mins[i]
		}
	}
	if ix.t.key {
		index.BoundaryOrder = format.Unordered
		if nondescending(index.MinValues) && nondescending(index.MaxValues) {
			index.BoundaryOrder = format.Ascending
		}
	}
	return index
}

func nondescending(values [][]byte) bool {
	for i := 1; i < len(values); i++ {
		if bytes.Compare(values[i-1], values[i]) > 0 {
			return false
		}
	}
	return true
}
