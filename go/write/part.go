// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package write writes SeekParquet part files (format 1, draft revision 1):
// streaming, from rows already in key order, with everything the profile
// requires of a part and nothing a caller can switch off.
//
// A part written by NewPart has the seekparquet.* stamps (STAMP-1),
// TYPE_ORDER column orders (KEY-5), the key column ascending as every row
// group's first sorting column (RG-1), a ColumnIndex and an OffsetIndex on
// every column, the key column's among them (RG-2 to RG-4), with exact
// page bounds (RG-5), and key pages of at most 1,024 rows by default
// (GEO-1). Its keys are checked as they arrive (KEY-2, KEY-3, KEY-6).
// Publishing parts and committing a manifest is package local's.
package write

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Geometry defaults and the fixed buffer sizes of the encoder. The fixed
// sizes decide page boundaries and so the bytes of a part: they are not
// options.
const (
	defaultPageRows      = 1024    // GEO-1
	defaultRowGroupBytes = 8 << 20 // on parquet-go's size estimate, checked after each page
	defaultRowGroupRows  = 1_000_000
	dictionaryBytes      = 8 << 10 // a column chunk's dictionary limit, then PLAIN
	pageBufferBytes      = 1 << 20 // parquet-go's page buffer: a column's page also ends there
	writeBufferBytes     = 4096    // parquet-go's output buffer
	writeQuantum         = 64      // rows per column write: page boundaries depend on it
)

// ErrProfile reports rows or a schema that break the profile: the error
// names the rule (for example KEY-3).
var ErrProfile = errors.New("seekparquet/write: profile violation")

func violation(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrProfile}, args...)...)
}

// PartFile is where a part is written: an *os.File, or a wrapper that
// counts, hashes or injects faults. Seal syncs and closes it; Abort closes
// it. Its name and removal are the caller's.
type PartFile interface {
	io.Writer
	Sync() error
	Close() error
}

// PartOptions configures a part. Key and SchemaID are required; every
// other zero field takes its default.
type PartOptions struct {
	// Key names the key column, a top-level REQUIRED BYTE_ARRAY column of
	// the schema, unannotated or annotated STRING (KEY-1), and declares
	// whether keys are unique: strictly ascending when Unique, otherwise
	// non-decreasing with equal keys consecutive (KEY-3, KEY-7).
	Key seekparquet.Key
	// SchemaID is the producer's schema id, stamped on the part
	// (STAMP-1) and repeated by the manifest.
	SchemaID string
	// Codec compresses every page; it must be safe for concurrent use when
	// ColumnWorkers allows several. Nil is ZSTD at its default speed.
	Codec compress.Codec
	// PageRows is the most rows in a page of any column; 0 is 1,024
	// (GEO-1). A column's page also ends once its buffer reaches 1 MiB.
	PageRows int
	// RowGroupBytes and RowGroupRows end a row group, after a page, once
	// parquet-go's estimate of its encoded size reaches RowGroupBytes or
	// its rows reach RowGroupRows; 0 is 8 MiB and 1,000,000.
	RowGroupBytes int64
	RowGroupRows  int64
	// CreatedBy is the footer's created_by, verbatim; "" is parquet-go's.
	CreatedBy string
	// ColumnWorkers, when set, is called before each page and returns how
	// many goroutines encode the page's columns; nil, or a result below 2,
	// encodes them in turn. The bytes written do not depend on it.
	ColumnWorkers func() int
}

// PartWriter writes one part. It is not safe for concurrent use.
type PartWriter struct {
	opt   PartOptions
	file  PartFile
	out   *tracking
	w     *parquet.GenericWriter[any]
	key   int            // the key column's leaf index
	utf8  bool           // the key column is annotated STRING or UTF8 (KEY-6)
	kinds []parquet.Kind // each leaf column's physical kind
	page  []column       // each leaf column's values of the page being filled
	flat  bool           // no leaf column repeats: one value per row in each

	pageRows, groupRows int64
	baseline            int64 // w.Size() when the row group began
	rows                int64
	minKey, lastKey     []byte
	err                 error // the first error, returned by every later call
	done                bool
}

// column holds one leaf column's values of the page being filled. Byte
// values are copied into data, so that a caller may reuse its rows, and
// rebuilt from it when the page is encoded.
type column struct {
	values []parquet.Value
	data   []byte
	spans  []span
}

// span is values[value]'s bytes, data[start:end].
type span struct{ value, start, end int }

// NewPart starts a part written to f. It checks the key column against
// KEY-1 and the options, and fails with ErrProfile when they do not
// describe a part of the profile. The schema is used as given, with its
// columns' encodings and compression, except that every page is
// compressed with opt.Codec and the key column's ColumnIndex is marked
// ASCENDING (RG-6). The caller keeps f when NewPart fails.
func NewPart(f PartFile, schema *parquet.Schema, opt PartOptions) (*PartWriter, error) {
	if opt.SchemaID == "" {
		return nil, violation("STAMP-1: no schema id")
	}
	if opt.PageRows < 0 || opt.RowGroupBytes < 0 || opt.RowGroupRows < 0 {
		return nil, fmt.Errorf("seekparquet/write: negative page or row group size")
	}
	if opt.PageRows == 0 {
		opt.PageRows = defaultPageRows
	}
	if opt.RowGroupBytes == 0 {
		opt.RowGroupBytes = defaultRowGroupBytes
	}
	if opt.RowGroupRows == 0 {
		opt.RowGroupRows = defaultRowGroupRows
	}
	if opt.Codec == nil {
		opt.Codec = defaultCodec
	}
	field, utf8Key, err := keyField(schema, opt.Key.Column)
	if err != nil {
		return nil, err
	}
	leaf, _ := schema.Lookup(opt.Key.Column)
	options := []parquet.WriterOption{
		parquet.NewSchema(schema.Name(), keyed{schema, field}),
		parquet.Compression(opt.Codec),
		parquet.DataPageVersion(2),
		parquet.DataPageStatistics(false),
		parquet.PageBufferSize(pageBufferBytes),
		parquet.WriteBufferSize(writeBufferBytes),
		parquet.MaxRowsPerRowGroup(opt.RowGroupRows),
		parquet.DictionaryMaxBytes(dictionaryBytes),
		parquet.ColumnIndexSizeLimit(exactBounds),
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
		parquet.DefaultEncodingFor(parquet.Int64, &parquet.DeltaBinaryPacked),
		parquet.DefaultEncodingFor(parquet.Int32, &parquet.DeltaBinaryPacked),
		parquet.DefaultEncodingFor(parquet.Boolean, &parquet.Plain),
		parquet.KeyValueMetadata(seekparquet.StampFormatVersion, strconv.FormatInt(seekparquet.FormatVersion, 10)),
		parquet.KeyValueMetadata(seekparquet.StampDraftRevision, strconv.FormatInt(seekparquet.DraftRevision, 10)),
		parquet.KeyValueMetadata(seekparquet.StampSchemaID, opt.SchemaID),
		parquet.KeyValueMetadata(seekparquet.StampKeyColumn, opt.Key.Column),
		parquet.KeyValueMetadata(seekparquet.StampKeyUnique, strconv.FormatBool(opt.Key.Unique)),
		parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending(opt.Key.Column))),
	}
	if opt.CreatedBy != "" {
		options = append(options, &parquet.WriterConfig{CreatedBy: opt.CreatedBy})
	}
	out := &tracking{w: f, digest: sha256.New()}
	w := &PartWriter{opt: opt, file: f, out: out, w: parquet.NewGenericWriter[any](out, options...), key: leaf.ColumnIndex, utf8: utf8Key}
	w.kinds = make([]parquet.Kind, len(schema.Columns()))
	w.flat = true
	for _, path := range schema.Columns() {
		c, _ := schema.Lookup(path...)
		w.kinds[c.ColumnIndex] = c.Node.Type().Kind()
		w.flat = w.flat && c.MaxRepetitionLevel == 0
	}
	if n := len(w.w.ColumnWriters()); n != len(w.kinds) {
		return nil, fmt.Errorf("seekparquet/write: %d column writers for %d columns", n, len(w.kinds))
	}
	w.page = make([]column, len(w.kinds))
	for i := range w.page {
		w.page[i].values = make([]parquet.Value, 0, opt.PageRows)
	}
	w.baseline = w.w.Size()
	return w, nil
}

// keyField returns the position of the top-level field named column after
// checking it against KEY-1: one such field, a REQUIRED BYTE_ARRAY leaf,
// unannotated or annotated STRING (or UTF8), and whether it is annotated.
func keyField(schema *parquet.Schema, column string) (int, bool, error) {
	if column == "" {
		return 0, false, violation("KEY-1: no key column")
	}
	found := -1
	for i, f := range schema.Fields() {
		if f.Name() != column {
			continue
		}
		if found >= 0 {
			return 0, false, violation("KEY-1: two top-level columns are named %q", column)
		}
		found = i
	}
	if found < 0 {
		return 0, false, violation("KEY-1: the schema has no top-level column %q", column)
	}
	f := schema.Fields()[found]
	switch {
	case !f.Leaf():
		return 0, false, violation("KEY-1: the key column %q is nested", column)
	case !f.Required():
		return 0, false, violation("KEY-1: the key column %q is not REQUIRED", column)
	case f.Type().Kind() != parquet.ByteArray:
		return 0, false, violation("KEY-1: the key column %q is %s, not BYTE_ARRAY", column, f.Type().Kind())
	}
	utf8Key := false
	if lt := f.Type().LogicalType(); lt != nil && lt.Value != nil {
		if _, ok := lt.Value.(*format.StringType); !ok {
			return 0, false, violation("KEY-1: the key column %q is annotated %s", column, lt)
		}
		utf8Key = true
	}
	if ct := f.Type().ConvertedType(); ct != nil {
		if *ct != deprecated.UTF8 {
			return 0, false, violation("KEY-1: the key column %q has converted type %d", column, *ct)
		}
		utf8Key = true
	}
	return found, utf8Key, nil
}

// WriteRows appends rows to the part. Rows are parquet-go rows of the
// schema, with their values in column order; they may be reused once
// WriteRows returns. Keys must be non-empty (KEY-2), valid UTF-8 when the
// key column is annotated STRING (KEY-6), and follow the
// previous row's key (KEY-3): strictly above it when keys are unique, at
// or above it otherwise.
//
// WriteRows returns the number of rows accepted. A row that breaks the
// profile, or does not match the schema, fails with ErrProfile, and the
// rows before it stay accepted. After any error the part cannot be
// sealed: every later call returns the same error, and the caller aborts
// it.
func (w *PartWriter) WriteRows(rows []parquet.Row) (int, error) {
	if w.done {
		return 0, errors.New("seekparquet/write: the part is sealed or aborted")
	}
	if w.err != nil {
		return 0, w.err
	}
	for i, row := range rows {
		key, err := w.check(row)
		if err != nil {
			w.err = err
			return i, err
		}
		for _, v := range row {
			c := &w.page[v.Column()]
			if k := w.kinds[v.Column()]; (k == parquet.ByteArray || k == parquet.FixedLenByteArray) && !v.IsNull() {
				start := len(c.data)
				c.data = append(c.data, v.ByteArray()...)
				c.spans = append(c.spans, span{value: len(c.values), start: start, end: len(c.data)})
				v = parquet.NullValue().Level(v.RepetitionLevel(), v.DefinitionLevel(), v.Column())
			}
			c.values = append(c.values, v)
		}
		if w.rows == 0 {
			w.minKey = bytes.Clone(key)
		}
		w.lastKey = append(w.lastKey[:0], key...)
		w.rows++
		w.pageRows++
		if w.pageRows == int64(w.opt.PageRows) {
			if err := w.flushPage(); err != nil {
				w.err = err
				return i + 1, err
			}
		}
	}
	return len(rows), nil
}

// check requires row to hold every leaf column in order, each column's
// values starting a row (repetition level 0) and continuing it, of the
// column's kind or null, and returns its key after checking it.
func (w *PartWriter) check(row parquet.Row) ([]byte, error) {
	var key []byte
	next := 0
	for i, v := range row {
		c := v.Column()
		switch {
		case c == next && c < len(w.kinds) && v.RepetitionLevel() == 0:
			next++
		case c == next-1 && v.RepetitionLevel() > 0:
		default:
			return nil, violation("row %d does not match the schema: value %d is of column %d, level %d", w.rows, i, c, v.RepetitionLevel())
		}
		if !v.IsNull() && v.Kind() != w.kinds[c] {
			return nil, violation("row %d does not match the schema: column %d holds a %s value, not %s", w.rows, c, v.Kind(), w.kinds[c])
		}
		if c == w.key {
			key = v.ByteArray()
			if v.IsNull() || len(key) == 0 {
				return nil, violation("KEY-2: row %d has an empty key", w.rows)
			}
			if w.utf8 && !utf8.Valid(key) {
				return nil, violation("KEY-6: row %d: the STRING key %q is not valid UTF-8", w.rows, key)
			}
		}
	}
	if next != len(w.kinds) {
		return nil, violation("row %d does not match the schema: %d columns of %d", w.rows, next, len(w.kinds))
	}
	if w.rows > 0 {
		switch cmp := bytes.Compare(key, w.lastKey); {
		case cmp < 0:
			return nil, violation("KEY-3: row %d: key %q is below the key before it, %q", w.rows, key, w.lastKey)
		case cmp == 0 && w.opt.Key.Unique:
			return nil, violation("KEY-3: row %d: key %q repeats with unique keys", w.rows, key)
		}
	}
	return key, nil
}

// flushPage encodes the rows WriteRows buffered as one page of every
// column.
func (w *PartWriter) flushPage() error {
	if w.pageRows == 0 {
		return nil
	}
	err := w.encodePage(func(i int) []parquet.Value {
		c := &w.page[i]
		for _, s := range c.spans {
			v := c.values[s.value]
			b := c.data[s.start:s.end:s.end]
			if w.kinds[i] == parquet.FixedLenByteArray {
				c.values[s.value] = parquet.FixedLenByteArrayValue(b).Level(v.RepetitionLevel(), v.DefinitionLevel(), i)
			} else {
				c.values[s.value] = parquet.ByteArrayValue(b).Level(v.RepetitionLevel(), v.DefinitionLevel(), i)
			}
		}
		return c.values
	})
	for i := range w.page {
		c := &w.page[i]
		c.values, c.data, c.spans = c.values[:0], c.data[:0], c.spans[:0]
	}
	if err != nil {
		return err
	}
	n := w.pageRows
	w.pageRows = 0
	return w.endPage(n)
}

// endPage ends the row group after a page of n rows when it has reached
// its size or row target.
func (w *PartWriter) endPage(n int64) error {
	w.groupRows += n
	if w.w.Size()-w.baseline >= w.opt.RowGroupBytes || w.groupRows >= w.opt.RowGroupRows {
		return w.flushGroup()
	}
	return nil
}

// encodePage writes and flushes every column of a page, whose values
// column(i) returns for leaf column i. parquet-go column writers are
// independent (their own buffers and page state; the shared pools and the
// codec are safe for concurrent use), so several goroutines may take
// columns, column(i) included; the bytes equal the serial result.
func (w *PartWriter) encodePage(column func(i int) []parquet.Value) error {
	cols := w.w.ColumnWriters()
	one := func(i int) error { return writeColumn(cols[i], column(i)) }
	workers := 1
	if w.opt.ColumnWorkers != nil {
		workers = min(w.opt.ColumnWorkers(), len(cols))
	}
	if workers <= 1 {
		for i := range cols {
			if err := one(i); err != nil {
				return err
			}
		}
		return nil
	}
	var next atomic.Int32
	errs := make([]error, len(cols))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := int(next.Add(1) - 1); i < len(cols); i = int(next.Add(1) - 1) {
				errs[i] = one(i)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// WritePage writes rows rows as one page of every column, without copying
// them: fill(i, values) sets values, of length rows, to leaf column i's
// value for each row, and the page is encoded before WritePage returns, so
// the values may point into the caller's memory, which must stay unchanged
// until then and may be reused after. It is for a producer that keeps its
// own rows and builds each column's values from them, page by page, as a
// row-at-a-time intake cannot do without copying.
//
// The schema's leaf columns must not repeat (one value per row each), and
// rows must be from 1 to PartOptions.PageRows; rows WriteRows buffered are
// first flushed as a page of their own. fill is called once per column,
// the key column first, on WritePage's goroutine, then the others, on as
// many goroutines as ColumnWorkers allows, so it must be safe to call
// concurrently for different columns. Values must be of the column's
// physical kind, or null where the column is optional, with their levels
// and column index set, as parquet.Schema.Deconstruct makes them. Only the
// key column's are checked, each key once: non-null and non-empty (KEY-2)
// and in the declared order after the key before it (KEY-3). A violation
// fails with ErrProfile before any column is encoded; as with WriteRows,
// any error leaves the part to be aborted. Unlike WriteRows, WritePage
// does not check that a STRING key is valid UTF-8 (KEY-6): that is the
// caller's, as the column's annotation promises it, and validation
// (seekparquet.ValidateDir, profile level) finds a key that breaks it.
func (w *PartWriter) WritePage(rows int, fill func(column int, values []parquet.Value)) error {
	if w.done {
		return errors.New("seekparquet/write: the part is sealed or aborted")
	}
	if w.err != nil {
		return w.err
	}
	if !w.flat || rows < 1 || rows > w.opt.PageRows {
		return fmt.Errorf("seekparquet/write: WritePage of %d rows (1 to %d, a schema whose columns do not repeat)", rows, w.opt.PageRows)
	}
	if err := w.flushPage(); err != nil {
		w.err = err
		return err
	}
	keys := w.page[w.key].values[:rows]
	fill(w.key, keys)
	if err := w.checkKeys(keys); err != nil {
		w.err = err
		return err
	}
	err := w.encodePage(func(i int) []parquet.Value {
		if i == w.key {
			return keys
		}
		values := w.page[i].values[:rows]
		fill(i, values)
		return values
	})
	if err == nil {
		err = w.endPage(int64(rows))
	}
	if err != nil {
		w.err = err
	}
	return err
}

// checkKeys checks a page's keys (KEY-2, KEY-3) and records them.
func (w *PartWriter) checkKeys(keys []parquet.Value) error {
	previous := w.lastKey
	for j, v := range keys {
		row := w.rows + int64(j)
		if v.Kind() != parquet.ByteArray || v.IsNull() {
			return violation("KEY-2: row %d has no key", row)
		}
		key := v.ByteArray()
		if len(key) == 0 {
			return violation("KEY-2: row %d has an empty key", row)
		}
		if row > 0 {
			switch cmp := bytes.Compare(key, previous); {
			case cmp < 0:
				return violation("KEY-3: row %d: key %q is below the key before it, %q", row, key, previous)
			case cmp == 0 && w.opt.Key.Unique:
				return violation("KEY-3: row %d: key %q repeats with unique keys", row, key)
			}
		}
		previous = key
	}
	if w.rows == 0 {
		w.minKey = bytes.Clone(keys[0].ByteArray())
	}
	w.lastKey = append(w.lastKey[:0], previous...)
	w.rows += int64(len(keys))
	return nil
}

// writeColumn writes a column's page values writeQuantum rows at a time,
// then flushes them as a page.
func writeColumn(cw *parquet.ColumnWriter, values []parquet.Value) error {
	for start := 0; start < len(values); {
		end, rows := start, 0
		for ; end < len(values); end++ {
			if values[end].RepetitionLevel() == 0 {
				if rows == writeQuantum {
					break
				}
				rows++
			}
		}
		n, err := cw.WriteRowValues(values[start:end])
		if err != nil {
			return err
		}
		if n != rows {
			return io.ErrShortWrite
		}
		start = end
	}
	return cw.Flush()
}

func (w *PartWriter) flushGroup() error {
	if w.groupRows == 0 {
		return nil
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.groupRows = 0
	w.baseline = w.w.Size()
	return nil
}

// Size is the part's size so far: the bytes written, plus parquet-go's
// estimate of the open row group, before compression. The page being
// filled and the footer are not counted.
func (w *PartWriter) Size() int64 { return w.w.Size() }

// Seal ends the part: it encodes the last page and row group, writes the
// footer, syncs and closes the file, and returns the part's rows, size,
// exact first and last keys (PART-3) and the SHA-256 of every byte written
// (PART-5), computed as they were written. Path is left for the caller,
// who names the part. A part holds at least one row (PART-2): sealing an
// empty one fails. The file is closed whatever the result; the part is
// valid only when Seal returns no error.
func (w *PartWriter) Seal() (seekparquet.PartInfo, error) {
	if w.done {
		return seekparquet.PartInfo{}, errors.New("seekparquet/write: the part is sealed or aborted")
	}
	w.done = true
	err := w.err
	if err == nil && w.rows == 0 {
		err = violation("PART-2: the part holds no rows")
	}
	if err == nil {
		err = w.flushPage()
	}
	if err == nil {
		err = w.flushGroup()
	}
	if err == nil {
		err = w.w.Close()
	}
	if err == nil {
		err = w.file.Sync()
	}
	if err = errors.Join(err, w.file.Close()); err != nil {
		return seekparquet.PartInfo{}, err
	}
	return seekparquet.PartInfo{Rows: w.rows, Bytes: w.out.n, MinKey: w.minKey, MaxKey: bytes.Clone(w.lastKey),
		SHA256: hex.EncodeToString(w.out.digest.Sum(nil))}, nil
}

// Abort ends the part without a footer and closes the file, whose
// removal is the caller's. It does nothing after Seal or Abort.
func (w *PartWriter) Abort() error {
	if w.done {
		return nil
	}
	w.done = true
	return w.file.Close()
}

// tracking counts and hashes the bytes written through it.
type tracking struct {
	w      io.Writer
	digest hash.Hash
	n      int64
}

func (t *tracking) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 {
		t.digest.Write(p[:n])
		t.n += int64(n)
	}
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
