// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/thriftcheck"
)

// storageError marks an error the source itself returned from ReadAt, so
// that a failure to read storage is never mistaken for corrupt data
// (CHECK-5): it is reported as an error, not a refusal.
type storageError struct{ err error }

func (e *storageError) Error() string { return e.err.Error() }
func (e *storageError) Unwrap() error { return e.err }

// storageReader marks the errors of a source's ReadAt. io.EOF is left
// unmarked: every location the reader follows is first checked to lie
// inside the file, so reading past its end means the data lied.
type storageReader struct {
	r  io.ReaderAt
	st *ReadStats // counts the reads of a request, nil for none
}

func (s storageReader) ReadAt(p []byte, off int64) (int, error) {
	if s.st != nil {
		s.st.SourceReads++
		s.st.SourceBytes += int64(len(p))
	}
	n, err := s.r.ReadAt(p, off)
	if err != nil && !errors.Is(err, io.EOF) {
		err = &storageError{err}
	}
	return n, err
}

// isEnvironment reports whether err comes from the context, Close, a limit
// or the source rather than from the data.
func isEnvironment(ctx context.Context, err error) bool {
	var se *storageError
	return ctx.Err() != nil || errors.As(err, &se) || errors.Is(err, ErrClosed) || errors.Is(err, ErrLimitExceeded) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// readFull reads len(p) bytes at off, reporting a short read of the data
// as io.ErrUnexpectedEOF.
func readFull(at io.ReaderAt, p []byte, off int64) error {
	n, err := at.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// errEncrypted reports an encrypted part (FILE-2).
var errEncrypted = &UnsupportedError{Code: UnsupportedEncryption, Detail: "the part uses Parquet modular encryption"}

// prefetched serves reads that fall inside data, the bytes at off, from
// memory, and every other read from r.
type prefetched struct {
	r    io.ReaderAt
	off  int64
	data []byte
}

func (p prefetched) ReadAt(b []byte, off int64) (int, error) {
	if off >= p.off && off-p.off <= int64(len(p.data)) && int64(len(b)) <= int64(len(p.data))-(off-p.off) {
		return copy(b, p.data[off-p.off:]), nil
	}
	return p.r.ReadAt(b, off)
}

// decodeBounded decodes one thrift struct into v from raw after
// thriftcheck has shown that its lengths fit and its decoded size is
// within budget. It returns the number of bytes the struct occupies.
func decodeBounded(raw []byte, v any, budget int64) (int, error) {
	n, _, err := thriftcheck.Check(raw, reflect.TypeOf(v), budget)
	if errors.Is(err, thriftcheck.ErrBudget) {
		return 0, exceeded("decoding would use more than %d bytes (Limits.MetadataMemory)", budget)
	}
	if err != nil {
		return 0, err
	}
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), raw[:n], v); err != nil {
		return 0, err
	}
	return n, nil
}

// readFooter reads a part's footer and opens it with parquet-go, without
// its page indexes or bloom filters. Before parquet-go sees the footer it
// is read within Limits.FooterBytes, decoded within Limits.MetadataMemory,
// and checked (checkMetadata), so that hostile metadata cannot make the
// dependency allocate beyond the limits or index out of range. As a last
// line of defence, a panic inside parquet-go while it opens the footer is
// reported as invalid_footer rather than crashing the program.
//
// A fault in the data is a *RefusalError; an encrypted part an
// *UnsupportedError; a limit or storage failure an error.
func readFooter(ctx context.Context, name string, file io.ReaderAt, size int64, lim *Limits) (_ *parquet.File, err error) {
	at := storageReader{r: bind(ctx, file)}
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidFooter, name, -1, format, args...)
	}
	if size < 12 {
		return nil, bad("the file is too short to be Parquet")
	}
	var head, tail [8]byte
	if err := readFull(at, head[:4], 0); err != nil {
		return nil, footerError(ctx, name, err)
	}
	if err := readFull(at, tail[:], size-8); err != nil {
		return nil, footerError(ctx, name, err)
	}
	if string(head[:4]) == "PARE" || string(tail[4:]) == "PARE" {
		return nil, errEncrypted
	}
	if string(head[:4]) != "PAR1" || string(tail[4:]) != "PAR1" {
		return nil, bad("the file does not begin and end with PAR1")
	}
	n := int64(binary.LittleEndian.Uint32(tail[:4]))
	if n > size-12 {
		return nil, bad("the footer length %d does not fit in the file", n)
	}
	if n > lim.FooterBytes {
		return nil, exceeded("part %s: the footer is %d bytes (Limits.FooterBytes %d)", name, n, lim.FooterBytes)
	}
	raw := make([]byte, n+8)
	if err := readFull(at, raw, size-8-n); err != nil {
		return nil, footerError(ctx, name, err)
	}
	var meta format.FileMetaData
	used, err := decodeBounded(raw[:n], &meta, lim.MetadataMemory)
	switch {
	case errors.Is(err, ErrLimitExceeded):
		return nil, fmt.Errorf("part %s: footer: %w", name, err)
	case err != nil:
		return nil, bad("decode the footer: %v", err)
	case int64(used) == n-28:
		return nil, errEncrypted // a signed plaintext footer
	case int64(used) != n:
		return nil, bad("the footer has %d bytes after its metadata", n-int64(used))
	}
	if err := checkMetadata(name, &meta); err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = bad("parquet-go failed on the footer: %v", r)
		}
	}()
	f, err := parquet.OpenFile(prefetched{at, size - 8 - n, raw}, size,
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true), parquet.ReadBufferSize(4096))
	if err != nil {
		return nil, footerError(ctx, name, err)
	}
	return f, nil
}

// footerError classifies an error reading a footer: the environment's own
// error as is, anything else invalid_footer.
func footerError(ctx context.Context, name string, err error) error {
	var unsupported *UnsupportedError
	if isEnvironment(ctx, err) || errors.As(err, &unsupported) {
		return err
	}
	return refusePart(CodeInvalidFooter, name, -1, "%v", err)
}

// checkMetadata checks a decoded footer before parquet-go builds anything
// from it: no encryption (FILE-2); a well-formed schema tree whose leaves
// carry valid physical types and annotations; row groups with one column
// chunk per leaf, none referring to another file (FILE-1), and row counts
// that sum to the footer's (CHECK-1); and sorting_columns entries that name
// leaves (missing_sorting_columns for a non-empty row group's first entry,
// invalid_footer for any other).
func checkMetadata(name string, meta *format.FileMetaData) error {
	bad := func(group int, format string, args ...any) error {
		return refusePart(CodeInvalidFooter, name, group, format, args...)
	}
	if meta.EncryptionAlgorithm.Value != nil || len(meta.FooterSigningKeyMetadata) > 0 {
		return errEncrypted
	}
	leaves, err := checkSchema(meta.Schema)
	if err != nil {
		return bad(-1, "%v", err)
	}
	var total int64
	for g := range meta.RowGroups {
		rg := &meta.RowGroups[g]
		if rg.NumRows < 0 || total+rg.NumRows < total {
			return bad(g, "the row count is invalid")
		}
		total += rg.NumRows
		if len(rg.Columns) != leaves {
			return bad(g, "the row group has %d column chunks for %d leaf columns", len(rg.Columns), leaves)
		}
		for c := range rg.Columns {
			chunk := &rg.Columns[c]
			if chunk.FilePath != "" {
				return bad(g, "a column chunk refers to another file (file_path %q)", chunk.FilePath)
			}
			if chunk.CryptoMetadata.Value != nil || len(chunk.EncryptedColumnMetadata) > 0 {
				return errEncrypted
			}
		}
		for i, s := range rg.SortingColumns {
			if s.ColumnIdx >= 0 && int(s.ColumnIdx) < leaves {
				continue
			}
			if i == 0 && rg.NumRows > 0 {
				return refusePart(CodeMissingSortingColumns, name, g, "the first sorting column names no column (index %d)", s.ColumnIdx)
			}
			return bad(g, "sorting column %d names no column (index %d)", i, s.ColumnIdx)
		}
	}
	if total != meta.NumRows {
		return bad(-1, "the row groups hold %d rows, the footer %d", total, meta.NumRows)
	}
	return nil
}

// checkSchema checks that the schema elements form one tree in depth-first
// order, the root's children consuming every element, and that every leaf
// has a valid physical type and annotation. It returns the leaf count.
func checkSchema(schema []format.SchemaElement) (int, error) {
	if len(schema) == 0 {
		return 0, errors.New("the schema is empty")
	}
	pos, leaves := 1, 0
	var walk func(se *format.SchemaElement, depth int) error
	walk = func(se *format.SchemaElement, depth int) error {
		if depth > 64 {
			return errors.New("the schema is nested too deeply")
		}
		if rep, ok := se.RepetitionType.Get(); ok && (rep < format.Required || rep > format.Repeated) {
			return fmt.Errorf("column %q has an invalid repetition", se.Name)
		}
		children, isGroup := se.NumChildren.Get()
		if children < 0 {
			return fmt.Errorf("column %q has a negative child count", se.Name)
		}
		if !isGroup || children == 0 {
			leaves++
			return checkLeaf(se)
		}
		if se.Type.Valid {
			return fmt.Errorf("group %q has a physical type", se.Name)
		}
		for range children {
			if pos >= len(schema) {
				return errors.New("the schema is truncated")
			}
			child := &schema[pos]
			pos++
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	root := &schema[0]
	if n, ok := root.NumChildren.Get(); !ok || n <= 0 {
		return 0, errors.New("the schema root has no columns")
	}
	if err := walk(root, 0); err != nil {
		return 0, err
	}
	if pos != len(schema) {
		return 0, fmt.Errorf("the schema has %d elements outside its tree", len(schema)-pos)
	}
	return leaves, nil
}

// checkLeaf checks a leaf's physical type and that its annotation is one
// Parquet allows on that type.
func checkLeaf(se *format.SchemaElement) error {
	t, ok := se.Type.Get()
	if !ok || t < format.Boolean || t > format.FixedLenByteArray {
		return fmt.Errorf("leaf column %q has no valid physical type", se.Name)
	}
	length, hasLength := se.TypeLength.Get()
	if t == format.FixedLenByteArray && (!hasLength || length <= 0) {
		return fmt.Errorf("FIXED_LEN_BYTE_ARRAY column %q has no positive length", se.Name)
	}
	bad := func() error {
		return fmt.Errorf("column %q: annotation %s does not apply to %s", se.Name, annotation(se), t)
	}
	is := func(types ...format.Type) bool {
		for _, want := range types {
			if t == want {
				return true
			}
		}
		return false
	}
	decimalOK := func(precision, scale int32) bool {
		return is(format.Int32, format.Int64, format.ByteArray, format.FixedLenByteArray) && precision > 0 && scale >= 0 && scale <= precision
	}
	switch lt := se.LogicalType.Value.(type) {
	case nil:
	case *format.StringType, *format.EnumType, *format.JsonType, *format.BsonType, *format.GeometryType, *format.GeographyType:
		if !is(format.ByteArray) {
			return bad()
		}
	case *format.UUIDType:
		if !is(format.FixedLenByteArray) || length != 16 {
			return bad()
		}
	case *format.Float16Type:
		if !is(format.FixedLenByteArray) || length != 2 {
			return bad()
		}
	case *format.DecimalType:
		if !decimalOK(lt.Precision, lt.Scale) {
			return bad()
		}
	case *format.DateType:
		if !is(format.Int32) {
			return bad()
		}
	case *format.TimeType:
		_, millis := lt.Unit.Value.(*format.MilliSeconds)
		if lt.Unit.Value == nil || !(is(format.Int32) && millis || is(format.Int64) && !millis) {
			return bad()
		}
	case *format.TimestampType:
		if lt.Unit.Value == nil || !is(format.Int64) {
			return bad()
		}
	case *format.IntType:
		switch {
		case lt.BitWidth == 64 && is(format.Int64):
		case (lt.BitWidth == 8 || lt.BitWidth == 16 || lt.BitWidth == 32) && is(format.Int32):
		default:
			return bad()
		}
	case *format.NullType:
	default: // LIST, MAP, VARIANT and unknown annotations do not apply to a leaf
		return bad()
	}
	ct, ok := se.ConvertedType.Get()
	if !ok {
		return nil
	}
	switch ct {
	case deprecated.UTF8, deprecated.Enum, deprecated.Json, deprecated.Bson:
		if !is(format.ByteArray) {
			return bad()
		}
	case deprecated.Date, deprecated.TimeMillis, deprecated.Int8, deprecated.Int16, deprecated.Int32,
		deprecated.Uint8, deprecated.Uint16, deprecated.Uint32:
		if !is(format.Int32) {
			return bad()
		}
	case deprecated.TimeMicros, deprecated.TimestampMillis, deprecated.TimestampMicros, deprecated.Int64, deprecated.Uint64:
		if !is(format.Int64) {
			return bad()
		}
	case deprecated.Interval:
		if !is(format.FixedLenByteArray) || length != 12 {
			return bad()
		}
	case deprecated.Decimal:
		precision, _ := se.Precision.Get()
		scale, _ := se.Scale.Get()
		if !decimalOK(precision, scale) {
			return bad()
		}
	default: // MAP, LIST, MAP_KEY_VALUE and unknown values do not apply to a leaf
		return bad()
	}
	return nil
}

func annotation(se *format.SchemaElement) string {
	if se.LogicalType.Value != nil {
		return se.LogicalType.String()
	}
	if ct, ok := se.ConvertedType.Get(); ok {
		return "converted type " + strconv.Itoa(int(ct))
	}
	return "none"
}

// element is one top-level schema element with its subtree.
type element struct {
	se *format.SchemaElement
	// leaf is the leaf column index of a leaf element, -1 for a group.
	leaf int
	// signature spells the physical type (with a FIXED_LEN_BYTE_ARRAY's
	// length), repetition and nesting of the subtree (COL-5).
	signature string
}

// topLevel returns the top-level elements of a checked schema by name, and
// the names given twice.
func topLevel(schema []format.SchemaElement) (map[string]element, map[string]bool) {
	out := map[string]element{}
	dup := map[string]bool{}
	pos, leaf := 1, 0
	var walk func(b *strings.Builder)
	walk = func(b *strings.Builder) {
		se := &schema[pos]
		pos++
		if rep, ok := se.RepetitionType.Get(); ok {
			b.WriteString(strconv.Itoa(int(rep)))
		} else {
			b.WriteString("-")
		}
		if n, ok := se.NumChildren.Get(); ok && n > 0 {
			b.WriteString("{")
			for range n {
				b.WriteString(strconv.Quote(schema[pos].Name))
				walk(b)
				b.WriteString(";")
			}
			b.WriteString("}")
			return
		}
		t, _ := se.Type.Get()
		b.WriteString(":")
		b.WriteString(strconv.Itoa(int(t)))
		if t == format.FixedLenByteArray {
			b.WriteString("(")
			b.WriteString(strconv.Itoa(int(se.TypeLength.V)))
			b.WriteString(")")
		}
		leaf++
	}
	n, _ := schema[0].NumChildren.Get()
	for range n {
		se, first := &schema[pos], leaf
		var b strings.Builder
		walk(&b)
		e := element{se: se, leaf: -1, signature: b.String()}
		if c, ok := se.NumChildren.Get(); !ok || c == 0 {
			e.leaf = first
		}
		if _, seen := out[se.Name]; seen {
			dup[se.Name] = true
		}
		out[se.Name] = e
	}
	return out, dup
}

// checkKeyColumn applies KEY-1 and KEY-5 and reports whether the key is
// annotated as a string (KEY-6). leaves is the schema's leaf count.
func checkKeyColumn(name string, meta *format.FileMetaData, top map[string]element, dup map[string]bool, leaves int, keyColumn string) (utf8Key bool, err error) {
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidKeyColumn, name, -1, format, args...)
	}
	e, ok := top[keyColumn]
	if !ok || dup[keyColumn] {
		return false, bad("no single top-level column named %s", keyColumn)
	}
	se := e.se
	if e.leaf < 0 {
		return false, bad("the %s column is nested", keyColumn)
	}
	if t, ok := se.Type.Get(); !ok || t != format.ByteArray {
		return false, bad("the %s column is not BYTE_ARRAY", keyColumn)
	}
	if rep, ok := se.RepetitionType.Get(); !ok || rep != format.Required {
		return false, bad("the %s column is not REQUIRED", keyColumn)
	}
	switch se.LogicalType.Value.(type) {
	case nil:
	case *format.StringType:
		utf8Key = true
	default:
		return false, bad("the %s column is annotated %s", keyColumn, se.LogicalType.String())
	}
	if ct, ok := se.ConvertedType.Get(); ok {
		if ct != deprecated.UTF8 {
			return false, bad("the %s column has converted type %d", keyColumn, ct)
		}
		utf8Key = true
	}
	if len(meta.ColumnOrders) != leaves {
		return false, bad("column_orders has %d entries for %d leaf columns", len(meta.ColumnOrders), leaves)
	}
	if _, ok := meta.ColumnOrders[e.leaf].Value.(*format.TypeDefinedOrder); !ok {
		return false, bad("column_orders does not give the %s column TYPE_ORDER", keyColumn)
	}
	return utf8Key, nil
}

// leafCount returns the number of leaves of a checked schema.
func leafCount(schema []format.SchemaElement) int {
	n := 0
	for i := 1; i < len(schema); i++ {
		if c, ok := schema[i].NumChildren.Get(); !ok || c == 0 {
			n++
		}
	}
	return n
}
