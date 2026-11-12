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
	"slices"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/thriftcheck"
)

// isEnvironment reports whether err comes from the context, Close, a limit
// or the source rather than from the data.
func isEnvironment(ctx context.Context, err error) bool {
	var se *storageError
	return ctx.Err() != nil || errors.As(err, &se) || errors.Is(err, ErrClosed) || errors.Is(err, ErrLimitExceeded) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// readFull reads len(p) bytes at off, reporting a short read of the data
// as io.ErrUnexpectedEOF.
//
// A complete read succeeds only with a nil error or one that says no more
// than io.EOF (onlyEOF): any other error
// invalidates the bytes, even when all of them arrived. Through a
// storageReader a short read is already the environment's error.
func readFull(at io.ReaderAt, p []byte, off int64) error {
	if s, ok := at.(storageReader); ok {
		return s.readFull(p, off)
	}
	n, err := at.ReadAt(p, off)
	switch {
	case err != nil && !onlyEOF(err):
		return err
	case n == len(p):
		return nil
	}
	return io.ErrUnexpectedEOF
}

// errEncrypted reports an encrypted part (FILE-2): a new error each time,
// so that no caller can change what another read reports.
func errEncrypted() *UnsupportedError {
	return &UnsupportedError{Code: UnsupportedEncryption, Detail: "the part uses Parquet modular encryption"}
}

// prefetched serves reads that fall inside data, the bytes at off, or
// inside head, the file's first bytes, from memory, and every other read
// from r. parquet-go keeps it with the footer, so r counts in no
// operation's statistics.
type prefetched struct {
	r    storageReader
	off  int64
	data []byte
	head []byte
}

func (p prefetched) ReadAt(b []byte, off int64) (int, error) {
	if off >= p.off && off-p.off <= int64(len(p.data)) && int64(len(b)) <= int64(len(p.data))-(off-p.off) {
		return copy(b, p.data[off-p.off:]), nil
	}
	if off >= 0 && off <= int64(len(p.head)) && int64(len(b)) <= int64(len(p.head))-off {
		return copy(b, p.head[off:]), nil
	}
	return p.r.ReadAt(b, off)
}

// footerWindow is the most bytes readFooter reads speculatively from a
// part's end, so that a footer up to that size takes one read with its
// trailer. A larger footer takes one more read, of the bytes before the
// window.
const footerWindow = 64 << 10

// testFooterRetained, when set by a test, receives the reader readFooter
// gives parquet-go to keep, and the window it read the file's end into.
var testFooterRetained func(retained prefetched, window []byte)

// ownCopy returns a copy of b in an allocation of its own, whose capacity
// is its length.
func ownCopy(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// decodeBounded decodes one thrift struct into v from raw after
// thriftcheck has shown that its lengths fit and its decoded size is
// within budget. It returns the number of bytes the struct occupies.
func decodeBounded(raw []byte, v any, budget int64) (int, error) {
	n, _, err := thriftcheck.Check(raw, reflect.TypeOf(v), budget)
	if errors.Is(err, thriftcheck.ErrBudget) {
		return 0, exceeded(LimitMetadataMemory, -1, budget, "decoding metadata")
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
// *UnsupportedError; a limit or storage failure an error. Its reads are
// counted in st, which may be nil.
//
// The file's end is read in one window of up to maxWindow bytes (at least
// its 8-byte trailer, at most the largest footer Limits.FooterBytes admits
// with its trailer): it holds the trailer and, when the footer fits, the
// footer, so that a footer within the window costs one read besides the
// head magic, and a larger one a second, of the bytes before the window. A
// file no larger than the window is read whole, its head with it, in one
// read. Otherwise the head magic is read first, as its own 4 bytes. A
// maxWindow of 0 reads the trailer alone, then the footer: the file's head
// and footer, nothing else (InspectFooter). Nothing is allocated from the
// declared footer length before it is checked.
func readFooter(ctx context.Context, name string, file io.ReaderAt, size int64, lim *Limits, st *ReadStats, maxWindow int64, concurrency int, deferHead bool) (_ *parquet.File, tail footerTail, err error) {
	reports := reportsIO(file)
	at := storageReader{r: bind(ctx, file), op: operationOf(ctx), st: st, name: name, unreported: unreported(ctx, reports)}
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidFooter, name, -1, format, args...)
	}
	if size < 12 {
		return nil, tail, bad("the file is too short to be Parquet")
	}
	window := min(size, max(maxWindow, 8))
	if lim.FooterBytes < window-8 {
		window = max(lim.FooterBytes, 0) + 8
	}
	win := make([]byte, window)
	var head []byte
	if window < size && deferHead {
		// The caller establishes the head later (checkPart): the window
		// alone is read now, the head taken to be PAR1 until then, and
		// every verdict reached before then judged again with the real
		// head (magicFirst).
		tail.headDeferred = true
		if err := at.readFull(win, size-window); err != nil {
			return nil, tail, footerError(ctx, name, err)
		}
		head = []byte("PAR1")
	} else if window < size {
		// The head magic and the window are read together; a failure of
		// the head is reported first, as when it was read first.
		head = make([]byte, 4)
		ranges := []ReadRange{{Off: 0, Buf: head}, {Off: size - window, Buf: win}}
		for _, err := range readRanges(ctx, rangeFile{file: file, name: name, reports: reports, concurrency: concurrency}, ranges, st) {
			if err != nil {
				return nil, tail, footerError(ctx, name, err)
			}
		}
	} else if err := at.readFull(win, size-window); err != nil {
		return nil, tail, footerError(ctx, name, err)
	}
	tail.data, tail.off = win, size-window
	if head == nil {
		head = win[:4:4]
	}
	trailer := win[window-8:]
	if string(head) == "PARE" || string(trailer[4:]) == "PARE" {
		return nil, tail, errEncrypted()
	}
	if string(head) != "PAR1" || string(trailer[4:]) != "PAR1" {
		return nil, tail, bad("the file does not begin and end with PAR1")
	}
	n := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if n > size-12 {
		return nil, tail, bad("the footer length %d does not fit in the file", n)
	}
	if n > lim.FooterBytes {
		return nil, tail, exceeded(LimitFooterBytes, n, lim.FooterBytes, "part %s: the footer", name)
	}
	var raw []byte
	if n+8 <= window {
		raw = win[window-8-n:]
	} else {
		// The footer starts before the window: read the bytes before it.
		raw = make([]byte, n+8)
		copy(raw[n+8-window:], win)
		if err := at.readFull(raw[:n+8-window], size-8-n); err != nil {
			return nil, tail, footerError(ctx, name, err)
		}
	}
	var meta format.FileMetaData
	used, err := decodeBounded(raw[:n], &meta, lim.MetadataMemory)
	switch {
	case errors.Is(err, ErrLimitExceeded):
		return nil, tail, fmt.Errorf("part %s: footer: %w", name, err)
	case err != nil:
		return nil, tail, bad("decode the footer: %v", err)
	case int64(used) == n-28 && (meta.EncryptionAlgorithm.Value != nil || meta.FooterSigningKeyMetadata != nil):
		return nil, tail, errEncrypted() // a plaintext footer declaring encryption, then its 28-byte signature
	case int64(used) != n:
		return nil, tail, bad("the footer has %d bytes after its metadata", n-int64(used))
	}
	if err := checkMetadata(name, &meta); err != nil {
		return nil, tail, err
	}
	// parquet-go decodes the footer again from the bytes it reads. When the
	// checked footer holds what the dependency would refuse or mishandle
	// but the format ignores, it gets a re-encoded footer without it, at
	// the same offset: everything before the footer is read as stored.
	footer, end := raw, size
	if forParquetGo(&meta) {
		encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
		if err != nil {
			return nil, tail, bad("re-encode the footer: %v", err)
		}
		footer = binary.LittleEndian.AppendUint32(encoded, uint32(len(encoded)))
		footer = append(footer, "PAR1"...)
		end = size - 8 - n + int64(len(footer))
	}
	// The opened file keeps the bytes it is given for as long as the part
	// is open: exactly the footer with its trailer and the head magic, in
	// slices of their own, so that the speculative window is not pinned
	// with them.
	footer, head = ownCopy(footer), ownCopy(head)
	defer func() {
		if r := recover(); r != nil {
			err = bad("parquet-go failed on the footer: %v", r)
		}
	}()
	// parquet-go reads the head magic and the footer again: both are served
	// from the bytes checked above, not read from the source a second time.
	retained := prefetched{r: storageReader{r: at.r, name: name, unreported: at.unreported}, off: size - 8 - n, data: footer, head: head}
	if testFooterRetained != nil {
		testFooterRetained(retained, win)
	}
	f, err := parquet.OpenFile(retained, end,
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true), parquet.ReadBufferSize(4096))
	if err != nil {
		return nil, tail, footerError(ctx, name, err)
	}
	tail.off, tail.footerStart = size-window, size-8-n
	return f, tail, nil
}

// footerTail is the end of a part that readFooter read: window data at
// off, and where the footer begins in the file.
type footerTail struct {
	off, footerStart int64
	data             []byte
	// headDeferred is set when the head was not read (deferHead).
	headDeferred bool
}

// footerError classifies an error reading a footer: the environment's own
// error, or the refusal of a part the source found missing or of another
// size (sourceError), as is; anything else invalid_footer.
func footerError(ctx context.Context, name string, err error) error {
	var unsupported *UnsupportedError
	var refusal *RefusalError
	if isEnvironment(ctx, err) || errors.As(err, &unsupported) || errors.As(err, &refusal) {
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
	if meta.EncryptionAlgorithm.Value != nil || meta.FooterSigningKeyMetadata != nil {
		return errEncrypted()
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
		if len(rg.Columns) != len(leaves) {
			return bad(g, "the row group has %d column chunks for %d leaf columns", len(rg.Columns), len(leaves))
		}
		for c := range rg.Columns {
			chunk := &rg.Columns[c]
			if chunk.FilePath != "" {
				return bad(g, "a column chunk refers to another file (file_path %q)", chunk.FilePath)
			}
			if chunk.CryptoMetadata.Value != nil || len(chunk.EncryptedColumnMetadata) > 0 {
				return errEncrypted()
			}
			// The chunk's metadata describes the leaf it holds: the reader
			// decodes by the schema's type, so a chunk claiming another is
			// not this column's data.
			if md := &chunk.MetaData; md.Type != leaves[c].typ || !slices.Equal(md.PathInSchema, leaves[c].path) {
				return bad(g, "column chunk %d (%s %s) does not describe its schema leaf (%s %s)", c,
					strings.Join(md.PathInSchema, "."), md.Type, strings.Join(leaves[c].path, "."), leaves[c].typ)
			}
		}
		// RG-1: only the first entry matters; an entry after it that names
		// no column is ignored, and forParquetGo drops it before the
		// dependency, which would index out of range, sees it.
		if len(rg.SortingColumns) > 0 && rg.NumRows > 0 {
			if s := rg.SortingColumns[0]; s.ColumnIdx < 0 || int(s.ColumnIdx) >= len(leaves) {
				return refusePart(CodeMissingSortingColumns, name, g, "the first sorting column names no column (index %d)", s.ColumnIdx)
			}
		}
	}
	if total != meta.NumRows {
		return bad(-1, "the row groups hold %d rows, the footer %d", total, meta.NumRows)
	}
	return nil
}

// schemaLeaf is a leaf column of a checked schema: its path from the root
// (the root's name excluded) and its physical type.
type schemaLeaf struct {
	path []string
	typ  format.Type
}

// checkSchema checks that the schema elements form one tree in depth-first
// order, the root's children consuming every element, and that every leaf
// has a valid physical type and annotation. It returns the leaves in order.
func checkSchema(schema []format.SchemaElement) ([]schemaLeaf, error) {
	if len(schema) == 0 {
		return nil, errors.New("the schema is empty")
	}
	pos := 1
	var leaves []schemaLeaf
	var path []string
	var walk func(se *format.SchemaElement, depth int) error
	walk = func(se *format.SchemaElement, depth int) error {
		if depth > 0 {
			path = append(path, se.Name)
			defer func() { path = path[:len(path)-1] }()
		}
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
			leaves = append(leaves, schemaLeaf{path: slices.Clone(path), typ: se.Type.V})
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
		return nil, errors.New("the schema root has no columns")
	}
	if err := walk(root, 0); err != nil {
		return nil, err
	}
	if pos != len(schema) {
		return nil, fmt.Errorf("the schema has %d elements outside its tree", len(schema)-pos)
	}
	return leaves, nil
}

// forParquetGo removes, from a checked footer, what the format lets a
// reader ignore but parquet-go refuses or mishandles, and reports whether
// it changed anything:
//   - sorting_columns entries that name no column, other than a non-empty
//     row group's first (refused by checkMetadata): RG-1 ignores entries
//     after the first, and parquet-go would index out of range on them;
//   - row-group ordinals that are set and differ from the row groups'
//     positions: they serve encryption only (FILE-2), and parquet-go's
//     OpenFile refuses them. They are cleared, which parquet-go reads as
//     omitted.
func forParquetGo(meta *format.FileMetaData) bool {
	changed := false
	leaves := 0
	for _, se := range meta.Schema[1:] {
		if n, ok := se.NumChildren.Get(); !ok || n == 0 {
			leaves++
		}
	}
	allZero, allPositions := true, true // what parquet-go accepts
	for g := range meta.RowGroups {
		rg := &meta.RowGroups[g]
		kept := rg.SortingColumns[:0:0]
		for i, s := range rg.SortingColumns {
			if s.ColumnIdx >= 0 && int(s.ColumnIdx) < leaves || i == 0 && rg.NumRows > 0 {
				kept = append(kept, s)
			}
		}
		if len(kept) != len(rg.SortingColumns) {
			rg.SortingColumns, changed = kept, true
		}
		allZero = allZero && rg.Ordinal == 0
		allPositions = allPositions && int(rg.Ordinal) == g
	}
	if !allZero && !allPositions {
		for g := range meta.RowGroups {
			meta.RowGroups[g].Ordinal = 0
		}
		changed = true
	}
	return changed
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

// columnsOf returns the top-level columns of a checked schema in schema
// order, keyColumn marked as the key.
func columnsOf(schema []format.SchemaElement, keyColumn string) []Column {
	n, _ := schema[0].NumChildren.Get()
	out := make([]Column, 0, n)
	var skip func(pos int) int
	skip = func(pos int) int {
		children, _ := schema[pos].NumChildren.Get()
		pos++
		for range children {
			pos = skip(pos)
		}
		return pos
	}
	for pos := 1; len(out) < int(n); pos = skip(pos) {
		out = append(out, columnOf(&schema[pos], keyColumn))
	}
	return out
}

// columnOf describes one top-level schema element.
func columnOf(se *format.SchemaElement, keyColumn string) Column {
	c := Column{Name: se.Name, Key: se.Name == keyColumn}
	repetition, _ := se.RepetitionType.Get()
	c.Repetition = Repetition(repetition)
	if children, ok := se.NumChildren.Get(); ok && children > 0 {
		c.Group = true
		return c
	}
	typ, _ := se.Type.Get()
	c.Type = Kind(typ + 1)
	if typ == format.FixedLenByteArray {
		c.Length = se.TypeLength.V
	}
	if v := se.LogicalType.Value; v != nil {
		c.Logical = logicalOf(v)
	} else if ct, ok := se.ConvertedType.Get(); ok {
		c.Logical = convertedLogical(ct, se)
	}
	return c
}

// convertedLogical returns the logical type a leaf's legacy converted_type
// stands for, as Parquet's LogicalTypes.md maps it (time and timestamp
// adjusted to UTC), or none for INTERVAL, which has none, and for the
// converted types of groups.
func convertedLogical(ct deprecated.ConvertedType, se *format.SchemaElement) Logical {
	switch ct {
	case deprecated.UTF8:
		return Logical{Kind: LogicalString}
	case deprecated.Enum:
		return Logical{Kind: LogicalEnum}
	case deprecated.Json:
		return Logical{Kind: LogicalJSON}
	case deprecated.Bson:
		return Logical{Kind: LogicalBSON}
	case deprecated.Date:
		return Logical{Kind: LogicalDate}
	case deprecated.TimeMillis:
		return Logical{Kind: LogicalTime, AdjustedToUTC: true, Unit: Millis}
	case deprecated.TimeMicros:
		return Logical{Kind: LogicalTime, AdjustedToUTC: true, Unit: Micros}
	case deprecated.TimestampMillis:
		return Logical{Kind: LogicalTimestamp, AdjustedToUTC: true, Unit: Millis}
	case deprecated.TimestampMicros:
		return Logical{Kind: LogicalTimestamp, AdjustedToUTC: true, Unit: Micros}
	case deprecated.Int8, deprecated.Int16, deprecated.Int32, deprecated.Int64:
		return Logical{Kind: LogicalInt, BitWidth: int8(8 << (ct - deprecated.Int8)), Signed: true}
	case deprecated.Uint8, deprecated.Uint16, deprecated.Uint32, deprecated.Uint64:
		return Logical{Kind: LogicalInt, BitWidth: int8(8 << (ct - deprecated.Uint8))}
	case deprecated.Decimal:
		return Logical{Kind: LogicalDecimal, Precision: se.Precision.V, Scale: se.Scale.V}
	}
	return Logical{}
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

// magicFirst judges again, with the part's real head, an error reached
// while its head was deferred (readFooter's deferHead): every verdict on
// the data, and every limit, came after the head magic was checked when
// the head was read first, so the head's verdict comes first. The error
// of the environment or the context is kept as is.
func magicFirst(ctx context.Context, name string, file io.ReaderAt, size int64, tail footerTail, st *ReadStats, err error) error {
	var se *storageError
	if ctx.Err() != nil || errors.As(err, &se) || errors.Is(err, ErrClosed) {
		return err
	}
	at := storageReader{r: bind(ctx, file), op: operationOf(ctx), st: st, name: name, unreported: unreported(ctx, reportsIO(file))}
	head := make([]byte, 4)
	if e := at.readFull(head, 0); e != nil {
		return footerError(ctx, name, e)
	}
	return headVerdict(ctx, name, at, size, head, tail, err)
}

// headVerdict returns readFooter's verdict on a part's magic given its
// head, or err when the head is PAR1. The end's magic is taken from tail,
// or read when tail lacks it.
func headVerdict(ctx context.Context, name string, at storageReader, size int64, head []byte, tail footerTail, err error) error {
	if string(head) == "PAR1" {
		return err
	}
	end := []byte(nil)
	if len(tail.data) >= 4 && tail.off+int64(len(tail.data)) == size {
		end = tail.data[len(tail.data)-4:]
	} else {
		end = make([]byte, 4)
		if e := at.readFull(end, size-4); e != nil {
			return footerError(ctx, name, e)
		}
	}
	if string(head) == "PARE" || string(end) == "PARE" {
		return errEncrypted()
	}
	return refusePart(CodeInvalidFooter, name, -1, "the file does not begin and end with PAR1")
}
