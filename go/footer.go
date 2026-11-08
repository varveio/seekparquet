// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
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
type storageReader struct{ r io.ReaderAt }

func (s storageReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := s.r.ReadAt(p, off)
	if err != nil && !errors.Is(err, io.EOF) {
		err = &storageError{err}
	}
	return n, err
}

// isEnvironment reports whether err comes from the context, Close or the
// source rather than from the data.
func isEnvironment(ctx context.Context, err error) bool {
	var se *storageError
	return ctx.Err() != nil || errors.As(err, &se) || errors.Is(err, ErrClosed) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// errEncrypted reports an encrypted part (FILE-2).
var errEncrypted = &UnsupportedError{Code: UnsupportedEncryption, Detail: "the part uses Parquet modular encryption"}

// readFooter parses a part's footer, without its page indexes or bloom
// filters, after checking that the footer length the file declares fits
// in the file (so a hostile length cannot make it allocate gigabytes). An
// encrypted part is an *UnsupportedError (FILE-2).
func readFooter(ctx context.Context, file io.ReaderAt, size int64) (*parquet.File, error) {
	at := storageReader{bind(ctx, file)}
	if size < 12 {
		return nil, errors.New("the file is too short to be Parquet")
	}
	var tail [8]byte
	if n, err := at.ReadAt(tail[:], size-8); n < len(tail) {
		return nil, fmt.Errorf("read the footer length: %w", err)
	}
	switch string(tail[4:]) {
	case "PAR1":
	case "PARE":
		return nil, errEncrypted
	default:
		return nil, errors.New("the file does not end with PAR1")
	}
	if n := int64(binary.LittleEndian.Uint32(tail[:4])); n > size-12 {
		return nil, fmt.Errorf("the footer length %d does not fit in the file", n)
	}
	f, err := parquet.OpenFile(at, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true), parquet.ReadBufferSize(64<<10))
	if err != nil {
		if strings.Contains(err.Error(), "DecryptionConfig") {
			return nil, errEncrypted // a signed plaintext footer
		}
		return nil, err
	}
	meta := f.Metadata()
	if meta.EncryptionAlgorithm.Value != nil || len(meta.FooterSigningKeyMetadata) > 0 {
		return nil, errEncrypted
	}
	return f, nil
}

// footerError classifies an error reading a footer: the environment's own
// error or an *UnsupportedError as is, anything else invalid_footer.
func footerError(ctx context.Context, name string, err error) error {
	var unsupported *UnsupportedError
	if isEnvironment(ctx, err) || errors.As(err, &unsupported) {
		return err
	}
	return refusePart(CodeInvalidFooter, name, -1, "%v", err)
}

// checkSelfContained applies FILE-1 (no column chunk refers to another
// file) and FILE-2 (no encrypted column) before any page is followed.
func checkSelfContained(name string, meta *format.FileMetaData) error {
	for g := range meta.RowGroups {
		for c := range meta.RowGroups[g].Columns {
			chunk := &meta.RowGroups[g].Columns[c]
			if chunk.FilePath != "" {
				return refusePart(CodeInvalidFooter, name, g, "a column chunk refers to another file (file_path %q)", chunk.FilePath)
			}
			if chunk.CryptoMetadata.Value != nil || len(chunk.EncryptedColumnMetadata) > 0 {
				return errEncrypted
			}
		}
	}
	return nil
}

// element is one top-level schema element with its subtree.
type element struct {
	se *format.SchemaElement
	// leaf is the leaf column index of a leaf element, -1 for a group.
	leaf int
	// signature spells the physical type, repetition and nesting of the
	// subtree (COL-5).
	signature string
}

// topLevel returns the top-level elements of a footer's schema by name,
// and the names given twice.
func topLevel(schema []format.SchemaElement) (map[string]element, map[string]bool, error) {
	if len(schema) == 0 {
		return nil, nil, errors.New("the schema is empty")
	}
	out := map[string]element{}
	dup := map[string]bool{}
	pos, leaf := 1, 0
	var walk func() (string, error)
	walk = func() (string, error) {
		if pos >= len(schema) {
			return "", errors.New("the schema is truncated")
		}
		se := &schema[pos]
		pos++
		var b strings.Builder
		if rep, ok := se.RepetitionType.Get(); ok {
			b.WriteString(strconv.Itoa(int(rep)))
		} else {
			b.WriteString("-")
		}
		if n, ok := se.NumChildren.Get(); ok && n > 0 {
			b.WriteString("{")
			for range n {
				b.WriteString(strconv.Quote(schema[min(pos, len(schema)-1)].Name))
				sub, err := walk()
				if err != nil {
					return "", err
				}
				b.WriteString(sub)
				b.WriteString(";")
			}
			b.WriteString("}")
			return b.String(), nil
		}
		t, _ := se.Type.Get()
		b.WriteString(":")
		b.WriteString(strconv.Itoa(int(t)))
		leaf++
		return b.String(), nil
	}
	n, _ := schema[0].NumChildren.Get()
	for range n {
		if pos >= len(schema) {
			return nil, nil, errors.New("the schema is truncated")
		}
		se, first := &schema[pos], leaf
		sig, err := walk()
		if err != nil {
			return nil, nil, err
		}
		e := element{se: se, leaf: -1, signature: sig}
		if c, ok := se.NumChildren.Get(); !ok || c == 0 {
			e.leaf = first
		}
		if _, seen := out[se.Name]; seen {
			dup[se.Name] = true
		}
		out[se.Name] = e
	}
	return out, dup, nil
}

// checkKeyColumn applies KEY-1 and KEY-5 and reports whether the key is
// annotated as a string (KEY-6).
func checkKeyColumn(name string, meta *format.FileMetaData, top map[string]element, dup map[string]bool) (utf8Key bool, err error) {
	bad := func(format string, args ...any) error {
		return refusePart(CodeInvalidKeyColumn, name, -1, format, args...)
	}
	e, ok := top[KeyColumn]
	if !ok || dup[KeyColumn] {
		return false, bad("no single top-level column named %s", KeyColumn)
	}
	se := e.se
	if e.leaf < 0 {
		return false, bad("the %s column is nested", KeyColumn)
	}
	if t, ok := se.Type.Get(); !ok || t != format.ByteArray {
		return false, bad("the %s column is not BYTE_ARRAY", KeyColumn)
	}
	if rep, ok := se.RepetitionType.Get(); !ok || rep != format.Required {
		return false, bad("the %s column is not REQUIRED", KeyColumn)
	}
	switch se.LogicalType.Value.(type) {
	case nil:
	case *format.StringType:
		utf8Key = true
	default:
		return false, bad("the %s column is annotated %s", KeyColumn, se.LogicalType.String())
	}
	if ct, ok := se.ConvertedType.Get(); ok {
		if ct != deprecated.UTF8 {
			return false, bad("the %s column has converted type %d", KeyColumn, ct)
		}
		utf8Key = true
	}
	if e.leaf >= len(meta.ColumnOrders) {
		return false, bad("column_orders does not cover the %s column", KeyColumn)
	}
	if _, ok := meta.ColumnOrders[e.leaf].Value.(*format.TypeDefinedOrder); !ok {
		return false, bad("column_orders does not give the %s column TYPE_ORDER", KeyColumn)
	}
	return utf8Key, nil
}
