// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// readHookSource wraps a Source: before each read of its files (bound or not)
// it calls read with the file's path, the read's length and offset, and
// fails the read with its error; it counts the reads in flight and the
// most at once. sized adds OpenSized, opening with Open and claiming the
// expected size.
type readHookSource struct {
	Source
	read           func(path string, n int, off int64) error
	inFlight, most atomic.Int64
}

type readHookFile struct {
	SourceFile
	s    *readHookSource
	path string
}

func (s *readHookSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return readHookFile{f, s, path}, nil
}

func (f readHookFile) ReadAt(p []byte, off int64) (int, error) { return f.readAt(p, off) }

func (f readHookFile) WithContext(context.Context) io.ReaderAt { return readerFunc(f.readAt) }

func (f readHookFile) readAt(p []byte, off int64) (int, error) {
	n := f.s.inFlight.Add(1)
	defer f.s.inFlight.Add(-1)
	for {
		m := f.s.most.Load()
		if n <= m || f.s.most.CompareAndSwap(m, n) {
			break
		}
	}
	if f.s.read != nil {
		if err := f.s.read(f.path, len(p), off); err != nil {
			return 0, err
		}
	}
	return f.SourceFile.ReadAt(p, off)
}

// sizedHookSource is readHookSource with OpenSized: it opens with Open and
// returns a file that claims the expected size, proving nothing.
type sizedHookSource struct{ *readHookSource }

func (s sizedHookSource) OpenSized(c context.Context, path string, size int64) (SourceFile, error) {
	f, err := s.readHookSource.Open(c, path)
	if err != nil {
		return nil, err
	}
	return claimedSize{f, size}, nil
}

type claimedSize struct {
	SourceFile
	size int64
}

func (f claimedSize) Size() int64 { return f.size }

func (f claimedSize) WithContext(c context.Context) io.ReaderAt {
	return f.SourceFile.(ContextBinder).WithContext(c)
}

// TestStorageReadClassification: a read that returned every byte with an
// error fails; a short read is the environment's error, never data; a
// broken pin stays one whatever it also wraps; a batch member's short read
// is the environment's too.
func TestStorageReadClassification(t *testing.T) {
	data := []byte("0123456789")
	reader := func(n int, err error) io.ReaderAt {
		return readerFunc(func(p []byte, off int64) (int, error) { return copy(p[:n], data[off:]), err })
	}
	read := func(r io.ReaderAt, want int) error {
		return storageReader{r: r, name: "p"}.readFull(make([]byte, want), 0)
	}
	var se *storageError
	changed := fmt.Errorf("pin: %w %w", ErrObjectChanged, io.EOF)
	resized := errors.Join(io.EOF, &SizeMismatchError{Expected: 10, Actual: 7})
	for _, tc := range []struct {
		name   string
		r      io.ReaderAt
		env    bool // the environment's error
		ok     bool
		target error
		code   RefusalCode // a refusal instead
	}{
		{"whole", reader(5, nil), false, true, nil, ""},
		{"whole with EOF", reader(5, io.EOF), false, true, nil, ""},
		{"whole with a wrapped EOF", reader(5, fmt.Errorf("read: %w", io.EOF)), false, true, nil, ""},
		{"whole with EOF joined to EOF", reader(5, errors.Join(io.EOF, io.EOF)), false, true, nil, ""},
		{"whole with an error", reader(5, errBoom), true, false, errBoom, ""},
		{"whole with a broken pin", reader(5, changed), true, false, ErrObjectChanged, ""},
		{"whole with EOF and an error", reader(5, errors.Join(io.EOF, errBoom)), true, false, errBoom, ""},
		{"whole with EOF and cancellation", reader(5, errors.Join(io.EOF, context.Canceled)), true, false, context.Canceled, ""},
		{"whole with EOF and a size mismatch", reader(5, resized), false, false, nil, CodePartMismatch},
		{"short", reader(3, nil), true, false, io.ErrUnexpectedEOF, ""},
		{"short with EOF", reader(3, io.EOF), true, false, io.ErrUnexpectedEOF, ""},
		{"short with EOF and an error", reader(3, errors.Join(io.EOF, errBoom)), true, false, errBoom, ""},
		{"short with EOF and a size mismatch", reader(3, resized), false, false, nil, CodePartMismatch},
	} {
		err := read(tc.r, 5)
		switch {
		case tc.ok && err != nil, !tc.ok && err == nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.env && (!errors.As(err, &se) || RefusalCodeOf(err) != "" || !errors.Is(err, tc.target)):
			t.Errorf("%s: %v, want the environment's %v", tc.name, err, tc.target)
		case tc.code != "" && RefusalCodeOf(err) != tc.code:
			t.Errorf("%s: %v, want %s", tc.name, err, tc.code)
		}
		// Through a plain reader the same outcomes stand, unclassified.
		if err := readFull(tc.r, make([]byte, 5), 0); (err == nil) != tc.ok || tc.target != nil && !errors.Is(err, tc.target) {
			t.Errorf("%s: readFull: %v", tc.name, err)
		}
	}
	for _, e := range []error{io.ErrUnexpectedEOF, io.EOF, fmt.Errorf("%w: %w", ErrObjectChanged, io.ErrUnexpectedEOF)} {
		got := batchError(context.Background(), "p", e)
		if !errors.As(got, &se) || RefusalCodeOf(got) != "" || !errors.Is(got, e) {
			t.Errorf("batch member %v: %v", e, got)
		}
	}
	if got := batchError(context.Background(), "p", fmt.Errorf("%w: %w", ErrObjectChanged, fs.ErrNotExist)); RefusalCodeOf(got) != "" || !errors.Is(got, ErrObjectChanged) {
		t.Errorf("a broken pin of a gone object: %v", got)
	}
}

// changedOpenSource fails every Open with an error wrapping both a broken
// pin and fs.ErrNotExist; with manifest it is a ManifestReader failing so.
type changedOpenSource struct{}

var errGonePin = fmt.Errorf("%w: the pinned version is gone: %w", ErrObjectChanged, fs.ErrNotExist)

func (changedOpenSource) Open(context.Context, string) (SourceFile, error) { return nil, errGonePin }

type changedManifestSource struct{ changedOpenSource }

func (changedManifestSource) ReadManifest(context.Context, int64) ([]byte, string, error) {
	return nil, "", errGonePin
}

// TestOpenClassification: a manifest whose pin broke is the environment's
// error, not no_manifest, through Open and through ReadManifest alike;
// OpenDir resets OpenStats and checks its options before any I/O.
func TestOpenClassification(t *testing.T) {
	for _, src := range []Source{changedOpenSource{}, changedManifestSource{}} {
		_, err := OpenDataset(ctx, src, OpenOptions{})
		if !errors.Is(err, ErrObjectChanged) || RefusalCodeOf(err) != "" {
			t.Errorf("%T: %v", src, err)
		}
	}
	st := ReadStats{SourceReads: 7}
	if _, err := OpenDir(ctx, "/nonexistent-dataset", OpenOptions{OpenStats: &st}); err == nil || st != (ReadStats{}) {
		t.Fatalf("OpenDir of a missing directory: %v, stats %+v", err, st)
	}
	if _, err := OpenDir(ctx, "/nonexistent-dataset", OpenOptions{Read: ReadPolicy{Concurrency: -1}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("OpenDir with an invalid policy: %v", err)
	}
}

// TestSizedTinyPart: a part whose manifest declares fewer than 12 bytes is
// opened with Open, so that a sized source refuses it as an ordinary one
// does: missing_part when it is absent, part_mismatch when it is larger.
func TestSizedTinyPart(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	editManifest(t, dir, func(m map[string]any) {
		m["parts"].([]any)[0].(map[string]any)["bytes"] = 8
	})
	for _, src := range []Source{&readHookSource{Source: dirOf(t, dir)}, sizedHookSource{&readHookSource{Source: dirOf(t, dir)}}} {
		d, err := OpenDataset(ctx, src, OpenOptions{})
		must(t, err)
		_, err = d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte("a"))}, Limit: 1}, nil)
		if RefusalCodeOf(err) != CodePartMismatch {
			t.Errorf("%T: %v, want part_mismatch", src, err)
		}
		must(t, d.Close())
	}
}

// TestVerifyHostileIndexLength: a key OffsetIndex with a negative length
// is refused by Verify, and again by a second Verify, without a panic.
func TestVerifyHostileIndexLength(t *testing.T) {
	keys := fixtureKeys(600)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 2)...))
	rewrite(t, dir, 1, func(m *format.FileMetaData) { m.RowGroups[1].Columns[0].OffsetIndexLength = -1 })
	d := open(t, dir, OpenOptions{Read: ReadPolicy{Concurrency: 4}})
	first := d.Verify(ctx, nil)
	if first == nil {
		t.Fatal("a negative index length was accepted")
	}
	if again := d.Verify(ctx, nil); refusalOf(again) != refusalOf(first) {
		t.Fatalf("again: %v, first %v", again, first)
	}
}

// TestVerifyMergedHeadFailure: when the range of a part's head merged with
// its first key page fails but its head alone reads, Verify reports the
// refusal a serial check reaches first, not the failed range.
func TestVerifyMergedHeadFailure(t *testing.T) {
	keys := fixtureKeys(2400)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
	rewrite(t, dir, 2, func(m *format.FileMetaData) {
		for i := range m.Schema {
			if m.Schema[i].Name == "key" {
				m.Schema[i].RepetitionType.V, m.Schema[i].RepetitionType.Valid = format.Optional, true
			}
		}
	})
	want := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}).Verify(ctx, nil)
	if RefusalCodeOf(want) == "" {
		t.Fatalf("the broken key column: %v", want)
	}
	var merged atomic.Int64
	src := &readHookSource{Source: dirOf(t, dir), read: func(path string, n int, off int64) error {
		if off == 0 && n > 4 && path == testutil.PartPath(2) {
			merged.Add(1)
			return errBoom
		}
		return nil
	}}
	d, err := OpenDataset(ctx, src, nearWindow(t, dir, 4))
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Verify(ctx, nil); refusalOf(err) != refusalOf(want) || merged.Load() != 1 {
		t.Fatalf("Verify: %v, want %v (%d merged reads failed)", err, want, merged.Load())
	}
}

// nearWindow returns options reading a part's end in a window just short
// of the smallest of dir's n parts: it holds every part's footer and page
// indexes, not its head.
func nearWindow(t *testing.T, dir string, n int) OpenOptions {
	t.Helper()
	window := int64(1 << 62)
	d := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
	for _, p := range d.Metadata().Parts[:n] {
		window = min(window, p.Bytes-64)
	}
	return OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: window}}
}

// TestVerifyColumnOrder: parts whose columns conflict are judged in
// manifest order, the first part setting the column, whichever part's
// reads end first; the verdicts kept are those of a serial check.
func TestVerifyColumnOrder(t *testing.T) {
	type narrow struct {
		Key  []byte `parquet:"key"`
		Size int32  `parquet:"size"`
	}
	type wide struct {
		Key  []byte `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	var a, c []narrow
	var b []wide
	for i := range 200 {
		a = append(a, narrow{[]byte(fmt.Sprintf("a%04d", i)), 1})
		b = append(b, wide{[]byte(fmt.Sprintf("b%04d", i)), 2})
		c = append(c, narrow{[]byte(fmt.Sprintf("c%04d", i)), 1})
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: a, GroupRows: 50}, {Rows: b, GroupRows: 50}, {Rows: c, GroupRows: 50}}})
	// Part 1 differs from part 0, which a serial check sees first: part 1
	// is refused, part 0 checked and part 2 not reached.
	want := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}).Verify(ctx, nil) // also out of any bubble first
	for _, slow := range []int{0, 1, 2} {
		synctest.Test(t, func(t *testing.T) {
			// The slow part's reads wait a virtual second: its check's
			// reads end last.
			src := &readHookSource{Source: dirOf(t, dir), read: func(path string, _ int, off int64) error {
				if path == testutil.PartPath(slow) {
					time.Sleep(time.Second)
				}
				return nil
			}}
			d, err := OpenDataset(ctx, src, smallWindow)
			must(t, err)
			defer d.Close()
			err = d.Verify(ctx, nil)
			var re *RefusalError
			if !errors.As(err, &re) || re.Code != CodeInconsistentColumn || re.Part != testutil.PartPath(1) || refusalOf(err) != refusalOf(want) {
				t.Fatalf("slow part %d: %v, want %v", slow, err, want)
			}
			if !d.r.parts[0].ready.Load() || d.r.parts[2].ready.Load() {
				t.Fatalf("slow part %d: parts checked %v %v", slow, d.r.parts[0].ready.Load(), d.r.parts[2].ready.Load())
			}
			// The verdicts kept are the serial ones: part 0 reads.
			if _, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte("a0001"))}, Limit: 1, Columns: []string{"size"}}, nil); err != nil {
				t.Fatalf("slow part %d: part 0 after Verify: %v", slow, err)
			}
		})
	}
}

// TestVerifySlowTails: Verify keeps at most ReadPolicy.Concurrency logical
// ranges outstanding, scalar end reads and batches together, however long
// the ends take.
func TestVerifySlowTails(t *testing.T) {
	keys := fixtureKeys(4000)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 8)...))
	must(t, open(t, dir, OpenOptions{}).Verify(ctx, nil)) // decoders made out of any bubble
	synctest.Test(t, func(t *testing.T) {
		src := &readHookSource{Source: dirOf(t, dir), read: func(_ string, n int, off int64) error {
			if n > 64 {
				time.Sleep(time.Duration(n) * time.Microsecond) // ends, larger, wait longer
			} else {
				time.Sleep(time.Millisecond)
			}
			return nil
		}}
		d, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{Concurrency: 3, MetadataPrefetchBytes: 1 << 12}})
		must(t, err)
		defer d.Close()
		must(t, d.Verify(ctx, nil))
		if m := src.most.Load(); m > 3 {
			t.Fatalf("%d reads at once, Concurrency 3", m)
		}
	})
}

// TestPartialTail: a page index that begins before a part's retained end
// and ends in it is completed by reading only its missing beginning.
func TestPartialTail(t *testing.T) {
	keys := fixtureKeys(600)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, keys))
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	ci := meta.RowGroups[0].Columns[0] // the key column's
	footerLen := int64(0)
	{
		d := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
		footerLen = d.Metadata().Parts[0].Bytes
	}
	// A window whose start falls 10 bytes into the key ColumnIndex of row
	// group 0.
	window := footerLen - (ci.ColumnIndexOffset + 10)
	src := &readHookSource{Source: dirOf(t, dir)}
	var got []int64
	src.read = func(_ string, n int, off int64) error {
		if off == ci.ColumnIndexOffset {
			got = append(got, int64(n))
		}
		return nil
	}
	d, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: window, Concurrency: 1}})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	var st ReadStats
	rows, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(keys[10]))}, Limit: 1}, &st)
	must(t, err)
	if len(rows) != 1 || len(got) != 1 || got[0] != 10 || st.MetadataHitBytes < int64(ci.ColumnIndexLength)-10 {
		t.Fatalf("rows %d, reads at the ColumnIndex %v, %d bytes from retained metadata", len(rows), got, st.MetadataHitBytes)
	}
}

// TestReadAheadPolicy: DisableSpeculation reads no page ahead, whatever
// ReadAhead asks; Concurrency bounds the pages read ahead.
func TestReadAheadPolicy(t *testing.T) {
	noTestReadAhead(t)
	keys := dirKeys(map[string]int{"a": 300, "b": 300, "c": 300}, "a", "b", "c")
	dir := write(t, parts(testutil.Part{GroupRows: 900, PageBytes: 200}, keys))
	for _, tc := range []struct {
		policy ReadPolicy
		most   int64
	}{{ReadPolicy{DisableSpeculation: true}, 0}, {ReadPolicy{Concurrency: 3}, 2}} {
		d := open(t, dir, OpenOptions{Read: tc.policy})
		r := d.r.newReadAhead(ctx, 8)
		switch {
		case tc.most == 0 && r != nil:
			t.Fatalf("%+v: a read-ahead", tc.policy)
		case tc.most > 0 && (r == nil || int64(r.slots) != tc.most):
			t.Fatalf("%+v: %v", tc.policy, r)
		}
		if r != nil {
			r.close(new(ReadStats))
		}
	}
	var st ReadStats
	_, err := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}).List(ctx, ListQuery{Delimiter: '/', Limit: 100, ReadAhead: 8}, &st)
	must(t, err)
	if st.ReadAheadReads != 0 {
		t.Fatalf("%d pages read ahead with speculation disabled", st.ReadAheadReads)
	}
}
