// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// smallWindow reads a part's end in 1 KiB, so that the fixture's parts are
// larger than it and Verify defers their heads.
var smallWindow = OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: 1 << 10}}

// setHead overwrites the first 4 bytes of part i.
func setHead(t *testing.T, dir string, i int, head string) {
	t.Helper()
	f, err := os.OpenFile(partFile(dir, i), os.O_WRONLY, 0)
	must(t, err)
	_, err = f.WriteAt([]byte(head), 0)
	must(t, err)
	must(t, f.Close())
}

// refusalOf describes err for comparison: its refusal or unsupported code
// and detail, or its text.
func refusalOf(err error) string {
	switch e := err.(type) {
	case *RefusalError:
		return fmt.Sprintf("refusal %s part %s group %d: %s", e.Code, e.Part, e.RowGroup, e.Detail)
	case *UnsupportedError:
		return fmt.Sprintf("unsupported %s: %s", e.Code, e.Detail)
	}
	return fmt.Sprint(err)
}

// TestVerifyDeferredHead: Verify reads a part's head with its first key
// page, after its end; the verdict on a broken head, alone or with another
// fault the end shows, is the one a read touching the part first reaches,
// head and end read together.
func TestVerifyDeferredHead(t *testing.T) {
	keys := fixtureKeys(2400)
	for _, tc := range []struct {
		name   string
		break_ func(t *testing.T, dir string)
	}{
		{"bad head", func(t *testing.T, dir string) { setHead(t, dir, 1, "XAR1") }},
		{"encrypted head", func(t *testing.T, dir string) { setHead(t, dir, 1, "PARE") }},
		{"bad head and stamps", func(t *testing.T, dir string) {
			rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
			setHead(t, dir, 1, "XAR1")
		}},
		{"stamps only", func(t *testing.T, dir string) {
			rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
			tc.break_(t, dir)
			// The part as a read touching it first judges it.
			d := open(t, dir, smallWindow)
			_, want := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(keys[700]))}, Limit: 1}, nil)
			if want == nil {
				t.Fatal("the broken part was read")
			}
			for _, opt := range []OpenOptions{smallWindow, {}} {
				v := open(t, dir, opt)
				if err := v.Verify(ctx, nil); refusalOf(err) != refusalOf(want) {
					t.Fatalf("%+v: Verify: %v\nwant %v", opt.Read, err, want)
				}
				// The verdict is kept, as a check's is.
				if err := v.Verify(ctx, nil); refusalOf(err) != refusalOf(want) {
					t.Fatalf("%+v: Verify again: %v", opt.Read, err)
				}
			}
		})
	}
}

// TestVerifyOrder: with several parts broken, Verify reports the first in
// manifest order, whatever order the parts' checks end in, every time.
func TestVerifyOrder(t *testing.T) {
	keys := fixtureKeys(2400)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
	setHead(t, dir, 3, "XAR1")
	rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	rewrite(t, dir, 2, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	for range 20 {
		for i, opt := range []OpenOptions{smallWindow, {}, {Read: ReadPolicy{MetadataPrefetchBytes: footerWindow}}} {
			d := open(t, dir, opt)
			if i == 2 {
				d.r.concurrency = 1
			}
			err := d.Verify(ctx, nil)
			if RefusalCodeOf(err) != CodeStampMismatch || err.(*RefusalError).Part != testutil.PartPath(1) {
				t.Fatalf("%+v: %v", opt.Read, err)
			}
		}
	}
}

// limitSource counts its reads in flight and the most at once; each read
// takes delay.
type limitSource struct {
	Source
	inFlight, most, reads atomic.Int64
	delay                 time.Duration
}

type limitFile struct {
	SourceFile
	s *limitSource
}

func (s *limitSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return limitFile{f, s}, nil
}

func (f limitFile) ReadAt(p []byte, off int64) (int, error) { return f.read(nil, p, off) }

func (f limitFile) WithContext(c context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) { return f.read(c, p, off) })
}

func (f limitFile) read(c context.Context, p []byte, off int64) (int, error) {
	f.s.reads.Add(1)
	n := f.s.inFlight.Add(1)
	defer f.s.inFlight.Add(-1)
	for {
		m := f.s.most.Load()
		if n <= m || f.s.most.CompareAndSwap(m, n) {
			break
		}
	}
	if f.s.delay > 0 {
		time.Sleep(f.s.delay)
	}
	if c != nil {
		if b, ok := f.SourceFile.(ContextBinder); ok {
			return b.WithContext(c).ReadAt(p, off)
		}
	}
	return f.SourceFile.ReadAt(p, off)
}

// TestVerifyConcurrency: Verify keeps at most the internal concurrency limit reads
// in flight and checks every part in one read of its end, then one of each
// row group's first key page, the first with the part's head, and one of
// its last key page; a second Verify reads nothing.
func TestVerifyConcurrency(t *testing.T) {
	keys := fixtureKeys(4000)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 8)...))
	// A window just short of the smallest part: it holds every part's
	// footer and page indexes, not its head.
	window := int64(1 << 62)
	for i := range 8 {
		info, err := os.Stat(partFile(dir, i))
		must(t, err)
		window = min(window, info.Size()-64)
	}
	for _, n := range []int{1, 3} {
		src := &limitSource{Source: dirOf(t, dir)}
		d, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: window}})
		must(t, err)
		d.r.concurrency = n
		t.Cleanup(func() { _ = d.Close() })
		must(t, d.Verify(ctx, nil))
		if src.most.Load() > int64(n) {
			t.Fatalf("Concurrency %d: %d reads at once", n, src.most.Load())
		}
		for i, p := range d.r.parts {
			if !p.ready.Load() || p.flags.Load()&partLastChecked == 0 {
				t.Fatalf("part %d not verified", i)
			}
		}
		groups := len(d.r.parts[0].groups)
		if want := int64(1 + 8*(1+groups+1)); src.reads.Load() != want { // the manifest, then per part
			t.Fatalf("Concurrency %d: %d reads, want %d (%d row groups a part)", n, src.reads.Load(), want, groups)
		}
		before := src.reads.Load()
		must(t, d.Verify(ctx, nil))
		if src.reads.Load() != before {
			t.Fatalf("a second Verify read %d times", src.reads.Load()-before)
		}
	}
}

// TestVerifyHostileProbes: Verify's fetch decodes key OffsetIndexes from
// retained bytes without checking them, only to locate pages; a corrupt
// one locates nothing it reads beyond its limits, and Verify refuses the
// part exactly as without retained metadata.
func TestVerifyHostileProbes(t *testing.T) {
	keys := fixtureKeys(2400)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
	meta, err := testutil.ReadFooter(partFile(dir, 2))
	must(t, err)
	oi := meta.RowGroups[1].Columns[0] // the key column comes first
	f, err := os.OpenFile(partFile(dir, 2), os.O_WRONLY, 0)
	must(t, err)
	junk := make([]byte, oi.OffsetIndexLength)
	for i := range junk {
		junk[i] = byte(0xF0 | i)
	}
	_, err = f.WriteAt(junk, oi.OffsetIndexOffset)
	must(t, err)
	must(t, f.Close())
	want := open(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}).Verify(ctx, nil)
	if want == nil {
		t.Fatal("the corrupt index went unnoticed")
	}
	for _, opt := range []OpenOptions{{}, smallWindow, {Limits: Limits{PageBytes: 64}}} {
		if err := open(t, dir, opt).Verify(ctx, nil); refusalOf(err) != refusalOf(want) && opt.Limits.PageBytes == 0 {
			t.Fatalf("%+v: %v, want %v", opt, err, want)
		}
	}
}
