// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestConcurrentColdIndexes starts range reads and listings with differing
// projections and cursors on the same row groups of a dataset none has
// read yet (a default open loads no page index and decodes no key;
// TestOpenReads checks that), so their first reads load each group's page
// index and first key concurrently, and compares every result with the
// same call on a dataset read sequentially. It runs with unique and with
// repeated keys.
func TestConcurrentColdIndexes(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		t.Run(fmt.Sprint("repeated keys ", repeated), func(t *testing.T) {
			keys := fixtureKeys(3000)
			if repeated {
				for i := range keys {
					keys[i] = keys[i-i%3] // groups of three equal keys
				}
			}
			// Two parts split at a group boundary: a group never crosses parts.
			doc := parts(testutil.Part{GroupRows: 500, PageBytes: 1024}, keys[:1500], keys[1500:])
			doc.RepeatedKeys = repeated
			dir := write(t, doc)
			cold, warm := open(t, dir, Options{}), open(t, dir, Options{})
			var calls []func(d *Dataset) string
			for i, start := range []string{"", "b/", "d03/", "d07/s02/", "long/", "root"} {
				columns := []string{"size"}
				if i%2 == 0 {
					columns = []string{"name", "size"}
				}
				calls = append(calls,
					func(d *Dataset) string {
						rows, err := d.Range(ctx, Range{Start: []byte(start), Limit: 40, Columns: columns})
						if err != nil {
							return err.Error()
						}
						return fmt.Sprint(rows)
					},
					func(d *Dataset) string {
						rows, err := d.Range(ctx, Range{Cursor: &Cursor{Key: []byte(start), Offset: 1}, Limit: 40, Columns: columns})
						if err != nil {
							return err.Error()
						}
						return fmt.Sprint(rows)
					},
					func(d *Dataset) string {
						entries, err := d.List(ctx, List{Prefix: []byte(start), Delimiter: '/', Limit: 30, Columns: columns})
						if err != nil {
							return err.Error()
						}
						var out []string
						for _, e := range entries {
							if e.IsPrefix() {
								out = append(out, "p:"+string(e.CommonPrefix))
							} else {
								out = append(out, fmt.Sprint(*e.Row))
							}
						}
						return fmt.Sprint(out)
					})
			}
			want := make([]string, len(calls))
			for i, c := range calls {
				want[i] = c(warm)
			}
			got := make([]string, len(calls)*4)
			var wg sync.WaitGroup
			for i := range got {
				wg.Go(func() { got[i] = calls[i%len(calls)](cold) })
			}
			wg.Wait()
			for i := range got {
				if got[i] != want[i%len(calls)] {
					t.Fatalf("call %d: %s\nwant %s", i%len(calls), got[i], want[i%len(calls)])
				}
			}
		})
	}
}

// TestCloseWhileReading closes a dataset while eight readers page it:
// every read ends with rows or ErrClosed, and every file it opened is
// closed.
func TestCloseWhileReading(t *testing.T) {
	keys := fixtureKeys(600)
	src := &countingSource{Source: dirOf(t, write(t, parts(testutil.Part{GroupRows: 50, PageBytes: 256}, split(keys, 3)...)))}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	var wg sync.WaitGroup
	var reads atomic.Int64
	start := make(chan struct{})
	for g := range 8 {
		wg.Go(func() {
			<-start
			for i := 0; ; i++ {
				var err error
				if g%2 == 0 {
					_, err = d.Range(ctx, Range{Start: []byte(keys[(g*7+i)%len(keys)]), Limit: 7, Columns: []string{"size"}})
				} else {
					_, err = d.List(ctx, List{Delimiter: '/', Limit: 7})
				}
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				reads.Add(1)
			}
		})
	}
	close(start)
	for reads.Load() < 20 {
		_, _ = d.Range(ctx, Range{Limit: 1})
	}
	must(t, d.Close())
	wg.Wait()
	if opened, closed := src.opened.Load(), src.closed.Load(); opened != closed {
		t.Fatalf("%d files opened, %d closed after Close", opened, closed)
	}
}

// TestCancelledRead returns the context's error.
func TestCancelledRead(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a", "b"})), Options{})
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.Range(c, Range{Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("range: %v", err)
	}
	if _, err := d.List(c, List{Delimiter: '/', Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("list: %v", err)
	}
	if _, err := OpenDir(c, write(t, parts(testutil.Part{}, []string{"a"})), Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("open: %v", err)
	}
}

// boundFile is a part file that counts the reads bound to a context and
// those that were not.
type boundFile struct {
	*os.File
	size         int64
	bound, plain *atomic.Int64
	sawCtxValues *atomic.Int64
	closed       *atomic.Bool
}

type ctxKey struct{}

func (f boundFile) Size() int64 { return f.size }
func (f boundFile) ReadAt(p []byte, off int64) (int, error) {
	f.plain.Add(1)
	return f.File.ReadAt(p, off)
}
func (f boundFile) WithContext(c context.Context) io.ReaderAt {
	if c.Value(ctxKey{}) != nil {
		f.sawCtxValues.Add(1)
	}
	return readerFunc(func(p []byte, off int64) (int, error) {
		f.bound.Add(1)
		if err := c.Err(); err != nil {
			return 0, err
		}
		return f.File.ReadAt(p, off)
	})
}
func (f boundFile) Close() error { f.closed.Store(true); return f.File.Close() }

type readerFunc func(p []byte, off int64) (int, error)

func (r readerFunc) ReadAt(p []byte, off int64) (int, error) { return r(p, off) }

type boundSource struct {
	dir                  string
	bound, plain, values atomic.Int64
	closed               atomic.Bool
}

func (s *boundSource) Open(_ context.Context, path string) (SourceFile, error) {
	f, err := os.Open(s.dir + "/" + path)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	return boundFile{File: f, size: info.Size(), bound: &s.bound, plain: &s.plain, sawCtxValues: &s.values, closed: &s.closed}, nil
}

// TestContextBinding: every read of a ContextBinder source, footers and
// manifest included, goes through the reader bound to the operation's
// context.
func TestContextBinding(t *testing.T) {
	src := &boundSource{dir: write(t, parts(testutil.Part{GroupRows: 3}, []string{"a", "b", "c", "d"}, []string{"e", "f"}))}
	c := context.WithValue(ctx, ctxKey{}, true)
	d, err := OpenDataset(c, src, Options{})
	must(t, err)
	if _, err := d.Range(c, Range{Limit: 10, Columns: []string{"size"}}); err != nil {
		t.Fatal(err)
	}
	must(t, d.Close())
	if src.plain.Load() != 0 || src.bound.Load() == 0 || src.values.Load() == 0 || !src.closed.Load() {
		t.Fatalf("plain reads %d, bound %d, with the caller's context %d, closed %v", src.plain.Load(), src.bound.Load(), src.values.Load(), src.closed.Load())
	}
}

// TestOpenPartsOwnership opens parts directly, without a manifest, and
// takes ownership of their files: they are closed by close, and by a
// failed open; bounds given for a part are checked.
func TestOpenPartsOwnership(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 2}, []string{"a", "b", "c"}, []string{"x", "y"}))
	inputs := func(closed *atomic.Int64) []partInput {
		var out []partInput
		for i := range 2 {
			f, err := os.Open(partFile(dir, i))
			must(t, err)
			info, _ := f.Stat()
			out = append(out, partInput{name: fmt.Sprint(i), file: closer{f, closed}, size: info.Size()})
		}
		return out
	}
	var closed atomic.Int64
	r, err := openParts(ctx, inputs(&closed), unique, Options{})
	must(t, err)
	if len(r.groups) != 3 || r.rows != 5 || r.bounded {
		t.Fatalf("%d groups, %d rows, bounded %v", len(r.groups), r.rows, r.bounded)
	}
	rows, err := r.readRange(ctx, Range{Start: []byte("b"), Limit: 10}, nil)
	if err != nil || fmt.Sprint(keysOf(rows)) != "[b c x y]" {
		t.Fatalf("%q, %v", keysOf(rows), err)
	}
	must(t, r.close())
	if closed.Load() != 2 {
		t.Fatalf("close closed %d files", closed.Load())
	}
	closed.Store(0)
	failing := inputs(&closed)
	failing[0].minKey, failing[0].maxKey = []byte("a"), []byte("d")
	if _, err := openParts(ctx, failing, unique, Options{VerifyOnOpen: true}); CodeOf(err) != CodeBoundsMismatch || closed.Load() != 2 {
		t.Fatalf("bounds: %v, closed %d of 2", err, closed.Load())
	}
}

type closer struct {
	*os.File
	closed *atomic.Int64
}

func (c closer) Close() error { c.closed.Add(1); return c.File.Close() }
