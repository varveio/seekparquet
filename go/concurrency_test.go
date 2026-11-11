// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
			cold, warm := open(t, dir, OpenOptions{}), open(t, dir, OpenOptions{})
			var calls []func(d *Dataset) string
			for i, start := range []string{"", "b/", "d03/", "d07/s02/", "long/", "root"} {
				columns := []string{"size"}
				if i%2 == 0 {
					columns = []string{"name", "size"}
				}
				calls = append(calls,
					func(d *Dataset) string {
						rows, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(start)}}, Limit: 40, Columns: columns}, nil)
						if err != nil {
							return err.Error()
						}
						return fmt.Sprint(rows)
					},
					func(d *Dataset) string {
						rows, err := d.Rows(ctx, Query{Selection: Selection{Cursor: &Cursor{Key: []byte(start), Offset: 1}}, Limit: 40, Columns: columns}, nil)
						if err != nil {
							return err.Error()
						}
						return fmt.Sprint(rows)
					},
					func(d *Dataset) string {
						entries, err := d.List(ctx, ListQuery{Prefix: []byte(start), Delimiter: '/', Limit: 30, Columns: columns}, nil)
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
	d, err := OpenDataset(ctx, src, OpenOptions{})
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
					_, err = d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(keys[(g*7+i)%len(keys)])}}, Limit: 7, Columns: []string{"size"}}, nil)
				} else {
					_, err = d.List(ctx, ListQuery{Delimiter: '/', Limit: 7}, nil)
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
		_, _ = d.Rows(ctx, Query{Limit: 1}, nil)
	}
	must(t, d.Close())
	wg.Wait()
	if opened, closed := src.opened.Load(), src.closed.Load(); opened != closed {
		t.Fatalf("%d files opened, %d closed after Close", opened, closed)
	}
}

// TestCancelledRead returns the context's error.
func TestCancelledRead(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a", "b"})), OpenOptions{})
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.Rows(c, Query{Limit: 1}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("range: %v", err)
	}
	if _, err := d.List(c, ListQuery{Delimiter: '/', Limit: 1}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("list: %v", err)
	}
	if _, err := OpenDir(c, write(t, parts(testutil.Part{}, []string{"a"})), OpenOptions{}); !errors.Is(err, context.Canceled) {
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
	d, err := OpenDataset(c, src, OpenOptions{})
	must(t, err)
	if _, err := d.Rows(c, Query{Limit: 10, Columns: []string{"size"}}, nil); err != nil {
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
	r, err := openParts(ctx, inputs(&closed), unique, OpenOptions{})
	must(t, err)
	if len(r.flat) != 3 || r.rows != 5 || r.bounded {
		t.Fatalf("%d groups, %d rows, bounded %v", len(r.flat), r.rows, r.bounded)
	}
	rows, err := r.readRange(ctx, Query{Selection: Selection{Span: Span{Start: []byte("b")}}, Limit: 10}, nil)
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
	r, err = openParts(ctx, failing, unique, OpenOptions{})
	must(t, err)
	if err := r.verify(ctx); RefusalCodeOf(err) != CodeBoundsMismatch {
		t.Fatalf("bounds: %v", err)
	}
	must(t, r.close())
	if closed.Load() != 2 {
		t.Fatalf("closed %d of 2", closed.Load())
	}
}

type closer struct {
	*os.File
	closed *atomic.Int64
}

func (c closer) Close() error { c.closed.Add(1); return c.File.Close() }

// TestRequestInsideScanWhileClosing: a Scan callback that starts another
// request while another goroutine's Close waits for the scan gets
// ErrClosed instead of waiting for ever (behind the Close, which waits for
// the scan); the scan then ends and Close returns.
func TestRequestInsideScanWhileClosing(t *testing.T) {
	for _, batches := range []bool{false, true} {
		t.Run(fmt.Sprintf("batches=%t", batches), func(t *testing.T) { testRequestInsideScanWhileClosing(t, batches) })
	}
}

func testRequestInsideScanWhileClosing(t *testing.T, batches bool) {
	keys := fixtureKeys(60)
	// Opened without open's cleanup Close: should the nested request wait,
	// the test fails at its timeout rather than waiting in a cleanup.
	d, err := OpenDataset(ctx, dirOf(t, write(t, parts(testutil.Part{GroupRows: 20}, split(keys, 2)...))), OpenOptions{})
	must(t, err)
	plan, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
	must(t, err)
	closed := make(chan error, 1)
	nested := make(chan error, 1)
	done := make(chan error, 1)
	stop := errors.New("stop")
	go func() {
		callback := func() error {
			go func() { closed <- d.Close() }()
			// Wait until Close has begun: it cancels requests first.
			for d.r.closing.Err() == nil {
				runtime.Gosched()
			}
			_, err := d.Rows(ctx, Query{Limit: 1}, nil)
			nested <- err
			return stop
		}
		if batches {
			done <- plan.Scan(ctx, nil, func(*Batch) error { return callback() })
		} else {
			done <- scanRows(ctx, plan, nil, func(Row) error { return callback() })
		}
	}()
	timeout := time.After(10 * time.Second)
	for _, c := range []struct {
		name string
		ch   chan error
		want error
	}{{"the nested request", nested, ErrClosed}, {"the scan", done, stop}, {"Close", closed, nil}} {
		select {
		case err := <-c.ch:
			if !errors.Is(err, c.want) && err != c.want {
				t.Fatalf("%s: %v, want %v", c.name, err, c.want)
			}
		case <-timeout:
			t.Fatalf("%s did not return: a request inside a scan waits behind Close", c.name)
		}
	}
}
