// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
	"github.com/varveio/seekparquet/go/source/local"
)

// TestConcurrentColdIndexes starts range reads and listings with differing
// projections on the same row groups of a dataset none has read yet, so
// their first reads load each group's page index concurrently, and
// compares every result with the same call on a dataset read sequentially.
func TestConcurrentColdIndexes(t *testing.T) {
	keys := fixtureKeys(3000)
	ds := parts(seekparquettest.Part{GroupRows: 500, PageBytes: 1024}, split(keys, 2)...)
	dir := write(t, ds)
	cold, warm := open(t, dir, seekparquet.Options{}), open(t, dir, seekparquet.Options{})
	var calls []func(d *seekparquet.Dataset) string
	for i, start := range []string{"", "b/", "d03/", "d07/s02/", "long/", "root"} {
		columns := []string{"size"}
		if i%2 == 0 {
			columns = []string{"name", "size"}
		}
		calls = append(calls,
			func(d *seekparquet.Dataset) string {
				rows, err := d.Range(ctx, seekparquet.Range{Start: []byte(start), Limit: 40, Columns: columns}, nil)
				if err != nil {
					return err.Error()
				}
				return fmt.Sprint(rows)
			},
			func(d *seekparquet.Dataset) string {
				entries, err := d.List(ctx, seekparquet.List{Prefix: []byte(start), Delimiter: '/', Limit: 30, Columns: columns}, nil)
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
}

// TestCloseWhileReading closes a dataset while eight readers page it:
// every read ends with rows or ErrClosed, and no descriptor stays open.
func TestCloseWhileReading(t *testing.T) {
	keys := fixtureKeys(600)
	dir := write(t, parts(seekparquettest.Part{GroupRows: 50, PageBytes: 256}, split(keys, 3)...))
	src := local.NewDir(dir, local.NewFileCache(2))
	d, err := seekparquet.OpenDataset(ctx, src, seekparquet.Options{})
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
					_, err = d.Range(ctx, seekparquet.Range{Start: []byte(keys[(g*7+i)%len(keys)]), Limit: 7, Columns: []string{"size"}}, nil)
				} else {
					_, err = d.List(ctx, seekparquet.List{Delimiter: '/', Limit: 7}, nil)
				}
				if errors.Is(err, seekparquet.ErrClosed) {
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
		_, _ = d.Range(ctx, seekparquet.Range{Limit: 1}, nil)
	}
	must(t, d.Close())
	wg.Wait()
	if open := src.Cache().Stats().Open; open != 0 {
		t.Fatalf("%d descriptors open after Close", open)
	}
}

// TestCancelledRead returns the context's error.
func TestCancelledRead(t *testing.T) {
	d := open(t, write(t, parts(seekparquettest.Part{}, []string{"a", "b"})), seekparquet.Options{})
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.Range(c, seekparquet.Range{Limit: 1}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("range: %v", err)
	}
	if _, err := d.List(c, seekparquet.List{Delimiter: '/', Limit: 1}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("list: %v", err)
	}
	if _, err := local.OpenDataset(c, write(t, parts(seekparquettest.Part{}, []string{"a"})), seekparquet.Options{}); !errors.Is(err, context.Canceled) {
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

func (s *boundSource) Open(_ context.Context, path string) (seekparquet.Part, error) {
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
	src := &boundSource{dir: write(t, parts(seekparquettest.Part{GroupRows: 3}, []string{"a", "b", "c", "d"}, []string{"e", "f"}))}
	c := context.WithValue(ctx, ctxKey{}, true)
	d, err := seekparquet.OpenDataset(c, src, seekparquet.Options{})
	must(t, err)
	if _, err := d.Range(c, seekparquet.Range{Limit: 10, Columns: []string{"size"}}, nil); err != nil {
		t.Fatal(err)
	}
	must(t, d.Close())
	if src.plain.Load() != 0 || src.bound.Load() == 0 || src.values.Load() == 0 || !src.closed.Load() {
		t.Fatalf("plain reads %d, bound %d, with the caller's context %d, closed %v", src.plain.Load(), src.bound.Load(), src.values.Load(), src.closed.Load())
	}
}

// TestOpenParts opens parts directly, without a manifest, with a footer
// and a row-group hook, and takes ownership of the files on failure.
func TestOpenParts(t *testing.T) {
	dir := write(t, parts(seekparquettest.Part{GroupRows: 2}, []string{"a", "b", "c"}, []string{"x", "y"}))
	sources := func(closed *atomic.Int64) []seekparquet.PartSource {
		var out []seekparquet.PartSource
		for i := range 2 {
			f, err := os.Open(partFile(dir, i))
			must(t, err)
			info, _ := f.Stat()
			out = append(out, seekparquet.PartSource{Name: fmt.Sprint(i), File: closer{f, closed}, Size: info.Size()})
		}
		return out
	}
	var closed atomic.Int64
	var groups []string
	r, err := seekparquet.OpenParts(ctx, sources(&closed), seekparquet.PartsOptions{
		CheckFooter: func(part string, footer *parquet.File) error {
			if _, ok := footer.Lookup(seekparquettest.StampSchemaID); !ok {
				return errors.New("no schema id")
			}
			return nil
		},
		CheckGroup: func(part string, _ *parquet.File, physical int, _ *format.RowGroup) error {
			groups = append(groups, fmt.Sprintf("%s/%d", part, physical))
			return nil
		},
	})
	must(t, err)
	if fmt.Sprint(groups) != "[0/0 0/1 1/0]" || r.RowGroups() != 3 || r.Rows() != 5 {
		t.Fatalf("groups %v", groups)
	}
	rows, err := r.Range(ctx, seekparquet.Range{Start: []byte("b"), Limit: 10}, nil)
	if err != nil || fmt.Sprint(keysOf(rows)) != "[b c x y]" {
		t.Fatalf("%q, %v", keysOf(rows), err)
	}
	must(t, r.Close())
	if closed.Load() != 2 {
		t.Fatalf("Close closed %d files", closed.Load())
	}
	closed.Store(0)
	hookErr := errors.New("refused by the caller")
	_, err = seekparquet.OpenParts(ctx, sources(&closed), seekparquet.PartsOptions{
		CheckGroup: func(part string, _ *parquet.File, physical int, _ *format.RowGroup) error {
			if part == "1" {
				return hookErr
			}
			return nil
		},
	})
	if err != hookErr || closed.Load() != 2 {
		t.Fatalf("failed open: %v, closed %d of 2", err, closed.Load())
	}
	// Bounds given for a part are checked.
	srcs := sources(&closed)
	srcs[0].MinKey, srcs[0].MaxKey = []byte("a"), []byte("d")
	if _, err := seekparquet.OpenParts(ctx, srcs, seekparquet.PartsOptions{}); seekparquet.CodeOf(err) != seekparquet.CodeBoundsMismatch {
		t.Fatalf("bounds: %v", err)
	}
}

type closer struct {
	*os.File
	closed *atomic.Int64
}

func (c closer) Close() error { c.closed.Add(1); return c.File.Close() }
