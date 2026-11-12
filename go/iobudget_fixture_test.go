// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil/fakestore"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// This file writes the I/O budget fixture: an S3 inventory shaped like
// the one the 2026-11-11 remote-read audit measured (swath's object
// schema, 1,000 top-level folders acct-NNNN/ with log-normal fan-out in
// four tree shapes, and one folder, acct-0500/, of 100 child folders
// interleaved with 1,400 direct objects), written by this module's own
// part writer (package write: 1,024-row pages, ZSTD, version 2 pages) and
// published by package local, at test time and from a fixed seed, so the
// bytes are the same on every run and platform.

// listTenant is the folder the listing operations list.
const listTenant = 500

// ioFixtureSpec sizes a fixture.
type ioFixtureSpec struct {
	rows      int  // about this many rows
	parts     int  // parts, cut by row count, never inside a group of equal keys
	groups    int  // row groups per part
	perFolder int  // objects in each of acct-0500/'s 100 child folders
	repeated  bool // each key is written 1 to 3 times (a non-unique key)
}

// The fixtures. ioSmall is the default budget fixture and ioRepeated its
// repeated-key variant (about 200,000 keys written 1 to 3 times, so about
// 400,000 rows; folders of 500 keys so that acct-0500/ stays a quarter of
// the rows); ioAudit is the audit's size (SEEKPARQUET_IO_BUDGET_FULL=1);
// ioTiny is the fault matrix's, the same shape with folders of 100.
var (
	ioSmall    = ioFixtureSpec{rows: 400_000, parts: 4, groups: 4, perFolder: 1000}
	ioRepeated = ioFixtureSpec{rows: 200_000, parts: 4, groups: 4, perFolder: 500, repeated: true}
	ioAudit    = ioFixtureSpec{rows: 4_000_000, parts: 4, groups: 4, perFolder: 1000}
	ioTiny     = ioFixtureSpec{rows: 40_000, parts: 4, groups: 4, perFolder: 100}
)

// ioFixture is a written fixture: its directory, its files in memory (for
// fakestore), its rows' keys and the keys the operations use.
type ioFixture struct {
	dir     string
	objects map[string]fakestore.Object
	keys    [][]byte // every row's key, in order
	starts  []int    // each part's first row
	unique  bool

	part                    int    // the part the per-part operations use
	first, mid, last, other []byte // keys of that part: first, middle, last, and in an earlier row group
	twenty                  [][]byte
	listPrefix              []byte
}

var ioSchema = parquet.NewSchema("object", parquet.Group{
	"key":           parquet.Encoded(parquet.String(), &parquet.DeltaByteArray),
	"size":          parquet.Int(64),
	"last_modified": parquet.Timestamp(parquet.Microsecond),
	"etag":          parquet.Encoded(parquet.String(), &parquet.Plain),
	"storage_class": parquet.String(), // the writer's default: dictionary
})

// ioColumns is the two-column projection of the lookups.
var ioColumns = []string{"size", "last_modified"}

func tenantKeys(rng *rand.Rand, t, n, perFolder int) []string {
	p := fmt.Sprintf("acct-%04d/", t)
	var keys []string
	if t == listTenant {
		for i := range 1500 {
			if i%15 == 0 {
				for j := range perFolder {
					keys = append(keys, fmt.Sprintf("%sitem-%04d/part-%05d.parquet", p, i, j))
				}
			} else {
				keys = append(keys, fmt.Sprintf("%sitem-%04d.bin", p, i))
			}
		}
		return keys
	}
	for len(keys) < n {
		switch t % 4 {
		case 0: // date-partitioned logs, 5 levels deep
			m, d, h := 1+rng.IntN(12), 1+rng.IntN(28), rng.IntN(24)
			keys = append(keys, fmt.Sprintf("%slogs/2025/%02d/%02d/%02d/app-%02d-%06d.log.gz", p, m, d, h, rng.IntN(8), rng.IntN(1_000_000)))
		case 1: // user uploads
			u := rng.IntN(max(n/40, 1))
			keys = append(keys, fmt.Sprintf("%susers/u%06d/photos/IMG_%05d.jpg", p, u, rng.IntN(100_000)))
		case 2: // flat exports
			keys = append(keys, fmt.Sprintf("%sexports/report-%08d.csv", p, rng.IntN(100_000_000)))
		case 3: // source tree, deep and narrow
			keys = append(keys, fmt.Sprintf("%ssrc/module-%02d/pkg/%s/sub-%d/file%04d.go", p, rng.IntN(20),
				[]string{"api", "core", "util", "internal", "cmd"}[rng.IntN(5)], rng.IntN(6), rng.IntN(10_000)))
		}
		if len(keys) == n {
			slices.Sort(keys)
			keys = slices.Compact(keys)
		}
	}
	return keys
}

// objectRow appends one row's values, in the schema's column order (etag,
// key, last_modified, size, storage_class), with values drawn as the
// audit drew them.
func objectRow(rng *rand.Rand, key string, row parquet.Row) parquet.Row {
	size := int64(math.Exp(rng.NormFloat64()*2.5 + 9))
	etag := fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
	if size > 64<<20 {
		etag += fmt.Sprintf("-%d", 1+size/(16<<20))
	}
	class := "STANDARD"
	switch r := rng.IntN(100); {
	case r < 10:
		class = "STANDARD_IA"
	case r < 14:
		class = "GLACIER_IR"
	case r < 15:
		class = "INTELLIGENT_TIERING"
	}
	mtime := 1_704_067_200_000_000 + rng.Int64N(2*365*86400*1_000_000)
	return append(row[:0],
		parquet.ByteArrayValue([]byte(etag)).Level(0, 0, 0),
		parquet.ByteArrayValue([]byte(key)).Level(0, 0, 1),
		parquet.Int64Value(mtime).Level(0, 0, 2),
		parquet.Int64Value(size).Level(0, 0, 3),
		parquet.ByteArrayValue([]byte(class)).Level(0, 0, 4),
	)
}

// writeIOFixture writes spec's fixture into a new directory under parent.
func writeIOFixture(t testing.TB, parent string, spec ioFixtureSpec) *ioFixture {
	t.Helper()
	rng := rand.New(rand.NewPCG(42, 1))
	weights := make([]float64, 1000)
	sum := 0.0
	for i := range weights {
		weights[i] = math.Exp(rng.NormFloat64() * 1.3)
		sum += weights[i]
	}
	listRows := 100*spec.perFolder + 1400
	var rows []parquet.Row
	var keys [][]byte
	for tenant := range 1000 {
		n := max(10, int(float64(spec.rows-listRows)*weights[tenant]/sum))
		for _, k := range tenantKeys(rng, tenant, n, spec.perFolder) {
			copies := 1
			if spec.repeated {
				copies = 1 + rng.IntN(3)
			}
			for range copies {
				rows = append(rows, objectRow(rng, k, nil))
				keys = append(keys, []byte(k))
			}
		}
	}

	f := &ioFixture{dir: filepath.Join(parent, "inventory"), keys: keys, unique: !spec.repeated, part: 1, listPrefix: fmt.Appendf(nil, "acct-%04d/", listTenant)}
	// Cut parts by row count, moving each cut past a group of equal keys.
	for p := range spec.parts {
		at := p * len(rows) / spec.parts
		for at > 0 && at < len(keys) && string(keys[at]) == string(keys[at-1]) {
			at++
		}
		f.starts = append(f.starts, at)
	}
	must(t, os.Mkdir(f.dir, 0o755))
	root, err := os.OpenRoot(f.dir)
	must(t, err)
	defer root.Close()
	must(t, root.Mkdir("data", 0o755))
	// Windows cannot sync a directory; the tests only read what they wrote.
	ops := local.Ops{Root: root, AllowUnsynced: runtime.GOOS == "windows"}
	key := seekparquet.Key{Column: "key", Unique: f.unique}
	var infos []seekparquet.PartInfo
	for p, start := range f.starts {
		end := len(rows)
		if p+1 < len(f.starts) {
			end = f.starts[p+1]
		}
		tmp, final := fmt.Sprintf("data/.part-%05d.tmp", p), fmt.Sprintf("data/part-%05d.parquet", p)
		file, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		must(t, err)
		w, err := write.NewPart(file, ioSchema, write.PartOptions{Key: key, SchemaID: "test/iobudget/object/v1",
			RowGroupRows: int64((end - start + spec.groups - 1) / spec.groups), RowGroupBytes: 1 << 40})
		must(t, err)
		_, err = w.WriteRows(rows[start:end])
		must(t, err)
		info, err := w.Seal()
		must(t, err)
		must(t, local.PublishPart(ops, tmp, final))
		info.Path = final
		infos = append(infos, info)
	}
	_, err = local.Commit(ops, seekparquet.Manifest{Key: key, SchemaID: "test/iobudget/object/v1", Parts: infos,
		Version: seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision}})
	must(t, err)

	f.objects = map[string]fakestore.Object{}
	for _, name := range append([]string{seekparquet.ManifestName}, partPaths(infos)...) {
		data, err := os.ReadFile(filepath.Join(f.dir, filepath.FromSlash(name)))
		must(t, err)
		f.objects[name] = fakestore.Object{Data: data}
	}

	// The keys the operations use, chosen as the audit chose them.
	pStart, pEnd := f.starts[f.part], f.starts[f.part+1]
	f.first, f.last = keys[pStart], keys[pEnd-1]
	f.mid, f.other = keys[pStart+(pEnd-pStart)/2], keys[pStart+(pEnd-pStart)/5]
	for i := range 20 {
		f.twenty = append(f.twenty, keys[i*len(keys)/20+len(keys)/5000])
	}
	return f
}

// The fixtures are written once per test binary, under ioFixtureDir,
// which TestMain removes.
var (
	ioFixtureMu  sync.Mutex
	ioFixtureDir string
	ioFixtures   = map[ioFixtureSpec]*ioFixture{}
)

// ioFixtureOf returns spec's fixture, writing it on first use.
func ioFixtureOf(t testing.TB, spec ioFixtureSpec) *ioFixture {
	t.Helper()
	ioFixtureMu.Lock()
	defer ioFixtureMu.Unlock()
	if f, ok := ioFixtures[spec]; ok {
		return f
	}
	if ioFixtureDir == "" {
		dir, err := os.MkdirTemp("", "seekparquet-iobudget-")
		must(t, err)
		ioFixtureDir = dir
	}
	parent, err := os.MkdirTemp(ioFixtureDir, "fixture-")
	must(t, err)
	f := writeIOFixture(t, parent, spec)
	// The reader's ZSTD decoder is shared by the process and made on first
	// use: made inside a synctest bubble, it could serve no one outside
	// that bubble. Decode a page out of any bubble first.
	d, err := seekparquet.OpenDir(context.Background(), f.dir, seekparquet.OpenOptions{})
	must(t, err)
	_, err = d.Get(context.Background(), f.first, nil, ioColumns...)
	must(t, errors.Join(err, d.Close()))
	ioFixtures[spec] = f
	return f
}

// TestMain removes the I/O budget fixtures after the tests.
func TestMain(m *testing.M) {
	code := m.Run()
	if ioFixtureDir != "" {
		if err := os.RemoveAll(ioFixtureDir); err != nil {
			fmt.Fprintln(os.Stderr, "remove the I/O budget fixtures:", err)
		}
	}
	os.Exit(code)
}

func partPaths(infos []seekparquet.PartInfo) []string {
	var out []string
	for _, p := range infos {
		out = append(out, p.Path)
	}
	return out
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// fakeSource adapts a fakestore.Source to seekparquet.Source as objfile's
// adapter serves a remote store today: Open (with the store's metadata
// request), and files with ReadAt and WithContext only. It reports its
// physical requests (IOReporter), and maps the store's errors to the
// module's. capableSource adds the optional capabilities.
type fakeSource struct{ *fakestore.Source }

func (s fakeSource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := s.Source.Open(observed(ctx), path)
	if err != nil {
		return nil, fakeError(err)
	}
	return fakeFile{f}, nil
}

func (fakeSource) ReportsIO() bool { return true }

// capableSource is fakeSource with every optional capability: OpenSized,
// ReadManifest and, on its files, ReadRanges.
type capableSource struct{ fakeSource }

func (s capableSource) OpenSized(ctx context.Context, path string, size int64) (seekparquet.SourceFile, error) {
	f, err := s.Source.OpenSized(observed(ctx), path, size)
	if err != nil {
		return nil, fakeError(err)
	}
	return fakeRangeFile{fakeFile{f}}, nil
}

func (s capableSource) ReadManifest(ctx context.Context, maxBytes int64) ([]byte, string, error) {
	data, identity, err := s.Source.ReadObject(observed(ctx), seekparquet.ManifestName, maxBytes)
	return data, identity, fakeError(err)
}

type fakeFile struct{ f *fakestore.File }

func (f fakeFile) Size() int64     { return f.f.Size() }
func (f fakeFile) Close() error    { return f.f.Close() }
func (f fakeFile) ReportsIO() bool { return true }

func (f fakeFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.f.ReadAt(p, off)
	return n, fakeError(err)
}

func (f fakeFile) WithContext(ctx context.Context) io.ReaderAt {
	return fakeView{f.f.WithContext(observed(ctx))}
}

type fakeView struct{ r io.ReaderAt }

func (v fakeView) ReadAt(p []byte, off int64) (int, error) {
	n, err := v.r.ReadAt(p, off)
	return n, fakeError(err)
}

type fakeRangeFile struct{ fakeFile }

func (f fakeRangeFile) ReadRanges(ctx context.Context, ranges []seekparquet.ReadRange) error {
	extents := make([]fakestore.Extent, len(ranges))
	for i, rg := range ranges {
		extents[i] = fakestore.Extent{Off: rg.Off, Buf: rg.Buf}
	}
	err := f.f.ReadRanges(observed(ctx), extents)
	var be *fakestore.BatchError
	if errors.As(err, &be) {
		errs := make([]error, len(be.Errs))
		for i, e := range be.Errs {
			errs[i] = fakeError(e)
		}
		return &seekparquet.BatchError{Errs: errs}
	}
	return fakeError(err)
}

// fakeError maps the store's errors to the module's: a broken pin to
// ErrObjectChanged, a sized open's size to *SizeMismatchError, an object
// beyond its read limit to ErrObjectTooLarge, keeping the store's error.
func fakeError(err error) error {
	var mismatch *fakestore.SizeMismatchError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fakestore.ErrObjectChanged):
		return fmt.Errorf("%w: %w", seekparquet.ErrObjectChanged, err)
	case errors.Is(err, fakestore.ErrTooLarge):
		return fmt.Errorf("%w: %w", seekparquet.ErrObjectTooLarge, err)
	case errors.As(err, &mismatch):
		return &seekparquet.SizeMismatchError{Path: mismatch.Path, Expected: mismatch.Expected, Actual: mismatch.Actual}
	}
	return err
}

// observed returns ctx with the store reporting its requests to the
// operation's IOObserver, when it has one.
func observed(ctx context.Context) context.Context {
	if o := seekparquet.IOObserverFromContext(ctx); o != nil {
		return fakestore.WithObserver(ctx, observer{o})
	}
	return ctx
}

type observer struct{ o seekparquet.IOObserver }

func (o observer) Begin(kind fakestore.Kind, name string, off, length int64) func(int64, error) {
	req := seekparquet.IORequest{Path: name, Offset: off, Length: length}
	switch kind {
	case fakestore.Metadata:
		req.Kind = seekparquet.IOMetadata
	case fakestore.Whole:
		req.Kind, req.Length = seekparquet.IOWholeObject, -1
	default:
		req.Kind = seekparquet.IORange
	}
	done := o.o.Begin(req)
	return func(got int64, err error) { done(seekparquet.IOResult{Bytes: got, Err: err}) }
}

var (
	_ seekparquet.ContextBinder  = fakeFile{}
	_ seekparquet.RangeReader    = fakeRangeFile{}
	_ seekparquet.SizedSource    = capableSource{}
	_ seekparquet.ManifestReader = capableSource{}
	_ seekparquet.IOReporter     = fakeSource{}
)
