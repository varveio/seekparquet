// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package local_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/samples"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// unsynced is set where directories cannot be synced (Windows): the tests
// then accept visible commits, and skip faults of directory syncs.
var unsynced = !*local.DirSyncSupported

// committed is what a successful commit reports here.
var committed = func() local.CommitResult {
	if unsynced {
		return local.Visible
	}
	return local.Durable
}()

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// checkDataset opens dir with the module's reader, checking every row
// group's first key page and each part's last on open, reads every row
// with every column, and compares keys and values with the rows written;
// it checks the parts' names, digests and that no temporary file is left.
func checkDataset(t *testing.T, dir string, s samples.Sample) seekparquet.Metadata {
	t.Helper()
	d, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatalf("%s: open: %v", s.Name, err)
	}
	defer d.Close()
	meta := d.Metadata()
	var columns []string
	for _, f := range s.Schema.Fields() {
		columns = append(columns, f.Name())
	}
	keys := s.Keys()
	var got []seekparquet.Row
	req := seekparquet.Range{Limit: 997, Columns: columns}
	for {
		rows, err := d.Range(context.Background(), req)
		if err != nil {
			t.Fatalf("%s: range: %v", s.Name, err)
		}
		if len(rows) == 0 {
			break
		}
		got = append(got, rows...)
		req.Cursor = rows[len(rows)-1].Cursor()
	}
	if len(got) != len(keys) {
		t.Fatalf("%s: read %d rows of %d", s.Name, len(got), len(keys))
	}
	for i, row := range got {
		if !bytes.Equal(row.Key, keys[i]) {
			t.Fatalf("%s: row %d has key %.40q, want %.40q", s.Name, i, row.Key, keys[i])
		}
		for c, v := range row.Values {
			want := s.Rows[i][c]
			if !parquet.Equal(v.Value, want) && !(v.IsNull() && want.IsNull()) {
				t.Fatalf("%s: row %d column %s: %v, want %v", s.Name, i, columns[c], v.Value, want)
			}
		}
	}
	for i, p := range meta.Parts {
		if want := local.PartPath(i); p.Path != want {
			t.Errorf("%s: part %d is %s, want %s", s.Name, i, p.Path, want)
		}
		raw, err := os.ReadFile(filepath.Join(dir, p.Path))
		must(t, err)
		if sum := sha256.Sum256(raw); p.SHA256 != hex.EncodeToString(sum[:]) {
			t.Errorf("%s: part %d digest %s does not match", s.Name, i, p.SHA256)
		}
	}
	filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if strings.HasSuffix(path, ".tmp") {
			t.Errorf("%s: temporary file %s left", s.Name, path)
		}
		return nil
	})
	return meta
}

// TestWriteDirSamples: every sample dataset is written, opens and reads
// back exactly; parts end only between groups.
func TestWriteDirSamples(t *testing.T) {
	for _, s := range samples.All() {
		s.Options.AllowUnsynced = unsynced
		dir := filepath.Join(t.TempDir(), s.Name)
		if err := local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options); err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		meta := checkDataset(t, dir, s)
		if ext, ok := meta.Extension("samples"); !ok || !strings.Contains(string(ext), s.Name) {
			t.Errorf("%s: extensions %s", s.Name, meta.Extensions)
		}
		switch s.Name {
		case "empty":
			if len(meta.Parts) != 0 {
				t.Errorf("empty: %d parts", len(meta.Parts))
			}
		case "grouped":
			// Several parts, the group of 5,000 rows inside one of them.
			if len(meta.Parts) < 3 {
				t.Errorf("grouped: %d parts", len(meta.Parts))
			}
			var big bool
			for _, p := range meta.Parts {
				big = big || p.Rows >= 5000
			}
			if !big {
				t.Errorf("grouped: no part holds the 5,000-row group")
			}
		default:
			if len(meta.Parts) < 2 {
				t.Errorf("%s: %d parts", s.Name, len(meta.Parts))
			}
		}
	}
}

func sample(name string) samples.Sample {
	for _, s := range samples.All() {
		if s.Name == name {
			s.Options.AllowUnsynced = unsynced
			return s
		}
	}
	panic(name)
}

// TestWriteDirGroupAtCut: with a tiny part target, every part ends at the
// first new key after each read, so a group straddling a read stays whole
// and a part may exceed the target.
func TestWriteDirGroupAtCut(t *testing.T) {
	s := sample("grouped")
	s.Options.PartBytes = 1
	dir := filepath.Join(t.TempDir(), "d")
	must(t, local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options))
	meta := checkDataset(t, dir, s)
	keys := s.Keys()
	var at int64
	for i, p := range meta.Parts[:len(meta.Parts)-1] {
		at += p.Rows
		if bytes.Equal(keys[at-1], keys[at]) {
			t.Fatalf("part %d ends inside the group of %q", i, keys[at])
		}
		if at%local.ReadRows == 0 && i > 0 {
			t.Errorf("part %d ends at a read boundary %d, not after the next new key", i, at)
		}
	}
}

// TestWriteDirRefuses: rows out of order, an existing destination and an
// invalid schema are refused; a refused destination that WriteDir created
// is removed, one it did not is left alone.
func TestWriteDirRefuses(t *testing.T) {
	s := sample("unique")
	rows := slices.Clone(s.Rows)
	rows[4000], rows[4001] = rows[4001], rows[4000]
	s.Rows = rows
	dir := filepath.Join(t.TempDir(), "d")
	if err := local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options); !errors.Is(err, write.ErrProfile) {
		t.Errorf("rows out of order: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the refused dataset was not removed: %v", err)
	}
	s = sample("unique")
	existing := t.TempDir()
	if err := local.WriteDir(context.Background(), existing, s.Schema, s.Reader(), s.Options); !errors.Is(err, fs.ErrExist) {
		t.Errorf("existing destination: %v", err)
	}
	if _, err := os.Stat(existing); err != nil {
		t.Errorf("the existing destination was removed: %v", err)
	}
	s = sample("empty")
	s.Options.Part.Key.Column = "size" // not a byte string
	if err := local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options); !errors.Is(err, write.ErrProfile) {
		t.Errorf("an empty dataset with an invalid key column: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the refused dataset was not removed: %v", err)
	}
}

// cancelling cancels its context after reading rows rows.
type cancelling struct {
	parquet.RowReader
	rows   int
	cancel context.CancelFunc
}

func (c *cancelling) ReadRows(buf []parquet.Row) (int, error) {
	n, err := c.RowReader.ReadRows(buf)
	if c.rows -= n; c.rows <= 0 {
		c.cancel()
	}
	return n, err
}

func TestWriteDirCancelled(t *testing.T) {
	s := sample("long")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := filepath.Join(t.TempDir(), "d")
	err := local.WriteDir(ctx, dir, s.Schema, &cancelling{RowReader: s.Reader(), rows: 1500, cancel: cancel}, s.Options)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled mid-part: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the cancelled dataset was not removed: %v", err)
	}
}

// failingReader fails after its first read.
type failingReader struct {
	parquet.RowReader
	reads int
}

func (f *failingReader) ReadRows(buf []parquet.Row) (int, error) {
	if f.reads++; f.reads > 1 {
		return 0, errors.New("source failed")
	}
	return f.RowReader.ReadRows(buf)
}

func TestWriteDirSourceFails(t *testing.T) {
	s := sample("unique")
	dir := filepath.Join(t.TempDir(), "d")
	if err := local.WriteDir(context.Background(), dir, s.Schema, &failingReader{RowReader: s.Reader()}, s.Options); err == nil || !strings.Contains(err.Error(), "source failed") {
		t.Errorf("a failing source: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the failed dataset was not removed: %v", err)
	}
}

// TestWriteDirRace: of two writers to one destination, one writes the
// dataset and the other is refused without touching it.
func TestWriteDirRace(t *testing.T) {
	s := sample("unique")
	dir := filepath.Join(t.TempDir(), "d")
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options) })
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) || !errors.Is(errors.Join(errs...), fs.ErrExist) {
		t.Fatalf("two writers: %v, %v", errs[0], errs[1])
	}
	checkDataset(t, dir, s)
}

// installed writes the parts of a small dataset under a new root, without
// a manifest, and returns the root and its manifest.
func installed(t *testing.T) (*os.Root, seekparquet.Manifest) {
	t.Helper()
	s := sample("string-key")
	dir := filepath.Join(t.TempDir(), "d")
	must(t, local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options))
	raw, err := os.ReadFile(filepath.Join(dir, seekparquet.ManifestName))
	must(t, err)
	m, err := seekparquet.ParseManifest(raw, seekparquet.Limits{})
	must(t, err)
	must(t, os.Remove(filepath.Join(dir, seekparquet.ManifestName)))
	root, err := os.OpenRoot(dir)
	must(t, err)
	t.Cleanup(func() { root.Close() })
	return root, m
}

func failAt(step local.Step) func(local.Step) error {
	return func(s local.Step) error {
		if s == step {
			return errors.New("injected at " + string(s))
		}
		return nil
	}
}

func exists(root *os.Root, name string) bool {
	_, err := root.Lstat(filepath.FromSlash(name))
	return err == nil
}

// TestCommitFaults: a failure at each step of Commit leaves no manifest
// and no temporary file, except a failed directory sync, which leaves the
// complete manifest visible; a commit after a failed one succeeds.
func TestCommitFaults(t *testing.T) {
	for step, want := range map[local.Step]local.CommitResult{local.StepWriteManifest: local.Unpublished, local.StepSyncFile: local.Unpublished, local.StepLink: local.Unpublished, local.StepSyncDir: local.Visible} {
		if unsynced && step == local.StepSyncDir {
			continue
		}
		root, m := installed(t)
		result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced, Inject: failAt(step)}, m)
		if err == nil || result != want {
			t.Errorf("%s: %v, %v; want %v", step, result, err, want)
		}
		if exists(root, seekparquet.ManifestName) != (want == local.Visible) {
			t.Errorf("%s: manifest present %v after %v", step, exists(root, seekparquet.ManifestName), result)
		}
		entries, err := fs.ReadDir(root.FS(), ".")
		must(t, err)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".tmp") {
				t.Errorf("%s: temporary file %s left", step, e.Name())
			}
		}
		if want == local.Unpublished {
			if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, m); result != committed || err != nil {
				t.Errorf("%s: commit after the failure: %v, %v", step, result, err)
			}
		}
		if _, err := seekparquet.OpenDir(context.Background(), root.Name(), seekparquet.Options{VerifyOnOpen: true}); err != nil {
			t.Errorf("%s: the committed dataset does not open: %v", step, err)
		}
	}
}

// TestCommitRefuses: a manifest that does not match the installed parts,
// or an existing manifest, is refused without a change.
func TestCommitRefuses(t *testing.T) {
	root, m := installed(t)
	bad := m
	bad.Parts = slices.Clone(m.Parts)
	bad.Parts[1].Bytes++
	if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, bad); result != local.Unpublished || err == nil {
		t.Errorf("wrong size: %v, %v", result, err)
	}
	bad.Parts[1] = m.Parts[1]
	bad.Parts[1].Path = "data/missing.parquet"
	if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, bad); result != local.Unpublished || !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing part: %v, %v", result, err)
	}
	bad = m
	bad.SchemaID = ""
	if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, bad); result != local.Unpublished || seekparquet.CodeOf(err) != seekparquet.CodeInvalidManifest {
		t.Errorf("invalid manifest: %v, %v", result, err)
	}
	if exists(root, seekparquet.ManifestName) {
		t.Fatal("a refused commit installed a manifest")
	}
	if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, m); result != committed || err != nil {
		t.Fatalf("commit: %v, %v", result, err)
	}
	if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, m); result != local.Unpublished || !errors.Is(err, fs.ErrExist) {
		t.Errorf("second commit: %v, %v", result, err)
	}
}

// TestCommitRace: of two commits racing for one root, exactly one
// installs its manifest; the other is refused.
func TestCommitRace(t *testing.T) {
	for range 20 {
		root, m := installed(t)
		other := m
		other.SchemaID = "other"
		results := make([]local.CommitResult, 2)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, mm := range []seekparquet.Manifest{m, other} {
			wg.Go(func() { results[i], errs[i] = local.Commit(local.Ops{Root: root, AllowUnsynced: unsynced}, mm) })
		}
		wg.Wait()
		won := 0
		for i := range errs {
			switch {
			case errs[i] == nil && results[i] == committed:
				won++
			case errors.Is(errs[i], fs.ErrExist) && results[i] == local.Unpublished:
			default:
				t.Fatalf("commit %d: %v, %v", i, results[i], errs[i])
			}
		}
		if won != 1 {
			t.Fatalf("%d commits won", won)
		}
	}
}

// TestPublishPartFaults: a failure before the rename leaves the part at
// its temporary path; a failed directory sync leaves it visible at its
// final path; an existing final path is refused.
func TestPublishPartFaults(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	must(t, err)
	defer root.Close()
	must(t, root.Mkdir("data", 0o755))
	for step, visible := range map[local.Step]bool{local.StepSyncFile: false, local.StepRename: false, local.StepSyncDir: true} {
		if unsynced && step == local.StepSyncDir {
			continue
		}
		name := strings.ReplaceAll(string(step), " ", "-")
		tmp, final := "data/"+name+".tmp", "data/"+name+".parquet"
		must(t, root.WriteFile(tmp, []byte("part"), 0o644))
		err := local.PublishPart(local.Ops{Root: root, AllowUnsynced: unsynced, Inject: failAt(step)}, tmp, final)
		if err == nil || exists(root, final) != visible || exists(root, tmp) == visible {
			t.Errorf("%s: %v; final present %v, temporary present %v", step, err, exists(root, final), exists(root, tmp))
		}
	}
	must(t, root.WriteFile("data/a.tmp", []byte("a"), 0o644))
	must(t, root.WriteFile("data/b.tmp", []byte("b"), 0o644))
	must(t, local.PublishPart(local.Ops{Root: root, AllowUnsynced: unsynced}, "data/a.tmp", "data/x.parquet"))
	if err := local.PublishPart(local.Ops{Root: root, AllowUnsynced: unsynced}, "data/b.tmp", "data/x.parquet"); !errors.Is(err, fs.ErrExist) {
		t.Errorf("publishing over a part: %v", err)
	}
	if got, _ := root.ReadFile("data/x.parquet"); string(got) != "a" {
		t.Errorf("the part was replaced")
	}
	if err := local.PublishPart(local.Ops{Root: root, AllowUnsynced: unsynced}, "data/b.tmp", "../outside.parquet"); err == nil {
		t.Errorf("published outside the root")
	}
}

// TestUnsupportedDirSync: where directories cannot be synced, publication
// is refused before anything changes, unless the caller accepts a visible
// but unproven commit.
func TestUnsupportedDirSync(t *testing.T) {
	defer func(v bool) { *local.DirSyncSupported = v }(*local.DirSyncSupported)
	root, m := installed(t)
	*local.DirSyncSupported = false
	if result, err := local.Commit(local.Ops{Root: root}, m); result != local.Unpublished || !errors.Is(err, local.ErrDirSyncUnsupported) {
		t.Errorf("commit: %v, %v", result, err)
	}
	if err := local.PublishPart(local.Ops{Root: root}, m.Parts[0].Path, "data/other.parquet"); !errors.Is(err, local.ErrDirSyncUnsupported) || !exists(root, m.Parts[0].Path) {
		t.Errorf("publish: %v", err)
	}
	s := sample("unique")
	s.Options.AllowUnsynced = false
	dir := filepath.Join(t.TempDir(), "d")
	if err := local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options); !errors.Is(err, local.ErrDirSyncUnsupported) {
		t.Errorf("local.WriteDir: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("local.WriteDir created its destination: %v", err)
	}
	if result, err := local.Commit(local.Ops{Root: root, AllowUnsynced: true}, m); result != local.Visible || err != nil {
		t.Errorf("commit, unsynced allowed: %v, %v", result, err)
	}
	s.Options.AllowUnsynced = true
	must(t, local.WriteDir(context.Background(), dir, s.Schema, s.Reader(), s.Options))
	checkDataset(t, dir, s)
}
