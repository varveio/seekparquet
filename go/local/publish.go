// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local publishes SeekParquet datasets in a local directory,
// durably and confined to the dataset root: parts first, each synced and
// moved into place with its directory synced, then the manifest, written
// whole, synced, and linked into place, which commits the dataset
// (COMMIT-1). A manifest is never replaced: a dataset root is committed
// once (COMMIT-3). WriteDir composes these steps with package write.
package local

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	seekparquet "github.com/varveio/seekparquet/go"
)

// ErrDirSyncUnsupported reports a platform where a directory cannot be
// synced, so a rename or a link cannot be made durable: Windows today,
// where Go has no directory fsync. Publication is refused there unless
// Ops.AllowUnsynced accepts the weaker outcome.
var ErrDirSyncUnsupported = errors.New("seekparquet/local: directory sync is not supported on this platform")

// Step names one step of a publication, for Ops.Inject.
type Step string

// The steps of PublishPart and Commit, in the order they run.
const (
	StepWriteManifest Step = "write manifest" // write the manifest's temporary file
	StepSyncFile      Step = "sync file"      // sync a part, or the manifest's temporary file
	StepRename        Step = "rename part"    // move a part to its final path
	StepLink          Step = "link manifest"  // link the manifest into place: the commit
	StepSyncDir       Step = "sync directory" // sync the directory of a part or of the manifest
)

// Ops is how publication reaches the file system. Every path is
// slash-separated and relative to Root, which confines it (PART-1).
type Ops struct {
	Root *os.Root
	// AllowUnsynced lets publication go ahead where directories cannot be
	// synced (ErrDirSyncUnsupported): files are then made visible, but not
	// proven to survive a power loss, and Commit reports Visible rather
	// than Durable. Without it, publication there is refused before
	// anything changes.
	AllowUnsynced bool
	// Inject, when set, runs before each step and fails the step with its
	// error, as if the step had failed: for fault tests.
	Inject func(Step) error
}

// dirSyncSupported reports whether directories can be synced here (a
// variable for tests).
var dirSyncSupported = dirSync

func (o Ops) step(s Step) error {
	if o.Inject == nil {
		return nil
	}
	return o.Inject(s)
}

// durable reports whether directory syncs will run, or refuses.
func (o Ops) durable() (bool, error) {
	if dirSyncSupported {
		return true, nil
	}
	if o.AllowUnsynced {
		return false, nil
	}
	return false, ErrDirSyncUnsupported
}

func (o Ops) syncDir(dir string) error {
	if err := o.step(StepSyncDir); err != nil {
		return err
	}
	d, err := o.Root.Open(filepath.FromSlash(dir))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// PublishPart installs a written part: it syncs the file at temporary,
// renames it to final, which must not exist yet, and syncs final's
// directory, so that the part is durable under its final path before a
// manifest lists it (COMMIT-1). An error leaves the part unpublished, or,
// when the directory sync failed, visible at final but not proven
// durable; either way the caller does not list it. The check that final
// does not exist is not atomic: the destination belongs to one writer.
func PublishPart(ops Ops, temporary, final string) error {
	durable, err := ops.durable()
	if err != nil {
		return err
	}
	if err := ops.step(StepSyncFile); err != nil {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	f, err := ops.Root.OpenFile(filepath.FromSlash(temporary), os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("publish %s: sync: %w", final, err)
	}
	if _, err := ops.Root.Lstat(filepath.FromSlash(final)); err == nil {
		return fmt.Errorf("publish %s: %w", final, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	if err := ops.step(StepRename); err != nil {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	if err := ops.Root.Rename(filepath.FromSlash(temporary), filepath.FromSlash(final)); err != nil {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	if durable {
		if err := ops.syncDir(path.Dir(final)); err != nil {
			return fmt.Errorf("publish %s: sync its directory: %w", final, err)
		}
	}
	return nil
}

// CommitResult is what a Commit left behind.
type CommitResult int

const (
	// Unpublished: no manifest was installed; the root holds no dataset
	// (LAYOUT-1).
	Unpublished CommitResult = iota
	// Visible: the manifest is installed, complete, and readers see the
	// dataset, but it is not proven to survive a power loss (its
	// directory sync failed, or was skipped under Ops.AllowUnsynced).
	Visible
	// Durable: the manifest is installed and its directory synced.
	Durable
)

func (r CommitResult) String() string {
	switch r {
	case Unpublished:
		return "unpublished"
	case Visible:
		return "visible"
	case Durable:
		return "durable"
	}
	return fmt.Sprintf("CommitResult(%d)", int(r))
}

// Commit publishes the manifest of parts already installed under the root
// (by PublishPart) and never changed afterwards (COMMIT-2). It marshals m
// (MarshalManifest, which refuses a manifest this module would not read),
// requires every part to be a regular file of the size m gives it, writes
// the manifest to a temporary file, syncs it, links it into place as
// manifest.json, which commits the dataset atomically, and syncs the root
// (COMMIT-1). An existing manifest is never replaced: Commit is refused
// with fs.ErrExist, also when another writer commits first.
//
// The result says what an error left behind: Unpublished (nothing
// committed) or Visible (committed, durability unknown). Without an error
// it is Durable, or Visible under Ops.AllowUnsynced where directories
// cannot be synced. The temporary file is removed; a crash may leave one,
// which readers ignore.
func Commit(ops Ops, m seekparquet.Manifest) (CommitResult, error) {
	durable, err := ops.durable()
	if err != nil {
		return Unpublished, err
	}
	raw, err := seekparquet.MarshalManifest(m)
	if err != nil {
		return Unpublished, err
	}
	for _, p := range m.Parts {
		fi, err := ops.Root.Lstat(filepath.FromSlash(p.Path))
		if err != nil {
			return Unpublished, fmt.Errorf("commit: part %s: %w", p.Path, err)
		}
		if !fi.Mode().IsRegular() || fi.Size() != p.Bytes {
			return Unpublished, fmt.Errorf("commit: part %s is not a regular file of %d bytes", p.Path, p.Bytes)
		}
	}
	if _, err := ops.Root.Lstat(seekparquet.ManifestName); err == nil {
		return Unpublished, fmt.Errorf("commit: %s: %w", seekparquet.ManifestName, fs.ErrExist)
	}
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	tmp := "." + seekparquet.ManifestName + "." + hex.EncodeToString(nonce[:]) + ".tmp"
	f, err := ops.Root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return Unpublished, fmt.Errorf("commit: %w", err)
	}
	defer func() { _ = ops.Root.Remove(tmp) }()
	err = ops.step(StepWriteManifest)
	if err == nil {
		var n int
		if n, err = f.Write(raw); err == nil && n != len(raw) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = ops.step(StepSyncFile)
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return Unpublished, fmt.Errorf("commit: write %s: %w", seekparquet.ManifestName, err)
	}
	if err := ops.step(StepLink); err != nil {
		return Unpublished, fmt.Errorf("commit: %w", err)
	}
	if err := ops.Root.Link(tmp, seekparquet.ManifestName); err != nil {
		return Unpublished, fmt.Errorf("commit: %w", err)
	}
	if !durable {
		return Visible, nil
	}
	if err := ops.syncDir("."); err != nil {
		return Visible, fmt.Errorf("commit: sync the dataset root: %w", err)
	}
	return Durable, nil
}
