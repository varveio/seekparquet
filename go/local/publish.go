// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local publishes SeekParquet datasets in a local directory,
// durably and confined to the dataset root, with swath's protocol: parts
// first, each synced and renamed into place with its directory synced,
// then the manifest, written whole, synced and renamed into place, which
// commits the dataset (COMMIT-1), and its directory synced. A dataset root
// belongs to one writer at a time; WriteDir, which composes these steps
// with package write, writes only into a directory it creates.
package local

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

// step names one step of a publication, for fault tests (inject).
type step string

// The steps of WriteDir, PublishPart and Commit.
const (
	stepWriteManifest step = "write manifest" // write the manifest's temporary file
	stepSyncFile      step = "sync file"      // sync a part, or the manifest's temporary file
	stepRename        step = "rename"         // rename a part or the manifest into place
	stepSyncDir       step = "sync directory" // sync a directory
)

// inject, when set (by tests), runs before each step with the step and
// the path it acts on, and fails the step with its error, as if the step
// had failed.
var inject func(s step, path string) error

// Ops is how publication reaches the file system. Every path is
// slash-separated and relative to Root, which confines it (PART-1).
type Ops struct {
	Root *os.Root
	// AllowUnsynced lets publication go ahead where directories cannot be
	// synced (ErrDirSyncUnsupported): files are then made visible, but not
	// proven to survive a power loss. Without it, publication there is
	// refused before anything changes.
	AllowUnsynced bool
}

// dirSyncSupported reports whether directories can be synced here (a
// variable for tests).
var dirSyncSupported = dirSync

func at(s step, path string) error {
	if inject == nil {
		return nil
	}
	return inject(s, path)
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
	if err := at(stepSyncDir, dir); err != nil {
		return err
	}
	d, err := o.Root.Open(filepath.FromSlash(dir))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// syncPath syncs the directory at path, outside any root: the parent of a
// dataset root WriteDir creates.
func syncPath(dir string) error {
	if err := at(stepSyncDir, dir); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// PublishPart installs a written part: it syncs the file at temporary,
// renames it to final and syncs final's directory, so that the part is
// durable under its final path before a manifest lists it (COMMIT-1). An
// error leaves the part unpublished, or, when the directory sync failed,
// at final but not proven durable; either way the caller does not list
// it. final's directory must exist.
func PublishPart(ops Ops, temporary, final string) error {
	durable, err := ops.durable()
	if err != nil {
		return err
	}
	if err := at(stepSyncFile, temporary); err != nil {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	f, err := ops.Root.OpenFile(filepath.FromSlash(temporary), os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("publish %s: %w", final, err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("publish %s: sync: %w", final, err)
	}
	if err := at(stepRename, final); err != nil {
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

// Commit publishes the manifest of parts already installed under the root
// (by PublishPart) and never changed afterwards (COMMIT-2). It marshals m
// (MarshalManifest, which refuses a manifest this module would not read),
// requires every part to be a regular file of the size m gives it, writes
// the manifest to a temporary file beside its target, syncs it, renames it
// to manifest.json, which commits the dataset atomically (COMMIT-1), and
// syncs the root. The rename replaces a manifest already there: keeping a
// dataset immutable (COMMIT-3) is the caller's, who owns the root, as
// WriteDir owns the directory it creates.
//
// installed reports whether the manifest is in place, complete: readers
// see the dataset. With an error, installed is true only when the root's
// sync failed after the rename, so the dataset is visible but not proven
// to survive a power loss, and the caller must not remove it. Durability
// is the file system's own: a synced file on tmpfs does not survive a
// reboot. Under Ops.AllowUnsynced the root is not synced at all.
func Commit(ops Ops, m seekparquet.Manifest) (installed bool, err error) {
	durable, err := ops.durable()
	if err != nil {
		return false, err
	}
	raw, err := seekparquet.MarshalManifest(m)
	if err != nil {
		return false, err
	}
	for _, p := range m.Parts {
		fi, err := ops.Root.Lstat(filepath.FromSlash(p.Path))
		if err != nil {
			return false, fmt.Errorf("commit: part %s: %w", p.Path, err)
		}
		if !fi.Mode().IsRegular() || fi.Size() != p.Bytes {
			return false, fmt.Errorf("commit: part %s is not a regular file of %d bytes", p.Path, p.Bytes)
		}
	}
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	tmp := "." + seekparquet.ManifestName + "." + hex.EncodeToString(nonce[:]) + ".tmp"
	f, err := ops.Root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	err = at(stepWriteManifest, tmp)
	if err == nil {
		var n int
		if n, err = f.Write(raw); err == nil && n != len(raw) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = at(stepSyncFile, tmp)
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err == nil {
		err = at(stepRename, seekparquet.ManifestName)
	}
	if err == nil {
		err = ops.Root.Rename(tmp, seekparquet.ManifestName)
	}
	if err != nil {
		_ = ops.Root.Remove(tmp)
		return false, fmt.Errorf("commit: write %s: %w", seekparquet.ManifestName, err)
	}
	if durable {
		if err := ops.syncDir("."); err != nil {
			return true, fmt.Errorf("commit: sync the dataset root: %w", err)
		}
	}
	return true, nil
}
