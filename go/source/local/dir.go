// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local reads SeekParquet datasets from the local filesystem: Dir
// is a seekparquet.Source over a directory, and FileCache bounds the file
// descriptors its parts hold open.
package local

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/varveio/seekparquet/go"
)

// Dir is a dataset root on the local filesystem. Every open goes through an
// os.Root of the directory, so no path, and no symbolic link inside the
// directory, can make it read a file outside it (PART-1). Files are opened
// without blocking on a special file and must be regular files; a path
// that names anything else is reported as not existing, which OpenDataset
// refuses as no_manifest or missing_part. It is safe for concurrent use.
type Dir struct {
	path  string
	cache *FileCache
}

// NewDir returns the dataset root at path, opening its files through
// cache; a nil cache is a cache of its own with DefaultMaxOpenFiles.
func NewDir(path string, cache *FileCache) *Dir {
	if cache == nil {
		cache = NewFileCache(0)
	}
	return &Dir{path: path, cache: cache}
}

// Cache returns the descriptor cache the directory's files are opened
// through.
func (d *Dir) Cache() *FileCache { return d.cache }

// Open opens the file at the slash-separated path relative to the
// directory, as a *CachedFile.
func (d *Dir) Open(ctx context.Context, path string) (seekparquet.Part, error) {
	open := func() (*os.File, error) {
		root, err := os.OpenRoot(d.path)
		if err != nil {
			return nil, err
		}
		defer func() { _ = root.Close() }()
		return root.OpenFile(filepath.FromSlash(path), openFlags, 0)
	}
	f, err := d.cache.Open(ctx, filepath.Join(d.path, filepath.FromSlash(path)), open)
	if err != nil {
		if errorsIsNotRegular(err) {
			return nil, &os.PathError{Op: "open", Path: path, Err: notExist{err}}
		}
		return nil, err
	}
	return f, nil
}

// OpenDataset opens the dataset in the directory at path with a descriptor
// cache of its own: seekparquet.OpenDataset over NewDir(path, nil).
func OpenDataset(ctx context.Context, path string, opt seekparquet.Options) (*seekparquet.Dataset, error) {
	return seekparquet.OpenDataset(ctx, NewDir(path, nil), opt)
}

func errorsIsNotRegular(err error) bool { return errors.Is(err, ErrNotRegular) }

// notExist reports a path that names something other than a regular file
// as not existing, keeping ErrNotRegular in its chain.
type notExist struct{ err error }

func (e notExist) Error() string        { return e.err.Error() }
func (e notExist) Unwrap() error        { return e.err }
func (e notExist) Is(target error) bool { return target == fs.ErrNotExist }
