// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// OpenDir opens the dataset in the local directory at path: OpenDataset
// over a Source that opens every file through an os.Root of the directory,
// so no path, and no symbolic link inside the directory, reads a file
// outside it (PART-1). Files are opened without blocking on a special file
// and must be regular files; a path that names anything else is reported
// as not existing, which OpenDataset refuses as no_manifest or
// missing_part. The directory stays open, so that parts can be opened when
// a read first touches them, and each part keeps its file open, until the
// dataset closes.
//
// A local read costs little: OpenDir reads a part's end in 64 KiB unless
// opt.Read.MetadataPrefetchBytes says otherwise, and with a zero opt.Read
// it reads one range at a time, as it was read before the storage
// capabilities, keeping no part metadata beyond the footer and no request
// memo. Any field of opt.Read set gives the policy of OpenDataset.
func OpenDir(ctx context.Context, path string, opt OpenOptions) (*Dataset, error) {
	local := opt.Read == ReadPolicy{}
	if opt.Read.MetadataPrefetchBytes == 0 {
		opt.Read.MetadataPrefetchBytes = footerWindow
	}
	st, spare := takeStats(opt.OpenStats)
	defer dropStats(spare)
	if err := prepareOpen(ctx, &opt); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	d, err := openDataset(ctx, dirSource{root}, opt, st, local)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	d.r.root = root
	return d, nil
}

// dirSource is a Source over an os.Root.
type dirSource struct{ root *os.Root }

func (d dirSource) Open(ctx context.Context, path string) (SourceFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := d.root.OpenFile(filepath.FromSlash(path), openFlags, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = &fs.PathError{Op: "open", Path: path, Err: errNotRegular}
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return localFile{f, info.Size()}, nil
}

// errNotRegular reports a path that names something other than a regular
// file, as not existing.
var errNotRegular = fmt.Errorf("not a regular file: %w", fs.ErrNotExist)

// localFile is an opened regular file and its size.
type localFile struct {
	*os.File
	size int64
}

func (f localFile) Size() int64 { return f.size }
