// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/write"
)

// defaultPartBytes is DatasetOptions.PartBytes's default.
const defaultPartBytes = 128 << 20

// readRows is how many rows WriteDir reads at a time; a part's size is
// checked between them.
const readRows = 1024

// DatasetOptions configures WriteDir.
type DatasetOptions struct {
	// Part configures every part: the key, the schema id, the codec and
	// the geometry (write.PartOptions). Key and SchemaID are required.
	Part write.PartOptions
	// PartBytes is the size at which a part ends, on the writer's estimate
	// (write.PartWriter.Size): the part ends before the first row whose
	// key differs from the last row's, so that a group of equal keys never
	// crosses parts (PART-4) and a large group can exceed it. 0 is
	// 128 MiB.
	PartBytes int64
	// Extensions is the manifest's extensions object (§3.4), nil for none.
	Extensions json.RawMessage
	// AllowUnsynced accepts publishing where directories cannot be synced
	// (Ops.AllowUnsynced).
	AllowUnsynced bool
}

// WriteDir writes the rows rows yields as a new dataset at dir, which must
// not exist (its parent must): rows must already be in key order (KEY-3),
// as the parquet-go rows of schema. It creates dir, syncs its parent, and
// creates and syncs data/; writes the parts as data/part-NNNNN.parquet
// (LAYOUT-3), publishes each (PublishPart) and commits the manifest
// (Commit), with the parts' SHA-256 digests. With no rows it commits an
// empty dataset.
//
// WriteDir does not sort, deduplicate or merge rows, write to an object
// store, or replace or extend an existing dataset; a second writer to the
// same dir is refused with fs.ErrExist. When it fails before the commit,
// including when ctx is cancelled, it removes dir; a failure after the
// manifest became visible leaves the dataset in place.
func WriteDir(ctx context.Context, dir string, schema *parquet.Schema, rows parquet.RowReader, opt DatasetOptions) (err error) {
	if !dirSyncSupported && !opt.AllowUnsynced {
		return ErrDirSyncUnsupported
	}
	if opt.PartBytes <= 0 {
		opt.PartBytes = defaultPartBytes
	}
	leaf, ok := schema.Lookup(opt.Part.Key.Column)
	if !ok {
		return fmt.Errorf("%w: KEY-1: the schema has no column %q", write.ErrProfile, opt.Part.Key.Column)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return fmt.Errorf("seekparquet/local: create the dataset: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return errors.Join(err, os.Remove(dir))
	}
	d := &dirWriter{ops: Ops{Root: root, AllowUnsynced: opt.AllowUnsynced}, schema: schema, opt: opt, keyColumn: leaf.ColumnIndex}
	installed := false
	defer func() {
		d.abort()
		err = errors.Join(err, root.Close())
		if !installed {
			err = errors.Join(err, os.RemoveAll(dir))
		}
	}()
	if err := root.Mkdir("data", 0o755); err != nil {
		return err
	}
	// The dataset root's entry in its parent, and data's in the root, are
	// made durable before any part is published, so that a durable
	// manifest never names a part a power loss can unlink.
	if dirSyncSupported {
		if err := syncPath(filepath.Dir(dir)); err != nil {
			return fmt.Errorf("seekparquet/local: sync the parent of %s: %w", dir, err)
		}
		if err := d.ops.syncDir("."); err != nil {
			return fmt.Errorf("seekparquet/local: sync %s: %w", dir, err)
		}
	}
	// The first part is started before any row is read, so that the
	// schema and options are checked even for an empty dataset.
	if err := d.start(); err != nil {
		return err
	}
	buf := make([]parquet.Row, readRows)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := rows.ReadRows(buf)
		if err := d.add(buf[:n]); err != nil {
			return err
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.rows > 0 {
		if err := d.seal(); err != nil {
			return err
		}
	} else {
		d.abort()
		if err := root.Remove(d.temporary); err != nil {
			return err
		}
	}
	m := seekparquet.Manifest{Key: opt.Part.Key, Version: seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision},
		SchemaID: opt.Part.SchemaID, Parts: d.parts, Extensions: opt.Extensions}
	installed, err = Commit(d.ops, m)
	return err
}

// dirWriter is the state of one WriteDir.
type dirWriter struct {
	ops       Ops
	schema    *parquet.Schema
	opt       DatasetOptions
	keyColumn int

	part      *write.PartWriter // the open part, nil between parts
	temporary string            // its temporary path
	rows      int64             // rows in the open part
	full      bool              // the open part has reached PartBytes
	last      []byte            // the last row's key
	parts     []seekparquet.PartInfo
}

func partPath(i int) string { return fmt.Sprintf("data/part-%05d.parquet", i) }

// start opens the next part.
func (d *dirWriter) start() error {
	d.temporary = fmt.Sprintf("data/.part-%05d.parquet.tmp", len(d.parts))
	f, err := d.ops.Root.OpenFile(d.temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if d.part, err = write.NewPart(f, d.schema, d.opt.Part); err != nil {
		return errors.Join(err, f.Close())
	}
	d.rows, d.full = 0, false
	return nil
}

// add writes rows, ending the open part before the first row of a new key
// once the part is full.
func (d *dirWriter) add(rows []parquet.Row) error {
	from := 0
	for i, row := range rows {
		key := d.keyOf(row)
		if d.full && !bytes.Equal(key, d.last) {
			if err := d.write(rows[from:i]); err != nil {
				return err
			}
			if key != nil && bytes.Compare(key, d.last) < 0 {
				return fmt.Errorf("%w: KEY-3: key %q is below the key before it, %q", write.ErrProfile, key, d.last)
			}
			if err := d.seal(); err != nil {
				return err
			}
			if err := d.start(); err != nil {
				return err
			}
			from = i
		}
		d.last = append(d.last[:0], key...)
	}
	if err := d.write(rows[from:]); err != nil {
		return err
	}
	if d.part != nil && d.part.Size() >= d.opt.PartBytes {
		d.full = true
	}
	return nil
}

func (d *dirWriter) write(rows []parquet.Row) error {
	n, err := d.part.WriteRows(rows)
	d.rows += int64(n)
	return err
}

// keyOf returns row's key, or nil when the row has none of the key's
// kind (the part writer then refuses the row).
func (d *dirWriter) keyOf(row parquet.Row) []byte {
	for _, v := range row {
		if v.Column() == d.keyColumn && v.Kind() == parquet.ByteArray {
			return v.ByteArray()
		}
	}
	return nil
}

// seal seals and publishes the open part.
func (d *dirWriter) seal() error {
	info, err := d.part.Seal()
	d.part = nil
	if err != nil {
		return err
	}
	final := partPath(len(d.parts))
	if err := PublishPart(d.ops, d.temporary, final); err != nil {
		return err
	}
	info.Path = final
	d.parts = append(d.parts, info)
	return nil
}

// abort abandons the open part, if any. Its temporary file goes with the
// dataset directory.
func (d *dirWriter) abort() {
	if d.part != nil {
		_ = d.part.Abort()
		d.part = nil
	}
}
