// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "bytes"

// Cursor resumes a query after one row or common prefix. Exactly one of
// Key and Prefix is non-nil. Offset is the row's zero-based position in
// its equal-key group in file order, and is zero for a prefix cursor.
// Cursors refer to the same immutable dataset and query that produced them.
type Cursor struct {
	Key    []byte
	Offset int64
	Prefix []byte
}

func (c *Cursor) check() error {
	if c == nil {
		return nil
	}
	if (c.Key == nil) == (c.Prefix == nil) || c.Offset < 0 || c.Prefix != nil && c.Offset != 0 {
		return invalidRequest("invalid continuation cursor")
	}
	return nil
}

func (c *Cursor) allows(key []byte, offset int64) bool {
	if c == nil {
		return true
	}
	if c.Prefix != nil {
		return bytes.Compare(key, c.Prefix) > 0
	}
	cmp := bytes.Compare(key, c.Key)
	return cmp > 0 || cmp == 0 && offset > c.Offset
}

func (c *Cursor) floor() ([]byte, bool) {
	if c.Prefix != nil {
		return c.Prefix, true
	}
	return c.Key, false
}

// Cursor returns a continuation after this row, owning its key bytes.
func (row Row) Cursor() *Cursor { return &Cursor{Key: bytes.Clone(row.Key), Offset: row.Offset} }

// Cursor returns a continuation after this entry, owning its bytes.
func (e Entry) Cursor() *Cursor {
	if e.Row != nil {
		return e.Row.Cursor()
	}
	return &Cursor{Prefix: bytes.Clone(e.CommonPrefix)}
}

// groupPosition tracks file-order offsets while walking complete groups.
type groupPosition struct {
	key    []byte
	offset int64
}

func (p *groupPosition) next(key []byte) int64 {
	if bytes.Equal(p.key, key) {
		p.offset++
	} else {
		p.key, p.offset = bytes.Clone(key), 0
	}
	return p.offset
}
