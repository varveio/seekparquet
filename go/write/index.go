// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package write

import (
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// keyed is a schema whose top-level field key (by position) is the key
// column: its ColumnIndex says ASCENDING when it has a single page, as
// well as when parquet-go finds several pages ascending, since some older
// readers expect it of a sorted column (RG-6: a writer SHOULD set
// ASCENDING when both bound sequences are non-decreasing, which the exact
// bounds of sorted keys always are).
type keyed struct {
	parquet.Node
	key int
}

func (k keyed) Fields() []parquet.Field {
	fields := append([]parquet.Field(nil), k.Node.Fields()...)
	fields[k.key] = ascendingKey{fields[k.key]}
	return fields
}

type ascendingKey struct{ parquet.Field }

func (f ascendingKey) Type() parquet.Type { return ascendingType{f.Field.Type()} }

type ascendingType struct{ parquet.Type }

func (t ascendingType) NewColumnIndexer(sizeLimit int) parquet.ColumnIndexer {
	return ascendingIndexer{t.Type.NewColumnIndexer(sizeLimit)}
}

type ascendingIndexer struct{ parquet.ColumnIndexer }

func (ix ascendingIndexer) ColumnIndex() format.ColumnIndex {
	index := ix.ColumnIndexer.ColumnIndex()
	if len(index.NullPages) == 1 {
		index.BoundaryOrder = format.Ascending
	}
	return index
}

// exactBounds is the ColumnIndex bound length of every column: no limit,
// so each page's bounds are its exact minimum and maximum. parquet-go
// shortens a longer maximum by incrementing its last kept byte, and one
// whose kept bytes are all 0xFF stays below the value it bounds, which is
// not a bound (RG-5).
func exactBounds([]string) int { return 0 }
