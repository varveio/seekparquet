// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package write writes SeekParquet part files from schema-correct rows
// already in unsigned key order. It accepts parquet-go schemas and values;
// the reader API uses seekparquet's own value types. See the [producing guide].
//
// # Write and seal a part
//
// [NewPart] installs the profile stamps and sorting metadata (STAMP-1,
// KEY-5, RG-1). It writes ColumnIndex and OffsetIndex on every column with
// exact page bounds (RG-5). [PartOptions] selects encoding and geometry.
// [PartWriter.WriteRows] copies input byte values and checks keys for
// nonempty values, order, and required UTF-8 (KEY-2, KEY-3, KEY-6).
// [PartWriter.WritePage] fills columns without an owning copy of every row;
// its caller supplies schema-correct values and any required valid UTF-8.
// Inputs must remain unchanged until the call returns.
//
// [PartWriter.Seal] writes the footer, syncs and closes the file, and returns
// part metadata including exact bounds and SHA-256. The caller supplies its
// final path. On a write error, abort the part with [PartWriter.Abort] and
// remove the unfinished file. [ErrProfile] identifies checked schema or key
// violations. See each method for its error and ownership contract.
//
// # Memory and concurrency
//
// The writer retains a page of rows and encoded pages for a row group.
// Geometry is a target; a single large value can exceed a byte target.
// [PartOptions.ColumnWorkers] controls internal column encoding. A
// WritePage fill callback must support concurrent calls for different
// columns when workers are enabled. Serialize calls on a PartWriter.
//
// # Publication
//
// Writing a part does not publish a dataset. Use package
// github.com/varveio/seekparquet/go/local for local publication and manifest
// commit, or provide the storage equivalent of COMMIT-1. Part rotation and
// keeping key groups within parts are the caller's responsibility; local's
// WriteDir composes those tasks. See the [producing guide] for examples.
//
// [producing guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/producing.md
package write
