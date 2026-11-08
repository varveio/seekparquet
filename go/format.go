// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "github.com/varveio/seekparquet/go/internal/manifest"

// Format is the manifest's "format" member (MAN-4).
const Format = manifest.Format

// FormatVersion and DraftRevision are the version this package reads and
// writes (§9): format 1, draft revision 1, which files carry as
// format_version 0 and draft_revision 1 until the format is final.
const (
	FormatVersion int64 = 0
	DraftRevision int64 = 1
)

// ManifestName is the name of the manifest in a dataset root (LAYOUT-1).
const ManifestName = "manifest.json"

// The footer key-value stamps every part carries (STAMP-1), each exactly
// once (STAMP-2). StampDraftRevision appears only when the format version
// is 0.
const (
	StampFormatVersion = "seekparquet.format_version"
	StampDraftRevision = "seekparquet.draft_revision"
	StampSchemaID      = "seekparquet.schema_id"
	StampKeyColumn     = "seekparquet.key_column"
	StampKeyUnique     = "seekparquet.key_unique"
)

// stampKeys are the stamps in the order they are checked.
var stampKeys = []string{StampFormatVersion, StampDraftRevision, StampSchemaID, StampKeyColumn, StampKeyUnique}
