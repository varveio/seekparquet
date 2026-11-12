// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local publishes SeekParquet parts and manifests in a local
// directory confined by os.Root. Use [WriteDir] for a complete dataset from
// sorted rows, or [PublishPart] and [Commit] for caller-managed parts.
// See the [producing guide] for runnable examples.
//
// # Write a dataset
//
// [WriteDir] creates a directory that must not exist, writes sorted input
// through package github.com/varveio/seekparquet/go/write, and rotates parts
// at key changes after reaching the configured size target. A key group
// stays whole (PART-4). It does not sort, merge, replace, or extend datasets.
// [DatasetOptions] controls part sizing and writing. Its error contract
// distinguishes failures before publication from a visible manifest whose
// final directory sync failed. Serialize writers to a dataset root.
//
// # Publish parts and commit a manifest
//
// [Ops] supplies the root and directory-sync policy. PublishPart syncs a
// completed part, renames it to its final path, and syncs its directory.
// Commit checks the manifest and listed part sizes, writes and syncs a
// temporary manifest, renames it into place, then syncs the root (COMMIT-1).
// It returns whether the manifest became visible, including when the final
// sync fails. See [Commit] before deciding whether failure cleanup is safe.
// Replacement, retention, and deletion policy belong to the caller
// (COMMIT-3); these functions do not implement a snapshot lifecycle.
//
// # Durability and platforms
//
// Durability depends on the filesystem's file and directory sync semantics.
// [ErrDirSyncUnsupported] reports platforms without directory sync support.
// [Ops.AllowUnsynced] permits publication there with visibility but without
// proven durability. Existing open readers still require immutable part
// bytes (COMMIT-2). See the [producing guide] for publication responsibilities.
//
// [producing guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/producing.md
package local
