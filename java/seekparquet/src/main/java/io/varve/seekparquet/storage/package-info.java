// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

/**
 * The storage boundary of the reader: how it reads a dataset's manifest and parts.
 *
 * <p>The reader core is storage-agnostic. It reads through {@link
 * io.varve.seekparquet.storage.DatasetStorage}, which reads the manifest and opens parts, and {@link
 * io.varve.seekparquet.storage.PartSource}, which serves positioned reads of one opened part. {@link
 * io.varve.seekparquet.storage.LocalStorage} implements both over a local directory. Other storage
 * (object stores, caches, request coalescing) is added by implementing or decorating these two
 * interfaces, in separate modules; the core needs no change.
 */
package io.varve.seekparquet.storage;
