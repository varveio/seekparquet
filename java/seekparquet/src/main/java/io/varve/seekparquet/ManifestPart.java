// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One part entry of a manifest (spec §3.3).
 *
 * @param path the part's path relative to the dataset root, literal
 * @param rows the exact number of rows in the part, at least 1
 * @param bytes the exact size of the part file in bytes, at least 1
 * @param minKey the part's exact first key
 * @param maxKey the part's exact last key
 * @param sha256 the lowercase hexadecimal SHA-256 of the part file, when declared
 * @param extensions the part's extension namespaces, each mapped to its value's JSON text exactly as the
 *     manifest spells it, in manifest order; empty when the part has none. Exposed unchanged (EXT-2); the
 *     reader ignores them (EXT-3)
 */
public record ManifestPart(String path, long rows, long bytes, Bytes minKey, Bytes maxKey, Optional<String> sha256,
        Map<String, String> extensions) {

    /**
     * Creates a part entry. Only nullness is checked here; {@link Manifest#parse} checks the rest.
     *
     * @param path the path
     * @param rows the row count
     * @param bytes the size
     * @param minKey the first key
     * @param maxKey the last key
     * @param sha256 the digest
     * @param extensions the extensions
     */
    public ManifestPart {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(minKey, "minKey");
        Objects.requireNonNull(maxKey, "maxKey");
        Objects.requireNonNull(sha256, "sha256");
        extensions = Manifest.extensionsOf(extensions);
    }
}
