// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A parsed and validated {@code manifest.json} (spec §3): the dataset's commit.
 *
 * <p>Extensions are exposed unchanged (EXT-2), the top-level object here and each part's on {@link
 * ManifestPart#extensions()}: each namespace maps to its value's JSON text exactly
 * as the manifest spells it, so that a consumer can apply its own acceptance policy with the JSON
 * library of its choice. The reader itself ignores them (EXT-1).
 *
 * @param version the format version and draft revision
 * @param schemaId the producer's identifier for the columns and their meaning
 * @param key the key column and ordering mode
 * @param parts the parts in ascending key order; empty for an empty dataset
 * @param extensions each extension namespace mapped to its value's JSON text, in manifest order
 */
public record Manifest(FormatVersion version, String schemaId, KeyDefinition key, List<ManifestPart> parts, Map<String, String> extensions) {

    /**
     * Creates a manifest. Only nullness is checked here; {@link #parse} checks the rest.
     *
     * @param version the version pair
     * @param schemaId the schema id
     * @param key the key declaration
     * @param parts the parts
     * @param extensions the extensions
     */
    public Manifest {
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(schemaId, "schemaId");
        Objects.requireNonNull(key, "key");
        parts = List.copyOf(parts);
        extensions = Collections.unmodifiableMap(new LinkedHashMap<>(extensions));
    }

    /**
     * Parses and validates a manifest strictly, as a reader must on open: UTF-8 without a byte order
     * mark, one JSON object, no duplicate member names at any depth, no undefined members, exact
     * integers, canonical base64, PART-1 paths, and ascending, disjoint part bounds. The version is
     * determined first (MAN-7), so that a manifest of another version is refused as unsupported
     * rather than invalid.
     *
     * @param json the manifest's bytes
     * @return the manifest
     * @throws RefusalException with {@link RefusalCode#INVALID_MANIFEST} or {@link
     *     RefusalCode#UNSUPPORTED_VERSION}
     */
    public static Manifest parse(byte[] json) {
        return ManifestParser.parse(json);
    }

    /**
     * Returns the total number of rows as a signed 64-bit integer.
     *
     * <p>The format does not cap the sum at this range. Use {@link #rowsExact()} for larger
     * totals. Overflow here does not make the manifest invalid or prevent opening or querying it.
     *
     * @return the total number of rows in the parts
     * @throws ArithmeticException if the total exceeds {@link Long#MAX_VALUE}
     */
    public long rows() {
        long total = 0;
        for (ManifestPart part : parts) {
            total = Math.addExact(total, part.rows());
        }
        return total;
    }

    /**
     * Returns the exact total number of rows, including totals beyond the range of {@link #rows()}.
     *
     * @return the sum of the per-part row counts, or zero for an empty dataset
     */
    public BigInteger rowsExact() {
        BigInteger total = BigInteger.ZERO;
        for (ManifestPart part : parts) {
            total = total.add(BigInteger.valueOf(part.rows()));
        }
        return total;
    }
}
