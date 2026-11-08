// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

/**
 * A dataset root (a directory, or an object-store prefix that behaves like one): where {@code
 * manifest.json} and the parts it lists live.
 *
 * <p>Implementations must be safe for concurrent use. The reader never closes a {@code
 * DatasetStorage}: whoever created it owns it. It does close every {@link PartSource} it opens.
 *
 * <p>Paths given to {@link #openPart} have already been checked against PART-1 (relative, {@code
 * /}-separated, no empty, {@code .} or {@code ..} segment, no backslash or control character), so
 * that joining them to the root cannot leave it. They are literal: an implementation must not
 * percent-decode or normalise them.
 */
public interface DatasetStorage {

    /**
     * Reads the whole of {@code manifest.json}.
     *
     * @param maxBytes the most bytes the caller accepts; an implementation may stop reading after
     *     {@code maxBytes + 1} bytes and return those, which the caller then rejects as too large
     * @param cancellation the caller's cancellation, for an implementation that can abort a read
     * @return the manifest's bytes
     * @throws NoSuchFileException if the root holds no manifest (the dataset does not exist)
     * @throws IOException if reading fails
     */
    byte[] readManifest(int maxBytes, Cancellation cancellation) throws IOException;

    /**
     * Opens a part for positioned reads.
     *
     * <p>{@code expectedSize} is the size the manifest records. An implementation may use it as a
     * hint (to choose how much of the file's tail to fetch with its first request, say), but {@link
     * PartSource#size()} must report the stored object's actual size, which the reader compares with
     * the manifest.
     *
     * @param path the part's manifest path, relative to the root
     * @param expectedSize the part's size according to the manifest
     * @param cancellation the caller's cancellation
     * @return the opened part, which the caller closes
     * @throws NoSuchFileException if the part does not exist
     * @throws IOException if opening fails
     */
    PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException;
}
