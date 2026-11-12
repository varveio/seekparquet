// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Receives the borrowed {@link Batch}es of a scan ({@link Dataset#scan}, {@link Plan#scan}), in serial
 * calls on the scanning thread. The batch is valid only until {@link #visit} returns; see {@link Batch}.
 *
 * <p>Returning {@code true} continues the scan; returning {@code false} stops it successfully, reading
 * nothing more (the Go reader's {@code Stop}). An exception {@code visit} throws ends the scan and
 * propagates unchanged, checked ones included: {@code E} lets a visitor that writes to an I/O sink throw
 * {@link java.io.IOException} without wrapping it.
 *
 * @param <E> the checked exception the visitor may throw, {@link RuntimeException} for none
 */
@FunctionalInterface
public interface BatchVisitor<E extends Exception> {

    /**
     * Visits one batch.
     *
     * @param batch the batch, borrowed until this returns
     * @return {@code true} to continue the scan, {@code false} to stop it successfully
     * @throws E to end the scan with this exception
     */
    boolean visit(Batch batch) throws E;
}
