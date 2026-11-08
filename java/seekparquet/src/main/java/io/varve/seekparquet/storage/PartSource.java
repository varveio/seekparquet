// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Objects;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * One opened part: an immutable object of known size, read by position.
 *
 * <p><b>Thread safety.</b> Implementations must allow concurrent {@link #read} calls from many
 * threads; the reader shares one {@code PartSource} between all requests on a dataset.
 *
 * <p><b>Identity.</b> Every read must return bytes of the same stored object version that {@link
 * #size()} describes. An implementation that can detect a change (an object-store ETag or
 * generation, a file's identity) must fail the read with {@link PartChangedException} rather than
 * return bytes of another version. {@link #version()} reports the pinned identity for diagnostics and
 * cache keys.
 *
 * <p><b>Cancellation.</b> A read should observe {@code cancellation}: check it before blocking, and
 * where the underlying call can be aborted, register an {@link Cancellation#onCancel} listener that
 * aborts it. A read that ends because of cancellation throws {@link
 * java.util.concurrent.CancellationException} or {@link java.io.InterruptedIOException}.
 *
 * <p><b>Ownership.</b> The reader closes each part it opened when the dataset is closed, or at once
 * when opening the dataset fails. {@link #close()} may be called while no read is in progress only.
 */
public interface PartSource extends InputFile, Closeable {

    /** {@return the known part size, using parquet-java's InputFile contract} */
    @Override
    default long getLength() {
        return size();
    }

    /** Opens an independent parquet-java stream without transferring ownership of this source.
     * @return an independently positioned stream
     */
    @Override
    default SeekableInputStream newStream() {
        return new PositionedStream(this);
    }

    /** Adapts an existing parquet-java input file. Each positioned read uses its own stream, so
     * concurrent requests never share a seek position. The file must describe an immutable object;
     * its owner retains responsibility for any resources held by the InputFile itself.
     * @param path the manifest path
     * @param file the parquet-java file, safe for concurrent newStream calls
     * @param version the optional pinned object identity, enforced by the input file
     * @return a source whose streams are closed after each read
     * @throws IOException if obtaining the file length fails
     */
    static PartSource of(String path, InputFile file, Optional<String> version) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(version, "version");
        long length = file.getLength();
        if (length < 0) {
            throw new IOException("negative input file length");
        }
        return new PartSource() {
            @Override public String path() { return path; }
            @Override public long size() { return length; }
            @Override public Optional<String> version() { return version; }
            @Override public void close() { }
            @Override public ByteBuffer read(long position, int count, Cancellation cancellation) throws IOException {
                if (position < 0 || count < 0 || position > length - count) {
                    throw new EOFException("read outside the part");
                }
                cancellation.throwIfCancelled();
                try (SeekableInputStream in = file.newStream();
                        Cancellation.Registration registration = cancellation.onCancel(() -> {
                            try { in.close(); } catch (IOException ignored) { }
                        })) {
                    in.seek(position);
                    ByteBuffer out = ByteBuffer.allocate(count);
                    in.readFully(out);
                    cancellation.throwIfCancelled();
                    return out.flip();
                } catch (IOException e) {
                    cancellation.throwIfCancelled();
                    throw e;
                }
            }
        };
    }

    /** {@return the part's manifest path, for diagnostics} */
    String path();

    /** {@return the size of the stored object in bytes} */
    long size();

    /**
     * {@return an opaque identity of the stored object version this source is pinned to, when the
     * storage has one} For example an ETag, a generation number, or a file identity and modification
     * time.
     */
    Optional<String> version();

    /**
     * Reads exactly {@code length} bytes from {@code position}.
     *
     * <p>The returned buffer holds the bytes between its position and limit. It may be read-only and
     * may be shared with other callers (a cache's block, say): the caller must not modify it.
     *
     * @param position the offset of the first byte, at least 0
     * @param length the number of bytes, at least 0, with {@code position + length <= size()}
     * @param cancellation the request's cancellation
     * @return the bytes
     * @throws EOFException if the object holds fewer bytes than requested
     * @throws PartChangedException if the stored object is no longer the version opened
     * @throws IOException if reading fails
     */
    ByteBuffer read(long position, int length, Cancellation cancellation) throws IOException;
}
