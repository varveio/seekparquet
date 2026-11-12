// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Conversions between {@link PartSource} and parquet-java's {@link InputFile}, for storage that already
 * has an {@code InputFile} and for tools that hand a part to parquet-java. They are the only place
 * parquet-java types appear in this library's API; the reader itself needs none of them.
 */
public final class ParquetJavaInterop {

    private ParquetJavaInterop() {}

    /**
     * Adapts a parquet-java input file as a part source. Each positioned read opens a stream of its own,
     * so concurrent requests never share a seek position, and returns a buffer of its own. The file must
     * describe an immutable object; its owner keeps responsibility for any resources the {@code InputFile}
     * holds, and closing the returned source closes nothing.
     *
     * @param path the manifest path
     * @param file the parquet-java file, safe for concurrent {@code newStream} calls
     * @param version the optional pinned object identity, enforced by the input file
     * @return the part source
     * @throws IOException if obtaining the file length fails
     */
    public static PartSource partSource(String path, InputFile file, Optional<String> version) throws IOException {
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

    /**
     * Views a part source as a parquet-java input file, without transferring ownership: closing a stream
     * it opens does not close the source, and the source must stay open while the file is used. Each
     * stream is independently positioned and reads through {@link PartSource#read} without cancellation.
     *
     * @param source the part source
     * @return the input file
     */
    public static InputFile inputFile(PartSource source) {
        Objects.requireNonNull(source, "source");
        return new InputFile() {
            @Override public long getLength() { return source.size(); }
            @Override public SeekableInputStream newStream() { return new PositionedStream(source); }
            @Override public String toString() { return source.path(); }
        };
    }
}
