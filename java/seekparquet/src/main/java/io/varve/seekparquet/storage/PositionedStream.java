// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.parquet.io.SeekableInputStream;

/** An independently positioned, non-owning view of a part. Confined to its caller's thread. */
final class PositionedStream extends SeekableInputStream {
    private final PartSource source;
    private long position;
    private boolean closed;

    PositionedStream(PartSource source) {
        this.source = source;
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("stream is closed");
        }
    }

    @Override
    public long getPos() throws IOException {
        checkOpen();
        return position;
    }

    @Override
    public void seek(long next) throws IOException {
        checkOpen();
        if (next < 0 || next > source.size()) {
            throw new EOFException("seek outside the part");
        }
        position = next;
    }

    @Override
    public int read() throws IOException {
        byte[] single = new byte[1];
        return read(single, 0, 1) < 0 ? -1 : single[0] & 255;
    }

    @Override
    public int read(byte[] out, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, out.length);
        return read(ByteBuffer.wrap(out, offset, length));
    }

    @Override
    public int read(ByteBuffer out) throws IOException {
        checkOpen();
        if (!out.hasRemaining()) {
            return 0;
        }
        if (position == source.size()) {
            return -1;
        }
        // Stream consumers can request very large buffers. Keep temporary reads bounded.
        int count = (int) Math.min(Math.min(out.remaining(), 64 * 1024), source.size() - position);
        ByteBuffer bytes = source.read(position, count, Cancellation.none());
        if (bytes.remaining() != count) {
            throw new EOFException("short positioned read");
        }
        out.put(bytes.duplicate());
        position += count;
        return count;
    }

    @Override
    public void readFully(byte[] out) throws IOException {
        readFully(out, 0, out.length);
    }

    @Override
    public void readFully(byte[] out, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, out.length);
        readFully(ByteBuffer.wrap(out, offset, length));
    }

    @Override
    public void readFully(ByteBuffer out) throws IOException {
        checkOpen();
        if (out.remaining() > source.size() - position) {
            throw new EOFException("read outside the part");
        }
        while (out.hasRemaining()) {
            if (read(out) < 0) {
                throw new EOFException("short read");
            }
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
