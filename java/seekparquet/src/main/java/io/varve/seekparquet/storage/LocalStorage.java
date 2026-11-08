// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Optional;

/**
 * A dataset root in a local directory.
 *
 * <p>Parts are read with positioned reads on one {@link FileChannel} per part, shared by all
 * requests. A part resolves within the root: a part path whose real path (after symbolic links)
 * lies outside the root's real path is not opened. A part is pinned to the file it opened: a read
 * fails with {@link PartChangedException} when the file's size changes, or when the channel had to
 * be reopened (after a thread interrupt closed it) and the path now names another file. Rewriting a
 * file in place without changing its size cannot be detected cheaply; parts must be immutable
 * (COMMIT-2).
 *
 * <p>This class holds no resources of its own and is safe for concurrent use.
 */
public final class LocalStorage implements DatasetStorage {

    private final Path root;

    private LocalStorage(Path root) {
        this.root = root;
    }

    /**
     * Returns the storage for the dataset root {@code root}. The directory need not exist yet: a
     * missing root reads as a root without a manifest.
     *
     * @param root the dataset root directory
     * @return the storage
     */
    public static LocalStorage of(Path root) {
        return new LocalStorage(Objects.requireNonNull(root, "root").toAbsolutePath().normalize());
    }

    /** {@return the dataset root directory} */
    public Path root() {
        return root;
    }

    @Override
    public byte[] readManifest(int maxBytes, Cancellation cancellation) throws IOException {
        cancellation.throwIfCancelled();
        Path manifest = root.resolve("manifest.json");
        if (!Files.isRegularFile(manifest)) {
            if (Files.exists(manifest)) {
                throw new IOException(manifest + " is not a regular file");
            }
            throw new NoSuchFileException(manifest.toString());
        }
        try (InputStream in = Files.newInputStream(manifest)) {
            return in.readNBytes(maxBytes < Integer.MAX_VALUE ? maxBytes + 1 : maxBytes);
        }
    }

    @Override
    public PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException {
        cancellation.throwIfCancelled();
        Path file = root;
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IOException("part path " + path + " is not a plain relative path");
            }
            file = file.resolve(segment);
        }
        if (!Files.exists(file)) {
            throw new NoSuchFileException(file.toString());
        }
        Path real = file.toRealPath();
        if (!real.startsWith(root.toRealPath())) {
            throw new IOException("part " + path + " resolves outside the dataset root");
        }
        if (!Files.isRegularFile(real)) {
            throw new IOException("part " + path + " is not a regular file");
        }
        return new LocalPart(path, real);
    }

    @Override
    public String toString() {
        return "LocalStorage[" + root + "]";
    }

    /** One part file. */
    private static final class LocalPart implements PartSource {
        private final String path;
        private final Path file;
        private final Object fileKey;
        private final long size;
        private final String version;
        private volatile FileChannel channel;
        private volatile boolean closed;

        LocalPart(String path, Path file) throws IOException {
            this.path = path;
            this.file = file;
            this.channel = FileChannel.open(file, StandardOpenOption.READ);
            try {
                BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                this.fileKey = attributes.fileKey();
                this.size = channel.size();
                this.version = (fileKey == null ? "" : fileKey + "@") + attributes.lastModifiedTime().toMillis() + ":" + size;
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        }

        @Override
        public String path() {
            return path;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public Optional<String> version() {
            return Optional.of(version);
        }

        @Override
        public ByteBuffer read(long position, int length, Cancellation cancellation) throws IOException {
            if (position < 0 || length < 0 || position > size - length) {
                throw new EOFException("read of " + length + " bytes at " + position + " is outside the " + size + "-byte part " + path);
            }
            cancellation.throwIfCancelled();
            ByteBuffer buffer = ByteBuffer.allocate(length);
            while (buffer.hasRemaining()) {
                FileChannel ch = channel;
                if (closed) {
                    throw new ClosedChannelException();
                }
                try {
                    int n = ch.read(buffer, position + buffer.position());
                    if (n < 0) {
                        throw new PartChangedException("part " + path + " is shorter than when it was opened");
                    }
                } catch (ClosedByInterruptException e) {
                    // This thread was interrupted; the channel is closed for every thread.
                    reopen(ch);
                    InterruptedIOException interrupted = new InterruptedIOException("read interrupted");
                    interrupted.initCause(e);
                    throw interrupted;
                } catch (ClosedChannelException e) {
                    if (closed) {
                        throw e;
                    }
                    // Another thread's interrupt closed the shared channel: reopen it and retry.
                    reopen(ch);
                }
            }
            if (channel.size() != size) {
                throw new PartChangedException("part " + path + " changed size after it was opened");
            }
            return buffer.flip();
        }

        /** Replaces a channel closed by a thread interrupt, if no other thread has yet. */
        private synchronized void reopen(FileChannel broken) throws IOException {
            if (closed || channel != broken) {
                return;
            }
            FileChannel fresh = FileChannel.open(file, StandardOpenOption.READ);
            try {
                Object key = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
                if (!Objects.equals(key, fileKey) || fresh.size() != size) {
                    throw new PartChangedException("part " + path + " was replaced after it was opened");
                }
            } catch (IOException | RuntimeException e) {
                fresh.close();
                throw e;
            }
            channel = fresh;
        }

        @Override
        public synchronized void close() throws IOException {
            closed = true;
            channel.close();
        }

        @Override
        public String toString() {
            return "LocalPart[" + file + "]";
        }
    }
}
