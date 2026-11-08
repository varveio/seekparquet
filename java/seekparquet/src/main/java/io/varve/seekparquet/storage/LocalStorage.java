// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.DirectoryStream;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Optional;

/**
 * A dataset root in a local directory.
 *
 * <p>Parts are read with positioned reads on one {@link FileChannel} per part, shared by all
 * requests. Parts are opened relative to pinned directory handles, rejecting symbolic links in
 * every part-path component. Filesystems without secure directory streams are rejected. A part is pinned to the file it opened: a read
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
        if (path.startsWith("/") || path.indexOf('\\') >= 0
                || path.chars().anyMatch(c -> c < 32 || c == 127)) {
            throw new IOException("part path is not a plain relative path");
        }
        String[] segments = path.split("/", -1);
        for (String segment : segments) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IOException("part path " + path + " is not a plain relative path");
            }
        }
        DirectoryStream<Path> directory = Files.newDirectoryStream(root);
        if (!(directory instanceof SecureDirectoryStream<Path>)) {
            directory.close();
            throw new IOException("the filesystem does not support secure part-path traversal");
        }
        @SuppressWarnings("unchecked")
        SecureDirectoryStream<Path> parent = (SecureDirectoryStream<Path>) directory;
        try {
            for (int i = 0; i < segments.length - 1; i++) {
                SecureDirectoryStream<Path> child = parent.newDirectoryStream(Path.of(segments[i]), LinkOption.NOFOLLOW_LINKS);
                try {
                    parent.close();
                } catch (IOException e) {
                    child.close();
                    throw e;
                }
                parent = child;
            }
            return new LocalPart(path, parent, Path.of(segments[segments.length - 1]));
        } catch (IOException | RuntimeException | Error e) {
            try { parent.close(); } catch (IOException suppressed) { e.addSuppressed(suppressed); }
            throw e;
        }
    }

    @Override
    public String toString() {
        return "LocalStorage[" + root + "]";
    }

    /** One part file. */
    private static final class LocalPart implements PartSource {
        private final String path;
        private final SecureDirectoryStream<Path> parent;
        private final Path file;
        private final Object fileKey;
        private final long size;
        private final String version;
        private volatile SeekableByteChannel channel;
        private volatile boolean closed;

        LocalPart(String path, SecureDirectoryStream<Path> parent, Path file) throws IOException {
            this.path = path;
            this.parent = parent;
            this.file = file;
            BasicFileAttributes attributes = attributes();
            if (!attributes.isRegularFile()) {
                throw new IOException("part " + path + " is not a regular file");
            }
            this.fileKey = attributes.fileKey();
            this.channel = openChannel();
            try {
                if (!Objects.equals(fileKey, attributes().fileKey())) {
                    throw new PartChangedException("part " + path + " changed while opening");
                }
                this.size = channel.size();
                this.version = (fileKey == null ? "" : fileKey + "@") + attributes.lastModifiedTime().toMillis() + ":" + size;
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        }

        private BasicFileAttributes attributes() throws IOException {
            return parent.getFileAttributeView(file, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
        }

        private SeekableByteChannel openChannel() throws IOException {
            return parent.newByteChannel(file, Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
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
            while (true) {
                cancellation.throwIfCancelled();
                SeekableByteChannel ch = channel;
                if (closed) {
                    throw new ClosedChannelException();
                }
                try {
                    if (buffer.hasRemaining()) {
                        int n;
                        if (ch instanceof FileChannel fc) {
                            n = fc.read(buffer, position + buffer.position());
                        } else {
                            synchronized (ch) {
                                ch.position(position + buffer.position());
                                n = ch.read(buffer);
                            }
                        }
                        if (n < 0) {
                            throw new PartChangedException("part " + path + " is shorter than when it was opened");
                        }
                        continue;
                    }
                    if (ch.size() != size) {
                        throw new PartChangedException("part " + path + " changed size after it was opened");
                    }
                    cancellation.throwIfCancelled();
                    return buffer.flip();
                } catch (ClosedByInterruptException e) {
                    // Leave repair to a surviving request; this thread must retain its flag.
                    throw interrupted(e);
                } catch (ClosedChannelException e) {
                    cancellation.throwIfCancelled();
                    if (closed) {
                        throw e;
                    }
                    reopen(ch);
                }
            }
        }

        private static CancellationException interrupted(IOException cause) {
            Thread.currentThread().interrupt();
            CancellationException failure = new CancellationException("read interrupted");
            failure.initCause(cause);
            return failure;
        }

        /** Reopens through the pinned parent handle, with no pathname re-resolution. */
        private synchronized void reopen(SeekableByteChannel broken) throws IOException {
            if (closed || channel != broken) {
                return;
            }
            SeekableByteChannel fresh = openChannel();
            try {
                Object key = attributes().fileKey();
                if (!Objects.equals(key, fileKey) || fresh.size() != size) {
                    throw new PartChangedException("part " + path + " was replaced after it was opened");
                }
            } catch (ClosedByInterruptException e) {
                fresh.close();
                throw interrupted(e);
            } catch (IOException | RuntimeException e) {
                fresh.close();
                throw e;
            }
            channel = fresh;
        }

        @Override
        public synchronized void close() throws IOException {
            closed = true;
            try { channel.close(); } finally { parent.close(); }
        }

        @Override
        public String toString() {
            return "LocalPart[" + file + "]";
        }
    }
}
