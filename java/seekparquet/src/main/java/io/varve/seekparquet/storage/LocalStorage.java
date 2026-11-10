// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.DirectoryStream;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>{@code manifest.json} is read through a pinned handle on the root too: it must be a regular file,
 * and a symbolic link there is refused as not one.
 *
 * <p>Java cannot open a file without blocking on a special file (it has no {@code O_NONBLOCK}), and
 * whoever can write the dataset directory can replace an entry checked as a regular file with a FIFO
 * before it is opened: the open would then wait for a writer, beyond the reach of cancellation,
 * interruption or {@code Dataset.close()}. So every open of a part or of the manifest runs on a new
 * platform thread, through its own handle on the file's directory, while the request waits for it and
 * stops waiting when cancelled. Each open therefore costs a short-lived thread and a directory handle,
 * besides the part's own; parts are opened once each, when first touched (and again only after an
 * interrupt closed the part's channel). An open that never returns strands its thread and directory
 * handle until the file is opened for writing or the process exits; once {@value #MAX_STRANDED_OPENS}
 * opens through one storage are stranded, its further opens fail with an {@link IOException} instead
 * of starting another thread.
 *
 * <p>This class holds no resources of its own beyond such stranded opens, and is safe for concurrent
 * use.
 */
public final class LocalStorage implements DatasetStorage {

    /** The most opens through one storage that may be left blocked before its further opens fail. */
    public static final int MAX_STRANDED_OPENS = 64;

    private final Path root;
    private final Opener opener = new Opener();

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
        SecureDirectoryStream<Path> directory;
        try {
            directory = secure(Files.newDirectoryStream(root));
        } catch (NoSuchFileException | NotDirectoryException e) {
            throw new NoSuchFileException(manifest.toString());
        }
        try (directory) {
            Path name = Path.of("manifest.json");
            BasicFileAttributes attributes;
            try {
                attributes = attributes(directory, name);
            } catch (NoSuchFileException e) {
                throw new NoSuchFileException(manifest.toString());
            }
            if (!attributes.isRegularFile()) {
                throw new IOException(manifest + " is not a regular file");
            }
            try (SeekableByteChannel channel = opener.open(directory, name, cancellation)) {
                if (!Objects.equals(attributes.fileKey(), attributes(directory, name).fileKey())) {
                    throw new IOException(manifest + " changed while opening");
                }
                return Channels.newInputStream(channel).readNBytes(maxBytes < Integer.MAX_VALUE ? maxBytes + 1 : maxBytes);
            }
        }
    }

    /** {@return {@code directory}, if it is secure; otherwise closes it and fails} */
    private static SecureDirectoryStream<Path> secure(DirectoryStream<Path> directory) throws IOException {
        if (!(directory instanceof SecureDirectoryStream<Path> secure)) {
            directory.close();
            throw new IOException("the filesystem does not support secure part-path traversal");
        }
        return secure;
    }

    private static BasicFileAttributes attributes(SecureDirectoryStream<Path> directory, Path name) throws IOException {
        return directory.getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
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
        SecureDirectoryStream<Path> parent = secure(Files.newDirectoryStream(root));
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
            return new LocalPart(path, parent, Path.of(segments[segments.length - 1]), opener, cancellation);
        } catch (IOException | RuntimeException | Error e) {
            try { parent.close(); } catch (IOException suppressed) { e.addSuppressed(suppressed); }
            throw e;
        }
    }

    @Override
    public String toString() {
        return "LocalStorage[" + root + "]";
    }

    /**
     * Opens files on threads of their own, so that a request can stop waiting for an open that blocks
     * (see the class comment), and counts the opens it has left blocked.
     */
    private static final class Opener {
        private final AtomicInteger stranded = new AtomicInteger();

        /**
         * Opens {@code name} in {@code directory} for reading, refusing a symbolic link. Returns once the
         * open does; when {@code cancellation} is cancelled or the thread interrupted first, it stops
         * waiting and throws {@link CancellationException}, leaving the open to finish on its own thread,
         * which then closes whatever it opened.
         */
        SeekableByteChannel open(SecureDirectoryStream<Path> directory, Path name, Cancellation cancellation) throws IOException {
            cancellation.throwIfCancelled();
            if (stranded.get() >= MAX_STRANDED_OPENS) {
                throw new IOException(MAX_STRANDED_OPENS + " opens through this storage are blocked: not opening " + name);
            }
            // Its own handle on the directory: an open left blocked holds this one, so that the caller's
            // handle can still be closed (closing a handle waits for the operations in progress on it).
            SecureDirectoryStream<Path> own = directory.newDirectoryStream(Path.of("."), LinkOption.NOFOLLOW_LINKS);
            CompletableFuture<SeekableByteChannel> result = new CompletableFuture<>();
            try {
                Thread.ofPlatform().daemon().name("seekparquet-open").start(() -> {
                    try (own) {
                        SeekableByteChannel channel = own.newByteChannel(name, Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
                        if (!result.complete(channel)) {
                            stranded.decrementAndGet();
                            channel.close();
                        }
                    } catch (Throwable e) {
                        if (!result.completeExceptionally(e)) {
                            stranded.decrementAndGet();
                        }
                    }
                });
            } catch (RuntimeException | Error e) {
                own.close();
                throw e;
            }
            try (Cancellation.Registration r = cancellation.onCancel(() -> abandon(result))) {
                return result.get();
            } catch (CancellationException e) {
                cancellation.throwIfCancelled();
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!abandon(result) && !result.isCompletedExceptionally()) {
                    // Opened just before the interrupt: nobody will use the channel.
                    result.getNow(null).close();
                }
                throw new CancellationException("interrupted");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) {
                    throw io;
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                if (cause instanceof Error err) {
                    throw err;
                }
                throw new IOException(cause);
            }
        }

        /** Stops waiting for {@code result}. {@return whether the open was still in progress, and is now stranded} */
        private boolean abandon(CompletableFuture<SeekableByteChannel> result) {
            if (result.completeExceptionally(new CancellationException())) {
                stranded.incrementAndGet();
                return true;
            }
            return false;
        }
    }

    /** One part file. */
    private static final class LocalPart implements PartSource {
        private final String path;
        private final SecureDirectoryStream<Path> parent;
        private final Path file;
        private final Opener opener;
        private final Object fileKey;
        private final long size;
        private final String version;
        private volatile SeekableByteChannel channel;
        private volatile boolean closed;

        LocalPart(String path, SecureDirectoryStream<Path> parent, Path file, Opener opener, Cancellation cancellation)
                throws IOException {
            this.path = path;
            this.parent = parent;
            this.file = file;
            this.opener = opener;
            BasicFileAttributes attributes = attributes();
            if (!attributes.isRegularFile()) {
                throw new IOException("part " + path + " is not a regular file");
            }
            this.fileKey = attributes.fileKey();
            this.channel = openChannel(cancellation);
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
            return LocalStorage.attributes(parent, file);
        }

        private SeekableByteChannel openChannel(Cancellation cancellation) throws IOException {
            return opener.open(parent, file, cancellation);
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
                    reopen(ch, cancellation);
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
        private synchronized void reopen(SeekableByteChannel broken, Cancellation cancellation) throws IOException {
            if (closed || channel != broken) {
                return;
            }
            SeekableByteChannel fresh = openChannel(cancellation);
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
