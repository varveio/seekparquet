// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import io.varve.seekparquet.Cancellation;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.DirectoryStream;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
 * requests. Each part path is resolved one segment at a time from the root, refusing a symbolic link
 * in any segment, and the part and {@code manifest.json} must be regular files. A part is pinned to the
 * file it opened: a read fails with {@link PartChangedException} when the file's size changes, or when
 * the channel had to be reopened (after a thread interrupt closed it) and the path now names another
 * file. Rewriting a file in place without changing its size cannot be detected cheaply; parts must be
 * immutable (COMMIT-2).
 *
 * <p>How strong these guarantees are depends on the filesystem:
 *
 * <ul>
 *   <li><b>Secure directory streams</b> (the default filesystem on Linux, macOS and other POSIX
 *       systems): each segment is opened relative to a pinned handle on its directory, so no
 *       replacement of a directory, by a symbolic link or otherwise, can redirect a lookup, and a part
 *       reopened after an interrupt is looked up through the same handles. The file's identity (its
 *       {@code fileKey}, device and inode) is compared before and after each open.
 *   <li><b>Plain directory streams</b> (the default filesystem on Windows, and other providers without
 *       secure streams): each segment is resolved as a path. A symbolic link, junction or other reparse
 *       point, which the JDK reports as a symbolic link or as {@code isOther()}, is refused in every
 *       segment, as are segments Windows would reinterpret (a {@code :}, a trailing dot or space, a
 *       reserved device name such as {@code NUL}). Since there is no handle to pin, every directory on
 *       the way is checked again before and after each open, and the file's identity is compared
 *       before and after: its {@code fileKey} where the filesystem has one (Windows has none), else its
 *       size and modification and creation times, which a replacement made to match all three can
 *       evade. A replacement between those checks can still go unseen; keep the dataset directory
 *       writable only by its producer. Files the JDK does not report as regular, such as cloud-storage
 *       placeholders, are refused as not regular files.
 * </ul>
 *
 * <p>Java cannot open a file without blocking on a special file (it has no {@code O_NONBLOCK}), and
 * whoever can write the dataset directory can replace an entry checked as a regular file with a FIFO
 * before it is opened: the open would then wait for a writer, beyond the reach of cancellation,
 * interruption or {@code Dataset.close()}. So with secure directory streams every open of a part or of
 * the manifest runs on a new platform thread, through its own handle on the file's directory, while the
 * request waits for it and stops waiting when cancelled. Each open therefore costs a short-lived thread
 * and a directory handle, besides the part's own; parts are opened once each, when first touched (and
 * again only after an interrupt closed the part's channel). An open that never returns strands its
 * thread and directory handle until the file is opened for writing or the process exits; once 64
 * opens through one storage are stranded, its further opens fail with an {@link
 * IOException} instead of starting another thread. Plain directory streams open on the caller's thread:
 * on Windows a named pipe exists only in the {@code \\.\pipe\} namespace and cannot be an entry of
 * the dataset directory, so an open there does not wait for a writer.
 *
 * <p>This class holds no resources of its own beyond such stranded opens, and is safe for concurrent
 * use.
 */
public final class LocalStorage implements DatasetStorage {

    /** The most opens through one storage that may be left blocked before its further opens fail. */
    static final int MAX_STRANDED_OPENS = 64;

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
        Dir directory;
        try {
            directory = rootDirectory();
        } catch (NoSuchFileException | NotDirectoryException e) {
            throw new NoSuchFileException(manifest.toString());
        }
        try (directory) {
            String name = "manifest.json";
            BasicFileAttributes attributes;
            try {
                attributes = directory.attributes(name);
            } catch (NoSuchFileException e) {
                throw new NoSuchFileException(manifest.toString());
            }
            if (!attributes.isRegularFile()) {
                throw new IOException(manifest + " is not a regular file");
            }
            try (SeekableByteChannel channel = directory.open(name, cancellation)) {
                if (!sameFile(attributes, directory.attributes(name), channel)) {
                    throw new IOException(manifest + " changed while opening");
                }
                return Channels.newInputStream(channel).readNBytes(maxBytes < Integer.MAX_VALUE ? maxBytes + 1 : maxBytes);
            }
        }
    }

    /** {@return the root directory: a pinned handle when the filesystem has secure directory streams, else the checked path} */
    private Dir rootDirectory() throws IOException {
        DirectoryStream<Path> directory = Files.newDirectoryStream(root);
        if (directory instanceof SecureDirectoryStream<Path> secure) {
            return new PinnedDir(secure, opener);
        }
        directory.close();
        return new CheckedDir(root, List.of(), List.of());
    }

    private static BasicFileAttributes attributes(SecureDirectoryStream<Path> directory, Path name) throws IOException {
        return directory.getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
    }

    /**
     * {@return what identifies a file across a replacement: its {@code fileKey} where the filesystem has
     * one (device and inode on POSIX), else its size and modification and creation times}
     */
    static Object identity(BasicFileAttributes a) {
        return a.fileKey() != null ? a.fileKey() : Arrays.asList(a.size(), a.lastModifiedTime(), a.creationTime());
    }

    /** {@return whether {@code after} and the opened {@code channel} are the file {@code before} described} */
    private static boolean sameFile(BasicFileAttributes before, BasicFileAttributes after, SeekableByteChannel channel) throws IOException {
        return Objects.equals(identity(before), identity(after)) && (before.fileKey() != null || channel.size() == after.size());
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
        Dir parent = rootDirectory();
        try {
            for (int i = 0; i < segments.length - 1; i++) {
                Dir child = parent.child(segments[i]);
                try {
                    parent.close();
                } catch (IOException e) {
                    child.close();
                    throw e;
                }
                parent = child;
            }
            return new LocalPart(path, parent, segments[segments.length - 1], cancellation);
        } catch (IOException | RuntimeException | Error e) {
            try { parent.close(); } catch (IOException suppressed) { e.addSuppressed(suppressed); }
            throw e;
        }
    }

    /** A directory that the manifest or a part is looked up and opened in. */
    private interface Dir extends Closeable {
        /** {@return the attributes of the entry {@code name}, not following a symbolic link} */
        BasicFileAttributes attributes(String name) throws IOException;

        /** {@return the subdirectory {@code name}, refusing a symbolic link} */
        Dir child(String name) throws IOException;

        /** {@return the entry {@code name} opened for reading, refusing a symbolic link} */
        SeekableByteChannel open(String name, Cancellation cancellation) throws IOException;
    }

    /** A directory pinned by a secure directory stream: lookups are relative to its handle. */
    private record PinnedDir(SecureDirectoryStream<Path> stream, Opener opener) implements Dir {
        @Override
        public BasicFileAttributes attributes(String name) throws IOException {
            return LocalStorage.attributes(stream, Path.of(name));
        }

        @Override
        public Dir child(String name) throws IOException {
            return new PinnedDir(stream.newDirectoryStream(Path.of(name), LinkOption.NOFOLLOW_LINKS), opener);
        }

        @Override
        public SeekableByteChannel open(String name, Cancellation cancellation) throws IOException {
            return opener.open(stream, Path.of(name), cancellation);
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    /**
     * A directory known by its path, on a filesystem without secure directory streams: the directories
     * from the root to it, each with the identity it had when checked, are checked again around every
     * open (see the class comment). Opens run on the caller's thread: on Windows, where the default
     * filesystem has no secure streams, a named pipe cannot be an entry of a directory, so an open
     * cannot block waiting for a writer as a POSIX FIFO's does.
     */
    private record CheckedDir(Path dir, List<Path> chain, List<Object> identities) implements Dir {

        /** Device names Windows resolves in any directory, with or without an extension. */
        private static final Set<String> DEVICES = Set.of("CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$",
                "COM0", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "COM\u00b9", "COM\u00b2", "COM\u00b3",
                "LPT0", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9", "LPT\u00b9", "LPT\u00b2", "LPT\u00b3");

        /**
         * {@return the path of the entry {@code name}}, after checking that it names exactly that entry:
         * not a drive, stream ({@code name:stream}) or device, and nothing Windows would trim.
         */
        private Path entry(String name) throws IOException {
            String stem = name.contains(".") ? name.substring(0, name.indexOf('.')) : name;
            if (name.chars().anyMatch(c -> ":<>\"|?*".indexOf(c) >= 0) || name.endsWith(".") || name.endsWith(" ")
                    || DEVICES.contains(stem.strip().toUpperCase(Locale.ROOT))) {
                throw new IOException("the path segment " + name + " cannot be resolved safely on this filesystem");
            }
            Path p;
            try {
                p = dir.resolve(name);
            } catch (InvalidPathException e) {
                throw new IOException("the path segment " + name + " cannot be resolved safely on this filesystem", e);
            }
            if (!dir.equals(p.getParent()) || p.getFileName() == null || !name.equals(p.getFileName().toString())) {
                throw new IOException("the path segment " + name + " cannot be resolved safely on this filesystem");
            }
            return p;
        }

        private static BasicFileAttributes read(Path p) throws IOException {
            return Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        }

        /** A link, junction or other reparse point: the JDK reports a Windows reparse point that is not a symbolic link as {@code isOther()}. */
        private static boolean linkLike(BasicFileAttributes a) {
            return a.isSymbolicLink() || a.isOther();
        }

        /** {@return a directory's identity: its fileKey, else its creation time, which entries added to it leave unchanged} */
        private static Object directoryIdentity(BasicFileAttributes a) {
            return a.fileKey() != null ? a.fileKey() : a.creationTime();
        }

        @Override
        public BasicFileAttributes attributes(String name) throws IOException {
            return read(entry(name));
        }

        @Override
        public Dir child(String name) throws IOException {
            Path p = entry(name);
            BasicFileAttributes a = read(p);
            if (linkLike(a)) {
                throw new FileSystemException(p.toString(), null, "a symbolic link, junction or other reparse point");
            }
            if (!a.isDirectory()) {
                throw new NotDirectoryException(p.toString());
            }
            List<Path> paths = new ArrayList<>(chain);
            paths.add(p);
            List<Object> ids = new ArrayList<>(identities);
            ids.add(directoryIdentity(a));
            return new CheckedDir(p, List.copyOf(paths), List.copyOf(ids));
        }

        /** Checks that every directory from the root is still the one it was, and no link. */
        private void recheck() throws IOException {
            for (int i = 0; i < chain.size(); i++) {
                BasicFileAttributes a;
                try {
                    a = read(chain.get(i));
                } catch (NoSuchFileException e) {
                    throw new PartChangedException("directory " + chain.get(i) + " was removed");
                }
                if (linkLike(a) || !a.isDirectory() || !Objects.equals(directoryIdentity(a), identities.get(i))) {
                    throw new PartChangedException("directory " + chain.get(i) + " was replaced");
                }
            }
        }

        @Override
        public SeekableByteChannel open(String name, Cancellation cancellation) throws IOException {
            cancellation.throwIfCancelled();
            Path p = entry(name);
            recheck();
            SeekableByteChannel channel = Files.newByteChannel(p, Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
            try {
                recheck();
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
            return channel;
        }

        @Override
        public void close() {
            // No handle to release.
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
        private final Dir parent;
        private final String file;
        private final Object identity;
        private final long size;
        private final String version;
        private volatile SeekableByteChannel channel;
        private volatile boolean closed;

        LocalPart(String path, Dir parent, String file, Cancellation cancellation) throws IOException {
            this.path = path;
            this.parent = parent;
            this.file = file;
            BasicFileAttributes attributes = attributes();
            if (!attributes.isRegularFile()) {
                throw new IOException("part " + path + " is not a regular file");
            }
            this.identity = identity(attributes);
            this.channel = openChannel(cancellation);
            try {
                if (!sameFile(attributes, attributes(), channel)) {
                    throw new PartChangedException("part " + path + " changed while opening");
                }
                this.size = channel.size();
                Object fileKey = attributes.fileKey();
                this.version = (fileKey == null ? "" : fileKey + "@") + attributes.lastModifiedTime().toMillis() + ":" + size;
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        }

        private BasicFileAttributes attributes() throws IOException {
            return parent.attributes(file);
        }

        private SeekableByteChannel openChannel(Cancellation cancellation) throws IOException {
            return parent.open(file, cancellation);
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

        /**
         * Reopens through the parent directory: its pinned handle, with no pathname re-resolution, or
         * its checked path, with the directories checked again.
         */
        private synchronized void reopen(SeekableByteChannel broken, Cancellation cancellation) throws IOException {
            if (closed || channel != broken) {
                return;
            }
            SeekableByteChannel fresh = openChannel(cancellation);
            try {
                if (!Objects.equals(identity(attributes()), identity) || fresh.size() != size) {
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
