// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A file system for tests that delegates every operation to the default one, except that its directory
 * streams are plain {@link DirectoryStream}s, not secure ones, as on Windows: LocalStorage then takes
 * its checked-path route. Optionally its attributes carry no {@code fileKey}, as Windows', so that file
 * identity falls back to size and times; and a hook runs after each attribute read, between
 * LocalStorage's check of an entry and its open.
 */
final class PlainDirectoryFileSystem extends FileSystem {

    /** Runs after attributes are read, with the real path and its attributes. */
    interface AfterRead {
        void run(Path real, BasicFileAttributes attributes) throws IOException;
    }

    private final FileSystem real;
    private final boolean fileKeys;
    private volatile AfterRead afterRead = (p, a) -> {};
    private final Provider provider = new Provider();

    /**
     * @param fileKeys whether attributes keep their {@code fileKey}; without, identity falls back to
     *     size and times, as on Windows
     */
    PlainDirectoryFileSystem(boolean fileKeys) {
        this.real = Path.of("").getFileSystem();
        this.fileKeys = fileKeys;
    }

    void afterRead(AfterRead hook) {
        this.afterRead = hook;
    }

    /** {@return {@code path} of the default file system, as a path of this one} */
    Path wrap(Path path) {
        return (Path) Proxy.newProxyInstance(Path.class.getClassLoader(), new Class<?>[] {Path.class}, new Wrapped(path));
    }

    private Path unwrap(Path path) {
        if (Proxy.isProxyClass(path.getClass()) && Proxy.getInvocationHandler(path) instanceof Wrapped w && w.owner() == this) {
            return w.real;
        }
        return path;
    }

    private Object[] unwrapAll(Object[] args) {
        if (args == null) {
            return null;
        }
        Object[] out = args.clone();
        for (int i = 0; i < out.length; i++) {
            if (out[i] instanceof Path p) {
                out[i] = unwrap(p);
            }
        }
        return out;
    }

    private final class Wrapped implements InvocationHandler {
        final Path real;

        Wrapped(Path real) {
            this.real = real;
        }

        PlainDirectoryFileSystem owner() {
            return PlainDirectoryFileSystem.this;
        }

        @Override
        public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
            switch (m.getName()) {
                case "getFileSystem":
                    return PlainDirectoryFileSystem.this;
                case "equals":
                    return args[0] instanceof Path p && unwrap(p) != p && real.equals(unwrap(p));
                case "hashCode":
                    return real.hashCode();
                case "toString":
                    return real.toString();
                case "toFile":
                case "register":
                    throw new UnsupportedOperationException(m.getName());
                case "iterator": {
                    List<Path> names = new ArrayList<>();
                    real.iterator().forEachRemaining(n -> names.add(wrap(n)));
                    return names.iterator();
                }
                default:
                    break;
            }
            Object result;
            try {
                result = m.invoke(real, unwrapAll(args));
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            return result instanceof Path p ? wrap(p) : result;
        }
    }

    /** Attributes without a fileKey. */
    private record NoKey(BasicFileAttributes a) implements BasicFileAttributes {
        @Override public FileTime lastModifiedTime() { return a.lastModifiedTime(); }
        @Override public FileTime lastAccessTime() { return a.lastAccessTime(); }
        @Override public FileTime creationTime() { return a.creationTime(); }
        @Override public boolean isRegularFile() { return a.isRegularFile(); }
        @Override public boolean isDirectory() { return a.isDirectory(); }
        @Override public boolean isSymbolicLink() { return a.isSymbolicLink(); }
        @Override public boolean isOther() { return a.isOther(); }
        @Override public long size() { return a.size(); }
        @Override public Object fileKey() { return null; }
    }

    private final class Provider extends FileSystemProvider {
        private FileSystemProvider d() {
            return real.provider();
        }

        @Override public String getScheme() { return "plain-directory"; }
        @Override public FileSystem newFileSystem(URI uri, Map<String, ?> env) { throw new UnsupportedOperationException(); }
        @Override public FileSystem getFileSystem(URI uri) { throw new UnsupportedOperationException(); }
        @Override public Path getPath(URI uri) { return wrap(d().getPath(uri)); }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return d().newByteChannel(unwrap(path), options, attrs);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return d().newFileChannel(unwrap(path), options, attrs);
        }

        @Override
        public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
            DirectoryStream<Path> stream = d().newDirectoryStream(unwrap(dir), p -> filter.accept(wrap(p)));
            // Deliberately not a SecureDirectoryStream.
            return new DirectoryStream<>() {
                @Override
                public Iterator<Path> iterator() {
                    Iterator<Path> it = stream.iterator();
                    return new Iterator<>() {
                        @Override public boolean hasNext() { return it.hasNext(); }
                        @Override public Path next() { return wrap(it.next()); }
                    };
                }

                @Override
                public void close() throws IOException {
                    stream.close();
                }
            };
        }

        @Override public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException { d().createDirectory(unwrap(dir), attrs); }
        @Override public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
            d().createSymbolicLink(unwrap(link), unwrap(target), attrs);
        }
        @Override public Path readSymbolicLink(Path link) throws IOException { return wrap(d().readSymbolicLink(unwrap(link))); }
        @Override public void delete(Path path) throws IOException { d().delete(unwrap(path)); }
        @Override public void copy(Path source, Path target, CopyOption... options) throws IOException { d().copy(unwrap(source), unwrap(target), options); }
        @Override public void move(Path source, Path target, CopyOption... options) throws IOException { d().move(unwrap(source), unwrap(target), options); }
        @Override public boolean isSameFile(Path a, Path b) throws IOException { return d().isSameFile(unwrap(a), unwrap(b)); }
        @Override public boolean isHidden(Path path) throws IOException { return d().isHidden(unwrap(path)); }
        @Override public FileStore getFileStore(Path path) throws IOException { return d().getFileStore(unwrap(path)); }
        @Override public void checkAccess(Path path, AccessMode... modes) throws IOException { d().checkAccess(unwrap(path), modes); }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
            return d().getFileAttributeView(unwrap(path), type, options);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
            Path p = unwrap(path);
            A a = d().readAttributes(p, type, options);
            afterRead.run(p, a);
            return !fileKeys && type == BasicFileAttributes.class ? (A) new NoKey(a) : a;
        }

        @Override
        public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
            return d().readAttributes(unwrap(path), attributes, options);
        }

        @Override
        public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
            d().setAttribute(unwrap(path), attribute, value, options);
        }
    }

    @Override public FileSystemProvider provider() { return provider; }
    @Override public void close() {}
    @Override public boolean isOpen() { return true; }
    @Override public boolean isReadOnly() { return false; }
    @Override public String getSeparator() { return real.getSeparator(); }

    @Override
    public Iterable<Path> getRootDirectories() {
        List<Path> roots = new ArrayList<>();
        real.getRootDirectories().forEach(r -> roots.add(wrap(r)));
        return roots;
    }

    @Override public Iterable<FileStore> getFileStores() { return real.getFileStores(); }
    @Override public Set<String> supportedFileAttributeViews() { return real.supportedFileAttributeViews(); }
    @Override public Path getPath(String first, String... more) { return wrap(real.getPath(first, more)); }
    @Override public PathMatcher getPathMatcher(String syntaxAndPattern) { return real.getPathMatcher(syntaxAndPattern); }
    @Override public UserPrincipalLookupService getUserPrincipalLookupService() { return real.getUserPrincipalLookupService(); }
    @Override public WatchService newWatchService() throws IOException { throw new UnsupportedOperationException(); }

    /** {@return whether {@code root}'s directory streams are secure} (a sanity check for the tests) */
    static boolean secure(Path root) throws IOException {
        try (DirectoryStream<Path> d = Files.newDirectoryStream(root)) {
            return d instanceof java.nio.file.SecureDirectoryStream;
        }
    }
}
