// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * Bounds on the resources a dataset can make the reader use (spec §11). Every limit is checked against
 * what the files declare before anything is allocated for it, and exceeding one fails the operation with
 * {@link LimitExceededException}, naming the limit ({@link LimitName}): a policy of the caller, not a
 * refusal of the data. {@link #offsetIndexMemory()} is the exception: it bounds a cache, and exceeding it
 * never fails an operation. The Go reader's {@code Limits}, with the same settings and defaults.
 *
 * <p>Limits are immutable and made with a {@link #builder()}. A setting left unset, or set to 0, takes
 * its default; a negative setting is rejected when the limits are built (the Go reader rejects it when the
 * dataset is opened). The accessors return the effective values.
 *
 * <p>The settings are of three kinds. <b>Admission limits</b> bound what the data may declare: {@link
 * #manifestBytes()}, {@link #parts()}, {@link #footerBytes()}, {@link #pageIndexBytes()}, {@link
 * #metadataMemory()}, {@link #pageBytes()}, {@link #pageValues()} and {@link #pageMemory()}. <b>Retained
 * budgets</b> bound what one dataset keeps across requests: {@link #indexMemory()} (fails a request past
 * it) and {@link #offsetIndexMemory()} (a cache). <b>Result limits</b> bound a collected result: {@link
 * #getRows()}, for {@link Dataset#get} only; {@link Dataset#rows} and {@link Dataset#list} are bounded by
 * the caller's limit, and a {@link Dataset#scan} streams, with no result limit.
 *
 * <p>No limit bounds the part sources and footers a dataset keeps open for the parts it has touched (see
 * {@link Dataset}). The byte sizes of what the reader holds in one array (the manifest, a footer, a page
 * index, a page) are {@code int}s, since a Java array holds at most 2^31-1 bytes; the Go reader's are
 * 64-bit.
 */
public final class Limits {

    private static final int MIB = 1 << 20;

    /** The default of {@link #manifestBytes()}: 64 MiB. */
    public static final int DEFAULT_MANIFEST_BYTES = 64 * MIB;
    /** The default of {@link #parts()}: 1,000,000. */
    public static final int DEFAULT_PARTS = 1_000_000;
    /** The default of {@link #footerBytes()}: 16 MiB. */
    public static final int DEFAULT_FOOTER_BYTES = 16 * MIB;
    /** The default of {@link #pageIndexBytes()}: 16 MiB. */
    public static final int DEFAULT_PAGE_INDEX_BYTES = 16 * MIB;
    /** The default of {@link #metadataMemory()}: 256 MiB. */
    public static final long DEFAULT_METADATA_MEMORY = 256L * MIB;
    /** The default of {@link #pageBytes()}: 64 MiB. */
    public static final int DEFAULT_PAGE_BYTES = 64 * MIB;
    /** The default of {@link #pageValues()}: 16,777,216. */
    public static final int DEFAULT_PAGE_VALUES = 1 << 24;
    /** The default of {@link #indexMemory()}: 1 GiB. */
    public static final long DEFAULT_INDEX_MEMORY = 1L << 30;
    /** The default of {@link #offsetIndexMemory()}: 256 MiB. */
    public static final long DEFAULT_OFFSET_INDEX_MEMORY = 256L * MIB;
    /** The default of {@link #getRows()}: 1,000. */
    public static final int DEFAULT_GET_ROWS = 1000;

    /** Every setting at its default. */
    public static final Limits DEFAULT = builder().build();

    // The settings as given, 0 for the default.
    private final int manifestBytes;
    private final int parts;
    private final int footerBytes;
    private final int pageIndexBytes;
    private final long metadataMemory;
    private final int pageBytes;
    private final int pageValues;
    private final long pageMemory;
    private final long indexMemory;
    private final long offsetIndexMemory;
    private final int getRows;

    private Limits(Builder b) {
        this.manifestBytes = b.manifestBytes;
        this.parts = b.parts;
        this.footerBytes = b.footerBytes;
        this.pageIndexBytes = b.pageIndexBytes;
        this.metadataMemory = b.metadataMemory;
        this.pageBytes = b.pageBytes;
        this.pageValues = b.pageValues;
        this.pageMemory = b.pageMemory;
        this.indexMemory = b.indexMemory;
        this.offsetIndexMemory = b.offsetIndexMemory;
        this.getRows = b.getRows;
    }

    /** {@return a builder with every setting at its default} */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@return a builder holding these limits' settings as given} A setting that took its default stays
     * unset, so that a derived default ({@link #pageMemory()}) follows the settings it derives from.
     */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.manifestBytes = manifestBytes;
        b.parts = parts;
        b.footerBytes = footerBytes;
        b.pageIndexBytes = pageIndexBytes;
        b.metadataMemory = metadataMemory;
        b.pageBytes = pageBytes;
        b.pageValues = pageValues;
        b.pageMemory = pageMemory;
        b.indexMemory = indexMemory;
        b.offsetIndexMemory = offsetIndexMemory;
        b.getRows = getRows;
        return b;
    }

    /** {@return the largest {@code manifest.json}, in bytes} Default 64 MiB. */
    public int manifestBytes() {
        return manifestBytes == 0 ? DEFAULT_MANIFEST_BYTES : manifestBytes;
    }

    /**
     * {@return the most parts a manifest may list} The parts limit is enforced before materializing more than
     * the permitted number of part entries, including intermediate JSON representations. Default 1,000,000.
     */
    public int parts() {
        return parts == 0 ? DEFAULT_PARTS : parts;
    }

    /** {@return the largest encoded footer of one part, in bytes} Default 16 MiB. */
    public int footerBytes() {
        return footerBytes == 0 ? DEFAULT_FOOTER_BYTES : footerBytes;
    }

    /**
     * {@return the largest encoded ColumnIndex or OffsetIndex of one column chunk, in bytes} Default 16 MiB.
     */
    public int pageIndexBytes() {
        return pageIndexBytes == 0 ? DEFAULT_PAGE_INDEX_BYTES : pageIndexBytes;
    }

    /**
     * {@return the estimated decoded size of one footer, page index or page header, in bytes} The estimate
     * is computed by a walk of the encoded Thrift bytes before the decoder allocates anything: 48 bytes per
     * structure, 16 per list or set element (32 per map entry), and for each string or binary value 48 plus
     * twice its encoded length (a decoded Java string can take two bytes per UTF-8 byte), every declared
     * length checked against the bytes that remain. It bounds the decoded metadata, not its encoded size
     * (see {@link #footerBytes()}). It also caps the window a page header of unknown length is read in,
     * at 16 MiB. Default 256 MiB.
     */
    public long metadataMemory() {
        return metadataMemory == 0 ? DEFAULT_METADATA_MEMORY : metadataMemory;
    }

    /**
     * {@return the largest page, compressed or uncompressed, dictionary pages included, and the bytes of
     * its decoded values} Default 64 MiB. It bounds a page's bytes, not the memory of decoding it, which
     * {@link #pageMemory()} bounds.
     */
    public int pageBytes() {
        return pageBytes == 0 ? DEFAULT_PAGE_BYTES : pageBytes;
    }

    /** {@return the most values in one page or dictionary} Default 16,777,216. */
    public int pageValues() {
        return pageValues == 0 ? DEFAULT_PAGE_VALUES : pageValues;
    }

    /**
     * {@return the estimated memory decoding one page may hold, data and dictionary pages alike, key pages
     * included} It is estimated from the page's header before anything is allocated for its values: a slot
     * per value for its representation, a null flag per row of an optional column, and the bytes of its
     * values; a page over it fails naming {@link LimitName#PAGE_MEMORY}. It bounds one page's decoding,
     * not a request's total memory: concurrent requests, and the shards of a split plan scanned at once,
     * each decode their own pages.
     *
     * <p>Its default derives as the Go reader's does, {@code 48 × pageValues() + pageBytes()} (832 MiB at
     * the defaults of those), so the two readers start from the same number. This reader's estimate of a
     * page differs from the Go reader's (its decoded representation differs), so the two need not refuse
     * the same pages: a byte-array dictionary near both {@code pageValues()} and {@code pageBytes()} can
     * exceed the default here.
     */
    public long pageMemory() {
        return pageMemory == 0 ? PageDecoder.worstPageMemory(pageValues(), pageBytes()) : pageMemory;
    }

    /**
     * {@return the memory one dataset retains for key page indexes and decoded row-group first keys} A
     * request that would load past it fails. It counts page entries and their bound bytes plus first-key
     * bytes; not footers, transient decoding buffers or results. Default 1 GiB.
     */
    public long indexMemory() {
        return indexMemory == 0 ? DEFAULT_INDEX_MEMORY : indexMemory;
    }

    /**
     * {@return the memory one dataset retains for the checked page locations of projected column chunks'
     * OffsetIndexes} It is a cache, not a limit on reads: past it, each request loads the locations it needs
     * and drops them, as if no cache were kept, and nothing fails. It counts 32 bytes per kept page plus 64
     * per kept chunk, as the Go reader does, apart from {@link #indexMemory()}; JVM headers and alignment
     * can exceed it, and the slot array each opened part allocates (one reference per physical row group
     * and leaf) is not counted. Default 256 MiB.
     */
    public long offsetIndexMemory() {
        return offsetIndexMemory == 0 ? DEFAULT_OFFSET_INDEX_MEMORY : offsetIndexMemory;
    }

    /**
     * {@return the most bytes a page header of unknown length is read in: 16 MiB, far beyond any real header,
     * or {@link #metadataMemory()} when that is less} The Go reader's {@code headerBytes}.
     */
    int headerBytes() {
        return (int) Math.min(16L << 20, metadataMemory());
    }

    /**
     * {@return the most rows {@link Dataset#get} returns} A group of more rows fails with {@link
     * LimitExceededException} naming {@link LimitName#GET_ROWS}, before any value page is read; read such a
     * group in pages of {@link Dataset#rows} over {@link Span#key}. It bounds a convenience call's row
     * count, not its memory. Default 1,000.
     */
    public int getRows() {
        return getRows == 0 ? DEFAULT_GET_ROWS : getRows;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Limits l && manifestBytes() == l.manifestBytes() && parts() == l.parts() && footerBytes() == l.footerBytes()
                && pageIndexBytes() == l.pageIndexBytes() && metadataMemory() == l.metadataMemory() && pageBytes() == l.pageBytes()
                && pageValues() == l.pageValues() && pageMemory() == l.pageMemory() && indexMemory() == l.indexMemory()
                && offsetIndexMemory() == l.offsetIndexMemory() && getRows() == l.getRows();
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestBytes(), parts(), footerBytes(), pageIndexBytes(), metadataMemory(), pageBytes(), pageValues(),
                pageMemory(), indexMemory(), offsetIndexMemory(), getRows());
    }

    @Override
    public String toString() {
        return "Limits[manifestBytes=" + manifestBytes() + ", parts=" + parts() + ", footerBytes=" + footerBytes()
                + ", pageIndexBytes=" + pageIndexBytes() + ", metadataMemory=" + metadataMemory() + ", pageBytes=" + pageBytes()
                + ", pageValues=" + pageValues() + ", pageMemory=" + pageMemory() + ", indexMemory=" + indexMemory()
                + ", offsetIndexMemory=" + offsetIndexMemory() + ", getRows=" + getRows() + "]";
    }

    /**
     * Builds {@link Limits}. Each setting is named after its accessor; 0 means the default. Not
     * thread-safe.
     */
    public static final class Builder {
        private int manifestBytes;
        private int parts;
        private int footerBytes;
        private int pageIndexBytes;
        private long metadataMemory;
        private int pageBytes;
        private int pageValues;
        private long pageMemory;
        private long indexMemory;
        private long offsetIndexMemory;
        private int getRows;

        private Builder() {}

        /**
         * Sets {@link Limits#manifestBytes()}.
         *
         * @param bytes the bound, 0 for the default
         * @return this builder
         */
        public Builder manifestBytes(int bytes) {
            this.manifestBytes = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#parts()}.
         *
         * @param parts the bound, 0 for the default
         * @return this builder
         */
        public Builder parts(int parts) {
            this.parts = parts;
            return this;
        }

        /**
         * Sets {@link Limits#footerBytes()}.
         *
         * @param bytes the bound, 0 for the default
         * @return this builder
         */
        public Builder footerBytes(int bytes) {
            this.footerBytes = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#pageIndexBytes()}.
         *
         * @param bytes the bound, 0 for the default
         * @return this builder
         */
        public Builder pageIndexBytes(int bytes) {
            this.pageIndexBytes = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#metadataMemory()}.
         *
         * @param bytes the bound, 0 for the default
         * @return this builder
         */
        public Builder metadataMemory(long bytes) {
            this.metadataMemory = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#pageBytes()}.
         *
         * @param bytes the bound, 0 for the default
         * @return this builder
         */
        public Builder pageBytes(int bytes) {
            this.pageBytes = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#pageValues()}.
         *
         * @param values the bound, 0 for the default
         * @return this builder
         */
        public Builder pageValues(int values) {
            this.pageValues = values;
            return this;
        }

        /**
         * Sets {@link Limits#pageMemory()}.
         *
         * @param bytes the bound, 0 for the default derived from the page values and bytes
         * @return this builder
         */
        public Builder pageMemory(long bytes) {
            this.pageMemory = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#indexMemory()}.
         *
         * @param bytes the budget, 0 for the default
         * @return this builder
         */
        public Builder indexMemory(long bytes) {
            this.indexMemory = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#offsetIndexMemory()}.
         *
         * @param bytes the budget, 0 for the default
         * @return this builder
         */
        public Builder offsetIndexMemory(long bytes) {
            this.offsetIndexMemory = bytes;
            return this;
        }

        /**
         * Sets {@link Limits#getRows()}.
         *
         * @param rows the bound, 0 for the default
         * @return this builder
         */
        public Builder getRows(int rows) {
            this.getRows = rows;
            return this;
        }

        /**
         * Builds the limits.
         *
         * @return the limits
         * @throws IllegalArgumentException if a setting is negative
         */
        public Limits build() {
            check("manifestBytes", manifestBytes);
            check("parts", parts);
            check("footerBytes", footerBytes);
            check("pageIndexBytes", pageIndexBytes);
            check("metadataMemory", metadataMemory);
            check("pageBytes", pageBytes);
            check("pageValues", pageValues);
            check("pageMemory", pageMemory);
            check("indexMemory", indexMemory);
            check("offsetIndexMemory", offsetIndexMemory);
            check("getRows", getRows);
            return new Limits(this);
        }

        private static void check(String name, long value) {
            if (value < 0) {
                throw new IllegalArgumentException("Limits." + name + " is negative (" + value + ")");
            }
        }
    }
}
