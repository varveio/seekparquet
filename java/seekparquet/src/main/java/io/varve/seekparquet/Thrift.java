// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.ByteArrayInputStream;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageHeader;
import shaded.parquet.org.apache.thrift.TBase;
import shaded.parquet.org.apache.thrift.TConfiguration;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TField;
import shaded.parquet.org.apache.thrift.protocol.TList;
import shaded.parquet.org.apache.thrift.protocol.TMap;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TSet;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;
import shaded.parquet.org.apache.thrift.transport.TTransport;

/**
 * Decodes parquet-format's Thrift structures (from parquet-java's {@code parquet-format-structures},
 * whose Thrift runtime it shades) with every size bounded by the bytes at hand: the message by the
 * buffer's length, and every string and list by it too. A hostile footer, page index or page header
 * can then make the decoder allocate no more than a small multiple of its own size.
 *
 * <p>Nesting is bounded as the Go reader's {@code internal/thriftcheck} bounds it: the top-level structure
 * is at depth 0, each field value, list, set or map element and map key or value one deeper, and no value
 * may be deeper than {@value #MAX_DEPTH} (Boolean struct fields, which the compact protocol carries in the
 * field header, excepted). This covers the fields the decoder skips as unknown too: the shaded Thrift
 * 0.23 runtime's {@code TProtocolUtil.skip} does not apply the configured recursion limit, so without
 * this bound a footer of a few kilobytes of nested unknown fields overflows the stack.
 *
 * <p>The decoded size is bounded by a budget ({@code Limits.metadataMemory()}), as the Go reader's {@code
 * thriftcheck} bounds it: before the decoder runs, a walk of the encoded bytes that allocates nothing
 * ({@link #preflight}) checks every declared length against the bytes that remain and charges each
 * structure {@value #STRUCT_BYTES} bytes, each list, set or map element {@value #ELEMENT_BYTES} bytes (twice
 * for a map entry), and each string or binary value twice its encoded length plus {@value #STRUCT_BYTES}
 * (a Java string can take two bytes per encoded UTF-8 byte). Past the budget
 * it fails with {@link LimitExceededException} naming {@link LimitName#METADATA_MEMORY}, before any string,
 * binary or container of the structure is allocated. The charges are estimates of the JVM representation,
 * not measured heap: the two readers need not refuse the same metadata.
 */
final class Thrift {

    /** The deepest value allowed, the top-level structure being at depth 0. */
    static final int MAX_DEPTH = 64;

    /** The charge for a decoded structure or a string's object. */
    static final int STRUCT_BYTES = 48;

    /** The charge for one container element: a reference and its slot. */
    static final int ELEMENT_BYTES = 16;

    private Thrift() {}

    /** A decoded structure and the number of bytes it took. */
    record Decoded<T>(T value, int length) {}

    static <T extends TBase<?, ?>> Decoded<T> read(byte[] b, int off, int len, T struct, long budget) throws DataException {
        preflight(b, off, len, budget);
        ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
        try {
            int max = Math.max(len, 1);
            TIOStreamTransport transport =
                    new TIOStreamTransport(new TConfiguration(max, TConfiguration.DEFAULT_MAX_FRAME_SIZE, MAX_DEPTH + 1), in);
            struct.read(new Bounded(transport, max));
        } catch (TException | RuntimeException e) {
            throw new DataException("the Thrift structure does not decode", e);
        }
        return new Decoded<>(struct, len - in.available());
    }

    private static <T extends TBase<?, ?>> T complete(byte[] bytes, T struct, long budget) throws DataException {
        Decoded<T> decoded = read(bytes, 0, bytes.length, struct, budget);
        if (decoded.length() != bytes.length) {
            throw new DataException("trailing bytes after the Thrift structure");
        }
        return decoded.value();
    }

    static FileMetaData fileMetaData(byte[] b, long budget) throws DataException {
        return complete(b, new FileMetaData(), budget);
    }

    static ColumnIndex columnIndex(byte[] b, long budget) throws DataException {
        return complete(b, new ColumnIndex(), budget);
    }

    static OffsetIndex offsetIndex(byte[] b, long budget) throws DataException {
        return complete(b, new OffsetIndex(), budget);
    }

    static Decoded<PageHeader> pageHeader(byte[] b, int off, int len, long budget) throws DataException {
        return read(b, off, len, new PageHeader(), budget);
    }


    /**
     * Walks one compact-protocol structure at {@code b[off, off + len)} without allocating, checking that
     * every declared length fits in the bytes that remain and charging the decoded estimate (see the class
     * comment) against {@code budget}.
     *
     * @throws LimitExceededException if the estimate exceeds the budget
     * @throws DataException if the bytes are not one well-formed structure; nesting far beyond {@value
     *     #MAX_DEPTH} is refused here, nesting near it by the decoder
     */
    static void preflight(byte[] b, int off, int len, long budget) throws DataException {
        Walk w = new Walk(b, off, off + len, budget);
        try {
            w.charge(STRUCT_BYTES);
            w.structure(0);
        } catch (Walk.Malformed e) {
            throw new DataException("the Thrift structure does not decode", e.getCause() != null ? e.getCause() : e);
        }
    }

    /** The preflight walk: a cursor over the bytes and the budget left. */
    private static final class Walk {
        /** Nesting the walk itself allows: beyond what the decoder admits, which checks the exact bound. */
        private static final int WALK_DEPTH = MAX_DEPTH + 2;

        private final byte[] b;
        private final long budget;
        private final int end;
        private int pos;
        private long left;

        /** Bytes that are not one well-formed structure. */
        static final class Malformed extends Exception {
            private static final long serialVersionUID = 1L;

            Malformed(String message, Throwable cause) {
                super(message, cause, false, false);
            }
        }

        Walk(byte[] b, int pos, int end, long budget) {
            this.b = b;
            this.pos = pos;
            this.end = end;
            this.budget = budget;
            this.left = budget;
        }

        void charge(long bytes) {
            left -= bytes;
            if (bytes < 0 || left < 0) {
                throw new LimitExceededException(LimitName.METADATA_MEMORY, -1, budget, "decoding metadata");
            }
        }

        private Malformed malformed(String what) {
            return new Malformed(what + " at byte " + pos, null);
        }

        private int nextByte() throws Malformed {
            if (pos >= end) {
                throw malformed("truncated");
            }
            return b[pos++] & 0xFF;
        }

        private long varint() throws Malformed {
            long x = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                int v = nextByte();
                x |= (long) (v & 0x7F) << shift;
                if (v < 0x80) {
                    return x;
                }
            }
            throw malformed("varint too long");
        }

        /** {@return a declared count of elements of at least {@code min} bytes each, if the bytes left hold them} */
        private int count(long n, int min) throws Malformed {
            if (n < 0 || n > (end - pos) / min) {
                throw malformed("a length beyond the input");
            }
            return (int) n;
        }

        void structure(int depth) throws Malformed {
            while (true) {
                int h = nextByte();
                if (h == 0) {
                    return;
                }
                int type = h & 0x0F;
                if ((h >> 4) == 0) {
                    varint(); // the field id
                }
                if (type == 1 || type == 2) {
                    continue; // a Boolean field carries its value in the header
                }
                value(type, depth + 1);
            }
        }

        private void value(int type, int depth) throws Malformed {
            if (depth > WALK_DEPTH) {
                throw new Malformed("too deep", new TProtocolException(TProtocolException.DEPTH_LIMIT,
                        "the Thrift structure is nested more than " + MAX_DEPTH + " deep"));
            }
            switch (type) {
                case 1, 2, 3 -> nextByte();
                case 4, 5, 6 -> varint();
                case 7 -> {
                    if (end - pos < 8) {
                        throw malformed("truncated");
                    }
                    pos += 8;
                }
                case 8 -> {
                    int n = count(varint(), 1);
                    // A string decodes to at most two bytes per encoded byte (UTF-16), a binary to one.
                    charge(STRUCT_BYTES + 2L * n);
                    pos += n;
                }
                case 9, 10 -> {
                    int h = nextByte();
                    long size = h >> 4;
                    if (size == 15) {
                        size = varint();
                    }
                    int n = count(size, 1);
                    charge((long) ELEMENT_BYTES * n);
                    for (int i = 0; i < n; i++) {
                        value(h & 0x0F, depth + 1);
                    }
                }
                case 11 -> {
                    int n = count(varint(), 2);
                    if (n == 0) {
                        return;
                    }
                    int kv = nextByte();
                    charge(2L * ELEMENT_BYTES * n);
                    for (int i = 0; i < n; i++) {
                        value(kv >> 4, depth + 1);
                        value(kv & 0x0F, depth + 1);
                    }
                }
                case 12 -> {
                    charge(STRUCT_BYTES);
                    structure(depth);
                }
                default -> throw malformed("unknown type " + type);
            }
        }
    }

    /**
     * The compact protocol with nesting bounded (see the class comment). It counts open containers in
     * the read*Begin and read*End methods, which both the generated readers and {@code
     * TProtocolUtil.skip} call for every struct, list, set and map, so skipped unknown fields are
     * bounded too. A structure at depth {@value #MAX_DEPTH} may hold only Boolean fields, and a
     * container there no elements, since those would be one deeper.
     */
    private static final class Bounded extends TCompactProtocol {
        /** Thrift type codes (TType, which the shaded runtime leaves out). */
        private static final byte STOP = 0;
        private static final byte BOOL = 2;

        /** Containers open, the top-level structure included: a container at depth d is at level d + 1. */
        private int level;

        Bounded(TTransport transport, long max) {
            super(transport, max, max);
        }

        private static TProtocolException tooDeep() {
            return new TProtocolException(TProtocolException.DEPTH_LIMIT, "the Thrift structure is nested more than " + MAX_DEPTH + " deep");
        }

        private void enter() throws TProtocolException {
            if (++level > MAX_DEPTH + 1) {
                throw tooDeep();
            }
        }

        /** Refuses elements in a container at depth {@value #MAX_DEPTH}: they would be deeper. */
        private void elements(int size) throws TProtocolException {
            if (size > 0 && level > MAX_DEPTH) {
                throw tooDeep();
            }
        }

        @Override
        public TStruct readStructBegin() throws TException {
            enter();
            return super.readStructBegin();
        }

        @Override
        public void readStructEnd() throws TException {
            super.readStructEnd();
            level--;
        }

        @Override
        public TField readFieldBegin() throws TException {
            TField field = super.readFieldBegin();
            if (field.type != STOP && field.type != BOOL && level > MAX_DEPTH) {
                throw tooDeep();
            }
            return field;
        }

        @Override
        public TList readListBegin() throws TException {
            enter();
            TList list = super.readListBegin();
            elements(list.size);
            return list;
        }

        @Override
        public void readListEnd() throws TException {
            super.readListEnd();
            level--;
        }

        @Override
        public TSet readSetBegin() throws TException {
            enter();
            // TCompactProtocol reads a set header as a list header; super's, so that it counts once.
            TSet set = new TSet(super.readListBegin());
            elements(set.size);
            return set;
        }

        @Override
        public void readSetEnd() throws TException {
            super.readSetEnd();
            level--;
        }

        @Override
        public TMap readMapBegin() throws TException {
            enter();
            TMap map = super.readMapBegin();
            elements(map.size);
            return map;
        }

        @Override
        public void readMapEnd() throws TException {
            super.readMapEnd();
            level--;
        }
    }
}
