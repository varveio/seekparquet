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
 * thriftcheck} bounds it: each structure is charged {@value #STRUCT_BYTES} bytes when it begins, each
 * list, set or map element {@value #ELEMENT_BYTES} bytes (twice for a map entry) before the container's
 * elements are allocated, and each string or binary value its length plus {@value #STRUCT_BYTES}. Past the
 * budget the decoding fails with {@link LimitExceededException} naming {@link LimitName#METADATA_MEMORY}.
 * The charges are estimates of the JVM representation, not measured heap: the two readers need not
 * refuse the same metadata.
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
        ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
        try {
            int max = Math.max(len, 1);
            TIOStreamTransport transport =
                    new TIOStreamTransport(new TConfiguration(max, TConfiguration.DEFAULT_MAX_FRAME_SIZE, MAX_DEPTH + 1), in);
            struct.read(new Bounded(transport, max, budget));
        } catch (TException | RuntimeException e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof OverBudget) {
                    throw new LimitExceededException(LimitName.METADATA_MEMORY, -1, budget, "decoding metadata");
                }
            }
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

    /** The decoded estimate passed the budget; {@link #read} reports it as a {@link LimitExceededException}. */
    private static final class OverBudget extends RuntimeException {
        private static final long serialVersionUID = 1L;

        OverBudget() {
            super("the decoded Thrift structure exceeds its memory budget", null, false, false);
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

        /** The bytes the decoded structure may still be charged. */
        private long remaining;

        Bounded(TTransport transport, long max, long budget) {
            super(transport, max, max);
            this.remaining = budget;
        }

        private void charge(long bytes) {
            remaining -= bytes;
            if (remaining < 0) {
                throw new OverBudget();
            }
        }

        @Override
        public String readString() throws TException {
            String s = super.readString();
            charge(STRUCT_BYTES + (long) s.length());
            return s;
        }

        @Override
        public java.nio.ByteBuffer readBinary() throws TException {
            java.nio.ByteBuffer b = super.readBinary();
            charge(STRUCT_BYTES + (long) b.remaining());
            return b;
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
            charge(STRUCT_BYTES);
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
            charge((long) ELEMENT_BYTES * list.size);
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
            charge((long) ELEMENT_BYTES * set.size);
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
            charge(2L * ELEMENT_BYTES * map.size);
            return map;
        }

        @Override
        public void readMapEnd() throws TException {
            super.readMapEnd();
            level--;
        }
    }
}
