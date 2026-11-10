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
 */
final class Thrift {

    /** The deepest value allowed, the top-level structure being at depth 0. */
    static final int MAX_DEPTH = 64;

    private Thrift() {}

    /** A decoded structure and the number of bytes it took. */
    record Decoded<T>(T value, int length) {}

    static <T extends TBase<?, ?>> Decoded<T> read(byte[] b, int off, int len, T struct) throws DataException {
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

    private static <T extends TBase<?, ?>> T complete(byte[] bytes, T struct) throws DataException {
        Decoded<T> decoded = read(bytes, 0, bytes.length, struct);
        if (decoded.length() != bytes.length) {
            throw new DataException("trailing bytes after the Thrift structure");
        }
        return decoded.value();
    }

    static FileMetaData fileMetaData(byte[] b) throws DataException {
        return complete(b, new FileMetaData());
    }

    static ColumnIndex columnIndex(byte[] b) throws DataException {
        return complete(b, new ColumnIndex());
    }

    static OffsetIndex offsetIndex(byte[] b) throws DataException {
        return complete(b, new OffsetIndex());
    }

    static Decoded<PageHeader> pageHeader(byte[] b, int off, int len) throws DataException {
        return read(b, off, len, new PageHeader());
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
