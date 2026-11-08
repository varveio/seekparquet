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
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/**
 * Decodes parquet-format's Thrift structures (from parquet-java's {@code parquet-format-structures},
 * whose Thrift runtime it shades) with every size bounded by the bytes at hand: the message by the
 * buffer's length, and every string and list by it too. A hostile footer, page index or page header
 * can then make the decoder allocate no more than a small multiple of its own size.
 */
final class Thrift {

    private static final int MAX_RECURSION = 64;

    private Thrift() {}

    /** A decoded structure and the number of bytes it took. */
    record Decoded<T>(T value, int length) {}

    static <T extends TBase<?, ?>> Decoded<T> read(byte[] b, int off, int len, T struct) throws DataException {
        ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
        try {
            int max = Math.max(len, 1);
            TIOStreamTransport transport =
                    new TIOStreamTransport(new TConfiguration(max, TConfiguration.DEFAULT_MAX_FRAME_SIZE, MAX_RECURSION), in);
            struct.read(new TCompactProtocol(transport, max, max));
        } catch (TException | RuntimeException e) {
            throw new DataException("the Thrift structure does not decode", e);
        }
        return new Decoded<>(struct, len - in.available());
    }

    static FileMetaData fileMetaData(byte[] b) throws DataException {
        return read(b, 0, b.length, new FileMetaData()).value();
    }

    static ColumnIndex columnIndex(byte[] b) throws DataException {
        return read(b, 0, b.length, new ColumnIndex()).value();
    }

    static OffsetIndex offsetIndex(byte[] b) throws DataException {
        return read(b, 0, b.length, new OffsetIndex()).value();
    }

    static Decoded<PageHeader> pageHeader(byte[] b, int off, int len) throws DataException {
        return read(b, off, len, new PageHeader());
    }
}
