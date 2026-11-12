// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.ByteArrayInputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
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
import shaded.parquet.org.apache.thrift.transport.TTransportException;

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
 * thriftcheck} bounds it, in the protocol the decoder reads through, so that the budget holds for whatever
 * the decoder actually reads (a container's elements are decoded by the schema, whatever element type the
 * bytes declare; unknown fields are skipped through the same reads): every string or binary length is
 * checked against the bytes that remain and charged before its bytes are allocated, and each
 * structure twice {@value #STRUCT_BYTES} bytes (generated object and wire record), each list, set or map element {@value #ELEMENT_BYTES} bytes (twice
 * for a map entry), each binary value its length plus {@value #STRUCT_BYTES}, and each string twice its
 * encoded length plus {@value #STRUCT_BYTES} (a Java string can take two bytes per encoded UTF-8 byte). Past the budget
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
        // Generated required-enum validation must see a known placeholder. After the complete
        // structure has validated, restore unknown values to null, retaining their wire integers.
        struct = compatible(struct);
        ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
        Bounded protocol;
        try {
            int max = Math.max(len, 1);
            TIOStreamTransport transport =
                    new TIOStreamTransport(new TConfiguration(max, TConfiguration.DEFAULT_MAX_FRAME_SIZE, MAX_DEPTH + 1), in);
            String kind = struct instanceof FileMetaData ? "FileMetaData" : struct instanceof PageHeader ? "PageHeader"
                    : struct instanceof ColumnIndex ? "ColumnIndex" : struct.getClass().getSimpleName();
            protocol = new Bounded(transport, max, in, budget, kind);
            struct.read(protocol);
            restore(struct, protocol.root);
        } catch (OverBudget e) {
            throw new LimitExceededException(LimitName.METADATA_MEMORY, -1, budget, "decoding metadata");
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

    /** Raw values live with the decoded object; no global cache or lifetime extension. */
    private static final class RawFooter extends FileMetaData { Wire wire; }
    private static final class RawPage extends PageHeader { Wire wire; }
    private static final class RawIndex extends ColumnIndex { Wire wire; }
    static final class RawSchema extends SchemaElement {
        final Wire wire;
        RawSchema(SchemaElement se, Wire wire) { super(se); this.wire = wire; }
    }

    @SuppressWarnings("unchecked") // each replacement extends the requested generated root type
    private static <T extends TBase<?, ?>> T compatible(T value) {
        if (value.getClass() == FileMetaData.class) return (T) new RawFooter();
        if (value.getClass() == PageHeader.class) return (T) new RawPage();
        if (value.getClass() == ColumnIndex.class) return (T) new RawIndex();
        return value;
    }

    /** Only recognized schema paths participate; unknown structures cannot masquerade as enums. */
    static final class Wire {
        final String kind;
        final List<Wire> children = new ArrayList<>();
        final java.util.Map<Short, Integer> integers = new java.util.HashMap<>();
        short field;
        byte type;
        int members;
        short alternative;
        Wire(String kind) { this.kind = kind; }
        Wire child(String name) { return children.stream().filter(w -> w.kind.equals(name)).findFirst().orElse(null); }
        int integer(int id, int fallback) { return integers.getOrDefault((short) id, fallback); }
    }

    private static String childKind(Wire parent) {
        return switch (parent.kind + ":" + parent.field) {
            case "FileMetaData:2" -> "SchemaElement";
            case "FileMetaData:4" -> "RowGroup";
            case "RowGroup:1" -> "ColumnChunk";
            case "ColumnChunk:3" -> "ColumnMetaData";
            case "SchemaElement:10" -> "LogicalType";
            case "LogicalType:7" -> "TimeType";
            case "LogicalType:8" -> "TimestampType";
            case "TimeType:2", "TimestampType:2" -> "TimeUnit";
            case "PageHeader:5" -> "DataPageHeader";
            case "PageHeader:7" -> "DictionaryPageHeader";
            case "PageHeader:8" -> "DataPageHeaderV2";
            default -> "";
        };
    }

    private static void restore(TBase<?, ?> value, Wire wire) {
        if (value instanceof RawFooter f) {
            f.wire = wire;
            int si = 0, gi = 0;
            for (Wire w : wire.children) {
                if (w.kind.equals("SchemaElement")) {
                    f.getSchema().set(si, new RawSchema(f.getSchema().get(si), w));
                    si++;
                } else if (w.kind.equals("RowGroup")) {
                    RowGroup group = f.getRow_groups().get(gi++);
                    int ci = 0;
                    for (Wire chunk : w.children) {
                        if (!chunk.kind.equals("ColumnChunk")) continue;
                        ColumnMetaData md = group.getColumns().get(ci++).getMeta_data();
                        Wire m = chunk.child("ColumnMetaData");
                        if (m != null && CompressionCodec.findByValue(m.integer(4, 0)) == null) md.setCodec(null);
                    }
                }
            }
        } else if (value instanceof RawPage p) {
            p.wire = wire;
            for (Wire w : wire.children) {
                if (w.kind.equals("DataPageHeader")) {
                    DataPageHeader h = p.getData_page_header();
                    h.setEncoding(Encoding.findByValue(w.integer(2, h.getEncoding().getValue())));
                    h.setDefinition_level_encoding(Encoding.findByValue(w.integer(3, h.getDefinition_level_encoding().getValue())));
                    h.setRepetition_level_encoding(Encoding.findByValue(w.integer(4, h.getRepetition_level_encoding().getValue())));
                } else if (w.kind.equals("DataPageHeaderV2")) {
                    p.getData_page_header_v2().setEncoding(Encoding.findByValue(w.integer(4, 0)));
                } else if (w.kind.equals("DictionaryPageHeader")) {
                    p.getDictionary_page_header().setEncoding(Encoding.findByValue(w.integer(2, 0)));
                }
            }
        } else if (value instanceof RawIndex i) {
            i.wire = wire;
            i.setBoundary_order(BoundaryOrder.findByValue(wire.integer(4, 0)));
        }
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

        /** The input, whose remaining bytes bound every declared length. */
        private final ByteArrayInputStream in;

        /** The decoded-size budget left. */
        private long left;

        private final String rootKind;
        private final ArrayDeque<Wire> structs = new ArrayDeque<>();
        private Wire root;

        Bounded(TTransport transport, long max, ByteArrayInputStream in, long budget, String rootKind) {
            super(transport, max, max);
            this.in = in;
            this.left = budget;
            this.rootKind = rootKind;
        }

        /** Charges the decoded estimate of what is about to be allocated, before it is. */
        private void charge(long bytes) {
            left -= bytes;
            if (bytes < 0 || left < 0) {
                throw new OverBudget();
            }
        }

        /**
         * Reads a string or binary length (an unsigned varint of at most 5 bytes, as the compact protocol
         * writes it) and checks it against the bytes that remain, before anything is allocated for it.
         */
        private int length() throws TException {
            int n = size32("a length");
            if (n > in.available()) {
                throw new TProtocolException(TProtocolException.SIZE_LIMIT, "a length of " + n + " beyond the " + in.available() + " bytes left");
            }
            return n;
        }

        /**
         * Reads an unsigned varint size of at most 5 bytes into a {@code long}, and {@return it}, refusing an
         * encoding wider than 5 bytes and a value above {@link Integer#MAX_VALUE} rather than wrapping it.
         */
        private int size32(String what) throws TException {
            long n = 0;
            for (int i = 0; ; i++) {
                if (i == 5) {
                    throw new TProtocolException(TProtocolException.INVALID_DATA, what + " varint is wider than 5 bytes");
                }
                int b = readByte() & 0xFF;
                n |= (long) (b & 0x7F) << (7 * i);
                if ((b & 0x80) == 0) {
                    break;
                }
            }
            if (n > Integer.MAX_VALUE) {
                throw new TProtocolException(TProtocolException.SIZE_LIMIT, what + " of " + n + " is beyond 2^31-1");
            }
            return (int) n;
        }

        /** {@return the Thrift type of a compact-protocol type code}, as the runtime maps it */
        private static byte type(int compact) throws TProtocolException {
            return switch (compact) {
                case 0 -> 0;          // STOP
                case 1, 2 -> 2;       // BOOL
                case 3 -> 3;          // BYTE
                case 4 -> 6;          // I16
                case 5 -> 8;          // I32
                case 6 -> 10;         // I64
                case 7 -> 4;          // DOUBLE
                case 8 -> 11;         // STRING
                case 9 -> 15;         // LIST
                case 10 -> 14;        // SET
                case 11 -> 13;        // MAP
                case 12 -> 12;        // STRUCT
                case 13 -> 16;        // UUID
                default -> throw new TProtocolException(TProtocolException.INVALID_DATA, "don't know what type: " + compact);
            };
        }

        /**
         * {@return the fewest bytes one element of Thrift type {@code type} takes}, as the runtime's {@code
         * getMinSerializedSize}, which has no UUID case: a container of UUIDs is refused, as the runtime
         * refuses it.
         */
        private static int minBytes(byte type) throws TTransportException {
            return switch (type) {
                case 4 -> 8;          // DOUBLE
                case 16 -> throw new TTransportException("unrecognized type code");
                default -> 1;
            };
        }

        /** Refuses a container of {@code count} elements of {@code min} bytes each that the bytes left cannot hold. */
        private void fits(long count, long min, String what) throws TProtocolException {
            if (count * min > in.available()) {
                throw new TProtocolException(TProtocolException.SIZE_LIMIT,
                        what + " of " + count + " elements beyond the " + in.available() + " bytes left");
            }
        }

        /**
         * Reads a list or set header itself, as the runtime's {@code readListBegin} does but with its size
         * checked by {@link #size32} and against the bytes actually left in the input (the runtime checks the
         * transport's message allowance, which this transport never decrements), before the generated reader
         * preallocates for it.
         */
        private TList listHeader(String what) throws TException {
            int h = readByte() & 0xFF;
            int size = (h >> 4) & 0x0F;
            byte elem = type(h & 0x0F);
            if (size == 15) {
                size = size32(what + " size");
            }
            fits(size, minBytes(elem), what);
            return new TList(elem, size);
        }

        private byte[] payload(int n) throws TException {
            byte[] b = new byte[n];
            if (n > 0) {
                getTransport().readAll(b, 0, n);
            }
            return b;
        }

        /**
         * Every binary the decoder reads, including those {@code TProtocolUtil.skip} reads for an unknown
         * field and those a container's elements hold whatever element type it declares, comes through here:
         * charged before it is allocated.
         */
        @Override
        public java.nio.ByteBuffer readBinary() throws TException {
            int n = length();
            charge(STRUCT_BYTES + (long) n);
            return java.nio.ByteBuffer.wrap(payload(n));
        }

        /** As {@link #readBinary()}, a string charged at twice its encoded length (UTF-16). */
        @Override
        public String readString() throws TException {
            int n = length();
            charge(STRUCT_BYTES + 2L * n);
            return new String(payload(n), java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public java.util.UUID readUuid() throws TException {
            charge(STRUCT_BYTES);
            return super.readUuid();
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
            charge(2L * STRUCT_BYTES); // generated object and its wire-presence record
            Wire w = new Wire(structs.isEmpty() ? rootKind : childKind(structs.peek()));
            if (structs.isEmpty()) root = w;
            else structs.peek().children.add(w);
            structs.push(w);
            return super.readStructBegin();
        }

        @Override
        public void readStructEnd() throws TException {
            super.readStructEnd();
            structs.pop();
            level--;
        }

        @Override
        public TField readFieldBegin() throws TException {
            TField field = super.readFieldBegin();
            if (field.type != STOP && field.type != BOOL && level > MAX_DEPTH) {
                throw tooDeep();
            }
            Wire w = structs.peek();
            w.field = field.id;
            w.type = field.type;
            if (field.type != STOP) { w.members++; w.alternative = field.id; }
            return field;
        }

        @Override
        public int readI32() throws TException {
            int raw = super.readI32();
            Wire w = structs.peek();
            if (w.type != 8) return raw; // only direct I32 fields, never values skipped inside another wire type
            String field = w.kind + ":" + w.field;
            boolean encoding = switch (field) {
                case "DataPageHeader:2", "DataPageHeader:3", "DataPageHeader:4",
                     "DataPageHeaderV2:4", "DictionaryPageHeader:2" -> true;
                default -> false;
            };
            // An I32 field only. Missing/wrong-type fields still
            // fail generated validation. No bytes are invented or skipped by this adaptation.
            if (encoding || field.equals("ColumnMetaData:4") || field.equals("ColumnIndex:4")) {
                charge(32);
                w.integers.put(w.field, raw);
                if (encoding && Encoding.findByValue(raw) == null) return Encoding.PLAIN.getValue();
                if (field.equals("ColumnMetaData:4") && CompressionCodec.findByValue(raw) == null) return CompressionCodec.LZO.getValue();
                if (field.equals("ColumnIndex:4") && BoundaryOrder.findByValue(raw) == null) return BoundaryOrder.UNORDERED.getValue();
            }
            return raw;
        }

        @Override
        public TList readListBegin() throws TException {
            enter();
            TList list = listHeader("a list");
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
            // The compact protocol writes a set header as a list header.
            TSet set = new TSet(listHeader("a set"));
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
            int size = size32("a map size");
            int kv = size == 0 ? 0 : readByte() & 0xFF;
            byte key = type(kv >> 4);
            byte value = type(kv & 0x0F);
            if (size > 0) {
                fits(size, (long) minBytes(key) + minBytes(value), "a map");
            }
            TMap map = new TMap(key, value, size);
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
