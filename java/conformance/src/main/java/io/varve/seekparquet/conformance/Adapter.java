// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.conformance;

import io.varve.seekparquet.Bytes;
import io.varve.seekparquet.Continuation;
import io.varve.seekparquet.Dataset;
import io.varve.seekparquet.FormatVersion;
import io.varve.seekparquet.Limits;
import io.varve.seekparquet.ListEntry;
import io.varve.seekparquet.ListQuery;
import io.varve.seekparquet.RangePlan;
import io.varve.seekparquet.RangeQuery;
import io.varve.seekparquet.RefusalException;
import io.varve.seekparquet.Row;
import io.varve.seekparquet.UnsupportedFeatureException;
import io.varve.seekparquet.Value;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The conformance adapter (conformance/protocol.md): reads one JSON request per line on stdin and
 * writes one JSON response per line on stdout, driving the library as an application would. It
 * claims the {@code reader} target and the optional {@code verify} and {@code planning} capabilities.
 * Diagnostics go to stderr.
 */
public final class Adapter {

    /** The members of a planned range (protocol.md, plan). */
    private static final java.util.Set<String> PLANNED_RANGE =
            java.util.Set.of("prefix_base64", "start_base64", "start_exclusive", "end_base64", "after");

    private final Map<String, Dataset> open = new HashMap<>();

    private final int pageLimit;

    Adapter() {
        this(Limits.DEFAULT.maxResultEntries());
    }

    // A smaller page size lets tests exercise continuations without huge fixtures.
    Adapter(int pageLimit) {
        if (pageLimit <= 0 || pageLimit > Limits.DEFAULT.maxResultEntries()) {
            throw new IllegalArgumentException("page limit exceeds the library default");
        }
        this.pageLimit = pageLimit;
    }

    /**
     * Runs the adapter until stdin closes.
     *
     * @param args ignored
     * @throws IOException if stdin or stdout fails
     */
    public static void main(String[] args) throws IOException {
        PrintStream out = new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), false, StandardCharsets.UTF_8);
        // Keep stdout for responses only.
        System.setOut(System.err);
        Adapter adapter = new Adapter();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                out.print(adapter.handle(line));
                out.print('\n');
                out.flush();
            }
        } finally {
            adapter.open.values().forEach(Dataset::close);
        }
    }

    /** A malformed request (QUERY-1, or a malformed member). */
    private static final class BadRequest extends Exception {
        private static final long serialVersionUID = 1L;

        BadRequest(String message) {
            super(message);
        }
    }

    String handle(String line) {
        Object id = MiniJson.NULL;
        StringBuilder out = new StringBuilder();
        try {
            if (!(MiniJson.parse(line) instanceof Map<?, ?> raw)) {
                return response(id, "error").append(",\"message\":\"the request is not an object\"}").toString();
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> req = (Map<String, Object>) raw;
            id = req.getOrDefault("id", MiniJson.NULL);
            String op = req.get("op") instanceof String s ? s : "";
            switch (op) {
                case "hello" -> {
                    out.append(response(id, "ok"));
                    out.append(",\"implementation\":\"seekparquet-java\",\"version\":\"0.1.0-dev\",\"supports\":[");
                    for (int i = 0; i < FormatVersion.SUPPORTED.size(); i++) {
                        FormatVersion v = FormatVersion.SUPPORTED.get(i);
                        out.append(i == 0 ? "" : ",").append("{\"format_version\":").append(v.formatVersion());
                        if (v.draftRevision().isPresent()) {
                            out.append(",\"draft_revision\":").append(v.draftRevision().getAsLong());
                        }
                        out.append('}');
                    }
                    out.append("],\"targets\":[\"reader\"],\"capabilities\":[\"open\",\"range\",\"list\",\"projection\",\"verify\",\"planning\"]}");
                }
                case "open" -> {
                    // A fresh handle: part checks kept by an earlier one do not carry over.
                    if (req.get("dataset") instanceof String path && open.remove(path) instanceof Dataset held) {
                        held.close();
                    }
                    dataset(req);
                    out.append(response(id, "ok")).append('}');
                }
                case "verify" -> {
                    dataset(req).verify();
                    out.append(response(id, "ok")).append('}');
                }
                case "range" -> {
                    BigInteger remaining = limit(req);
                    RangeQuery q = rangeQuery(req, pageSize(remaining));
                    Dataset d = dataset(req);
                    out.append(response(id, "ok")).append(",\"entries\":[");
                    boolean first = true;
                    while (true) {
                        List<Row> rows = d.range(q);
                        for (Row r : rows) {
                            if (!first) {
                                out.append(',');
                            }
                            first = false;
                            row(out, r);
                        }
                        remaining = remaining.subtract(BigInteger.valueOf(rows.size()));
                        if (rows.size() < q.limit() || remaining.signum() == 0) {
                            break;
                        }
                        q = q.next(rows.getLast()).toBuilder().limit(pageSize(remaining)).build();
                    }
                    out.append("]}");
                }
                case "list" -> {
                    BigInteger remaining = limit(req);
                    ListQuery q = listQuery(req, pageSize(remaining));
                    Dataset d = dataset(req);
                    if (System.getenv("SEEKPARQUET_LIST_INDEX") != null) d.buildListIndex();
                    out.append(response(id, "ok")).append(",\"entries\":[");
                    boolean first = true;
                    while (true) {
                        List<ListEntry> entries = d.list(q);
                        for (ListEntry entry : entries) {
                            if (!first) {
                                out.append(',');
                            }
                            first = false;
                            switch (entry) {
                                case ListEntry.RowEntry r -> row(out, r.row());
                                case ListEntry.CommonPrefix p -> {
                                    out.append("{\"kind\":\"prefix\",\"key_base64\":\"").append(p.prefix().toBase64()).append('"');
                                    if (p.exact().isPresent()) {
                                        out.append(",\"exact\":{\"offset\":").append(p.exact().get().offset()).append(',');
                                        values(out, p.exact().get());
                                        out.append('}');
                                    }
                                    out.append('}');
                                }
                            }
                        }
                        remaining = remaining.subtract(BigInteger.valueOf(entries.size()));
                        if (entries.size() < q.limit() || remaining.signum() == 0) {
                            break;
                        }
                        q = q.next(entries.getLast()).toBuilder().limit(pageSize(remaining)).build();
                    }
                    out.append("]}");
                }
                case "plan" -> {
                    if (!(req.get("ranges") instanceof List<?> rawRanges)) {
                        throw new BadRequest("ranges is not an array");
                    }
                    List<RangeQuery> ranges = new ArrayList<>(rawRanges.size());
                    for (Object r : rawRanges) {
                        if (!(r instanceof Map<?, ?> m)) {
                            throw new BadRequest("a range is not an object");
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> range = (Map<String, Object>) m;
                        // A planned range has bounds and a cursor only: no limit, no columns of its own.
                        if (!PLANNED_RANGE.containsAll(range.keySet())) {
                            throw new BadRequest("a planned range has undefined members");
                        }
                        ranges.add(rangeQuery(range, 1));
                    }
                    RangePlan plan = dataset(req).planRanges(ranges, columns(req));
                    StringBuilder entries = new StringBuilder();
                    plan.scan(row -> {
                        if (!entries.isEmpty()) {
                            entries.append(',');
                        }
                        row(entries, row);
                    });
                    out.append(response(id, "ok")).append(",\"entries\":[").append(entries)
                            .append("],\"count\":").append(plan.count())
                            .append(",\"count_exact\":").append(plan.countExact())
                            .append(",\"projected_bytes\":").append(plan.projectedBytes())
                            .append(",\"projected_bytes_exact\":").append(plan.projectedBytesExact()).append('}');
                }
                default -> {
                    out.append(response(id, "unsupported")).append(",\"code\":\"unsupported_operation\",\"message\":");
                    MiniJson.quote(out, "operation " + op + " is not implemented");
                    out.append('}');
                }
            }
            return out.toString();
        } catch (BadRequest | IllegalArgumentException e) {
            return withMessage(response(id, "invalid_request"), e.getMessage());
        } catch (RefusalException e) {
            StringBuilder r = response(id, "refused").append(",\"code\":\"").append(e.code().id()).append('"');
            return withMessage(r, e.getMessage());
        } catch (UnsupportedFeatureException e) {
            StringBuilder r = response(id, "unsupported").append(",\"code\":\"").append(e.code().id()).append('"');
            return withMessage(r, e.getMessage());
        } catch (RuntimeException e) {
            e.printStackTrace();
            return withMessage(response(id, "error"), e.toString());
        }
    }

    private static String withMessage(StringBuilder r, String message) {
        r.append(",\"message\":");
        MiniJson.quote(r, message == null ? "" : message);
        return r.append('}').toString();
    }

    private static StringBuilder response(Object id, String status) {
        StringBuilder sb = new StringBuilder("{\"id\":");
        if (id instanceof String s) {
            MiniJson.quote(sb, s);
        } else {
            sb.append("null");
        }
        return sb.append(",\"status\":\"").append(status).append('"');
    }

    private Dataset dataset(Map<String, Object> req) throws BadRequest {
        if (!(req.get("dataset") instanceof String path)) {
            throw new BadRequest("dataset is missing");
        }
        Dataset d = open.get(path);
        if (d == null) {
            d = Dataset.open(Path.of(path));
            open.put(path, d);
        }
        return d;
    }

    private static RangeQuery rangeQuery(Map<String, Object> req, int limit) throws BadRequest {
        RangeQuery.Builder b = RangeQuery.builder().limit(limit);
        Bytes prefix = bytes(req, "prefix_base64");
        if (prefix != null) {
            b.prefix(prefix);
        }
        Bytes start = bytes(req, "start_base64");
        if (start != null) {
            if (bool(req, "start_exclusive")) {
                b.startAfter(start);
            } else {
                b.start(start);
            }
        } else {
            bool(req, "start_exclusive");
        }
        Bytes end = bytes(req, "end_base64");
        if (end != null) {
            b.end(end);
        }
        Continuation after = cursor(req);
        if (after != null) {
            b.after(after);
        }
        return b.columns(columns(req)).build();
    }

    private static ListQuery listQuery(Map<String, Object> req, int limit) throws BadRequest {
        Bytes delimiter = bytes(req, "delimiter_base64");
        if (delimiter == null || delimiter.length() != 1) {
            throw new BadRequest("the delimiter is not exactly one byte");
        }
        ListQuery.Builder b = ListQuery.builder(delimiter.byteAt(0)).limit(limit);
        Bytes prefix = bytes(req, "prefix_base64");
        if (prefix != null) {
            b.prefix(prefix);
        }
        Bytes end = bytes(req, "end_base64");
        if (end != null) {
            b.end(end);
        }
        Continuation tagged = cursor(req);
        if (tagged != null) {
            b.after(tagged);
        }
        return b.columns(columns(req)).build();
    }

    private static Continuation cursor(Map<String, Object> req) throws BadRequest {
        Object value = req.get("after");
        if (value == null || value == MiniJson.NULL) {
            return null;
        }
        if (!(value instanceof Map<?, ?> raw)) {
            throw new BadRequest("after must be a tagged cursor");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) raw;
        if (m.keySet().equals(java.util.Set.of("prefix_base64"))) {
            Bytes prefix = bytes(m, "prefix_base64");
            if (prefix == null) {
                throw new BadRequest("cursor prefix must be base64");
            }
            return new Continuation.CommonPrefix(prefix);
        }
        if (!m.keySet().equals(java.util.Set.of("key_base64", "offset"))) {
            throw new BadRequest("cursor has undefined or missing members");
        }
        Bytes key = bytes(m, "key_base64");
        if (key == null || !(m.get("offset") instanceof MiniJson.Number n)
                || !n.text().matches("0|[1-9][0-9]*")) {
            throw new BadRequest("cursor requires a key and a non-negative integer offset");
        }
        try {
            return new Continuation.RowPosition(key, Long.parseLong(n.text()));
        } catch (NumberFormatException e) {
            throw new BadRequest("cursor offset exceeds 2^63-1");
        }
    }

    private static BigInteger limit(Map<String, Object> req) throws BadRequest {
        if (!(req.get("limit") instanceof MiniJson.Number n) || !n.isInteger()) {
            throw new BadRequest("limit is not an integer");
        }
        BigInteger v = new BigInteger(n.text());
        if (v.signum() <= 0) {
            throw new BadRequest("limit is not positive");
        }
        return v;
    }

    private int pageSize(BigInteger remaining) {
        return remaining.min(BigInteger.valueOf(pageLimit)).intValueExact();
    }

    /** Returns the decoded member, or null when absent or JSON null. */
    private static Bytes bytes(Map<String, Object> req, String name) throws BadRequest {
        Object v = req.get(name);
        if (v == null || v == MiniJson.NULL) {
            return null;
        }
        if (!(v instanceof String s)) {
            throw new BadRequest(name + " is not a string");
        }
        try {
            return Bytes.fromBase64(s);
        } catch (IllegalArgumentException e) {
            throw new BadRequest(name + ": " + e.getMessage());
        }
    }

    private static boolean bool(Map<String, Object> req, String name) throws BadRequest {
        Object v = req.get(name);
        if (v == null || v == MiniJson.NULL) {
            return false;
        }
        if (!(v instanceof Boolean b)) {
            throw new BadRequest(name + " is not a boolean");
        }
        return b;
    }

    private static List<String> columns(Map<String, Object> req) throws BadRequest {
        Object v = req.get("columns");
        if (v == null || v == MiniJson.NULL) {
            return List.of();
        }
        if (!(v instanceof List<?> l)) {
            throw new BadRequest("columns is not an array");
        }
        List<String> out = new ArrayList<>(l.size());
        for (Object o : l) {
            if (!(o instanceof String s)) {
                throw new BadRequest("columns holds a non-string");
            }
            out.add(s);
        }
        return out;
    }

    private static void row(StringBuilder out, Row row) {
        out.append("{\"kind\":\"row\",\"key_base64\":\"").append(row.key().toBase64()).append("\",\"offset\":").append(row.offset()).append(',');
        values(out, row);
        out.append('}');
    }

    private static void values(StringBuilder out, Row row) {
        out.append("\"values\":[");
        List<Value> values = row.values();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            switch (values.get(i)) {
                case Value.Absent a -> out.append("{\"absent\":true}");
                case Value.Null n -> out.append("{\"null\":true}");
                case Value.Bool b -> out.append("{\"bool\":").append(b.value()).append('}');
                case Value.Int32 x -> out.append("{\"int\":\"").append(x.value()).append("\"}");
                case Value.Int64 x -> out.append("{\"int\":\"").append(x.value()).append("\"}");
                case Value.Float32 f -> out.append("{\"float32\":\"").append(String.format("%08x", f.bits())).append("\"}");
                case Value.Float64 f -> out.append("{\"float64\":\"").append(String.format("%016x", f.bits())).append("\"}");
                case Value.ByteArray x -> out.append("{\"bytes_base64\":\"").append(x.value().toBase64()).append("\"}");
                case Value.FixedLenByteArray x -> out.append("{\"bytes_base64\":\"").append(x.value().toBase64()).append("\"}");
            }
        }
        out.append(']');
    }
}
