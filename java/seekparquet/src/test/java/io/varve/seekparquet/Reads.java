// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Test shorthand over the public read API: a query builder that takes bounds, limit and cursor in one
 * chain, and row- or batch-consumer forms of the scans. Every method is a direct composition of public
 * operations.
 */
final class Reads {

    private Reads() {}

    private static final Consumer<ReadStats> NO_STATS = s -> {};

    /** {@return a builder of bounds, limit, columns and cursor in one chain} */
    static Builder builder() {
        return new Builder();
    }

    static Query all(long limit) {
        return Query.builder().limit(limit).build();
    }

    static Query prefix(Bytes prefix, long limit) {
        return Query.builder(Span.prefix(prefix)).limit(limit).build();
    }

    static Query lookup(Bytes key, long limit) {
        return Query.builder(Span.key(key)).limit(limit).build();
    }

    static List<Selection> selections(List<Query> queries) {
        return queries.stream().map(Query::selection).toList();
    }

    static Plan plan(Dataset d, List<Query> queries, List<String> columns) {
        return d.plan(selections(queries), columns);
    }

    static Plan plan(Dataset d, List<Query> queries, List<String> columns, Cancellation cancellation) {
        return d.plan(selections(queries), columns, ReadOptions.of(cancellation));
    }

    static List<Row> range(Dataset d, Query q, Cancellation cancellation) {
        return d.rows(q, ReadOptions.of(cancellation));
    }

    static List<Row> range(Dataset d, Query q, Cancellation cancellation, Consumer<? super ReadStats> stats) {
        return d.rows(q, new ReadOptions(cancellation, stats));
    }

    static List<ListEntry> list(Dataset d, ListQuery q, Cancellation cancellation) {
        return d.list(q, ReadOptions.of(cancellation));
    }

    static List<ListEntry> list(Dataset d, ListQuery q, Cancellation cancellation, Consumer<? super ReadStats> stats) {
        return d.list(q, new ReadOptions(cancellation, stats));
    }

    /** A visitor that hands each batch to {@code action} and continues. */
    static BatchVisitor<RuntimeException> each(Consumer<? super Batch> action) {
        java.util.Objects.requireNonNull(action, "action");
        return batch -> {
            action.accept(batch);
            return true;
        };
    }

    static void rangeBatches(Dataset d, Query q, Consumer<? super Batch> action) {
        d.scan(q, each(action));
    }

    static void rangeBatches(Dataset d, Query q, Cancellation cancellation, Consumer<? super Batch> action) {
        d.scan(q, ReadOptions.of(cancellation), each(action));
    }

    static void rangeBatches(Dataset d, Query q, Cancellation cancellation, Consumer<? super ReadStats> stats, Consumer<? super Batch> action) {
        d.scan(q, new ReadOptions(cancellation, stats), each(action));
    }

    static void scanBatches(Plan plan, Consumer<? super Batch> action) {
        plan.scan(each(action));
    }

    static void scanBatches(Plan plan, Cancellation cancellation, Consumer<? super Batch> action) {
        plan.scan(ReadOptions.of(cancellation), each(action));
    }

    static void scanBatches(Plan plan, Cancellation cancellation, Consumer<? super ReadStats> stats, Consumer<? super Batch> action) {
        plan.scan(new ReadOptions(cancellation, stats), each(action));
    }

    /** Scans {@code plan}, handing {@code action} each row as {@link Batch#row} makes it. */
    static void scan(Plan plan, Consumer<? super Row> action) {
        scan(plan, Cancellation.none(), NO_STATS, action);
    }

    static void scan(Plan plan, Cancellation cancellation, Consumer<? super Row> action) {
        scan(plan, cancellation, NO_STATS, action);
    }

    static void scan(Plan plan, Cancellation cancellation, Consumer<? super ReadStats> stats, Consumer<? super Row> action) {
        java.util.Objects.requireNonNull(action, "action");
        plan.scan(new ReadOptions(cancellation, stats), each(batch -> {
            for (int i = 0; i < batch.size(); i++) {
                action.accept(batch.row(i));
            }
        }));
    }

    /** {@return the rows of {@code plan}} */
    static List<Row> rows(Plan plan) {
        List<Row> out = new ArrayList<>();
        scan(plan, out::add);
        return out;
    }

    /**
     * {@return the top-level columns of every part, each once, in the order they first appear, parts in
     * manifest order} It checks every part, through {@link Dataset#columns(int)}.
     */
    static List<Column> columns(Dataset d) {
        java.util.Map<String, Column> out = new java.util.LinkedHashMap<>();
        for (int i = 0; i < d.manifest().parts().size(); i++) {
            for (Column c : d.columns(i)) {
                out.putIfAbsent(c.name(), c);
            }
        }
        return List.copyOf(out.values());
    }

    /** Plans {@code queries}' selections and {@return the statistics planning delivered} */
    static ReadStats planStats(Dataset d, List<Query> queries, List<String> columns) {
        ReadStats[] stats = new ReadStats[1];
        d.plan(selections(queries), columns, ReadOptions.of(s -> stats[0] = s));
        return stats[0];
    }

    /** Builds a {@link Query} from bounds, limit, columns and cursor in one chain. Not thread-safe. */
    static final class Builder {
        private final Span.Builder span = Span.builder();
        private final Query.Builder query = Query.builder();

        Builder prefix(Bytes prefix) {
            span.prefix(prefix);
            return this;
        }

        Builder start(Bytes start) {
            span.start(start);
            return this;
        }

        Builder startAfter(Bytes start) {
            span.startAfter(start);
            return this;
        }

        Builder end(Bytes end) {
            span.end(end);
            return this;
        }

        Builder limit(long limit) {
            query.limit(limit);
            return this;
        }

        Builder columns(String... columns) {
            query.columns(columns);
            return this;
        }

        Builder columns(List<String> columns) {
            query.columns(columns);
            return this;
        }

        Builder after(Cursor cursor) {
            if (!(cursor instanceof Cursor.RowPosition row)) {
                throw new IllegalArgumentException("a range accepts only a row cursor");
            }
            query.cursor(row);
            return this;
        }

        Builder after(Optional<Cursor.RowPosition> cursor) {
            cursor.ifPresent(query::cursor);
            return this;
        }

        Query build() {
            return query.span(span.build()).build();
        }
    }
}
