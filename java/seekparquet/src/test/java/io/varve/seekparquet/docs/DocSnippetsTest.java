// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// The regions between tag::NAME[] and end::NAME[] are embedded in the
// documentation by go/internal/tools/docgen: after changing one, run
// go -C go run ./internal/tools/docgen -w.

package io.varve.seekparquet.docs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.varve.seekparquet.Bytes;
import io.varve.seekparquet.Dataset;
import io.varve.seekparquet.LimitExceededException;
import io.varve.seekparquet.ListEntry;
import io.varve.seekparquet.ListQuery;
import io.varve.seekparquet.Query;
import io.varve.seekparquet.RefusalException;
import io.varve.seekparquet.Row;
import io.varve.seekparquet.Span;
import io.varve.seekparquet.StorageException;
import io.varve.seekparquet.UnsupportedFeatureException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The Java code the documentation shows, each with the output the documentation states. */
class DocSnippetsTest {

    private static Path fixture(String name) {
        return Path.of(System.getProperty("seekparquet.conformance"), "datasets", name);
    }

    /**
     * Runs a snippet and returns the lines it printed. PrintStream.println ends lines with
     * System.lineSeparator() (CRLF on Windows), so the output is compared as lines, not as text.
     */
    private static List<String> run(Snippet snippet) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            snippet.run(out);
        }
        return bytes.toString(StandardCharsets.UTF_8).lines().toList();
    }

    private interface Snippet {
        void run(PrintStream out);
    }

    /** README.md: the same reads as the Go Example_readme, with the same output. */
    @Test
    void readme() {
        Path dir = fixture("multipart");
        List<String> output = run(out -> {
            // tag::readme[]
            try (Dataset dataset = Dataset.open(dir)) {
                // The first three keys under a prefix.
                Query query = Query.builder(Span.prefix(Bytes.utf8("logs/2026/01/"))).limit(3).build();
                for (Row row : dataset.rows(query)) {
                    out.println("row " + row.key().toStringUtf8());
                }

                // Common prefixes under logs/2026/, two per page, resuming from a cursor.
                ListQuery list = ListQuery.builder((byte) '/').prefix(Bytes.utf8("logs/2026/")).limit(2).build();
                for (int page = 1; ; page++) {
                    List<ListEntry> entries = dataset.list(list);
                    if (entries.isEmpty()) {
                        break;
                    }
                    for (ListEntry e : entries) {
                        out.println("page " + page + ": " + e.bytes().toStringUtf8());
                    }
                    list = list.next(entries.getLast());
                }
            }
            // end::readme[]
        });
        assertEquals("""
                row logs/2026/01/f000.gz
                row logs/2026/01/f001.gz
                row logs/2026/01/f002.gz
                page 1: logs/2026/01/
                page 1: logs/2026/02/
                page 2: logs/2026/03/
                """.lines().toList(), output);
    }

    /** docs/guides/troubleshooting.md: telling a refusal from the other kinds of failure. */
    @Test
    void errors() {
        Path dir = fixture("invalid/key-order-within-page");
        List<String> output = run(out -> {
            // tag::errors[]
            try (Dataset dataset = Dataset.open(dir)) {
                dataset.rows(Query.builder(Span.all()).limit(1000).build());
            } catch (RefusalException e) {
                // The data breaks the specification. The code is stable.
                out.println("refused: " + e.code().id() + " in " + e.part().orElse("the manifest"));
            } catch (UnsupportedFeatureException e) {
                // Valid data, a feature this reader does not implement.
                out.println("unsupported: " + e.code().id());
            } catch (LimitExceededException e) {
                // Your Limits, or a capacity of the reader; not a fault of the data.
                out.println("limit: " + e.limit());
            } catch (StorageException e) {
                // Reading storage failed; says nothing about the data.
                out.println("storage: " + e.getMessage());
            }
            // IllegalArgumentException: an invalid request.
            // IllegalStateException: the dataset is closed.
            // CancellationException: the read was cancelled.
            // end::errors[]
        });
        assertEquals(List.of("refused: key_order in data/part-00000.parquet"), output);
    }
}
