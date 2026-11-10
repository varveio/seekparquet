// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// BenchmarkValueIndexRanges serves 1000-row requests projecting three value
// columns from large row groups of small pages, so that each request opens
// value chunks whose OffsetIndexes list hundreds of pages. Key bounds are
// written unshortened, so that the key seek decodes one page rather than
// the many that the records' long shared prefix would otherwise cost.
// Fixture generation and a first request into every row group, which loads
// the key page indexes, are outside the measurement.
func BenchmarkValueIndexRanges(b *testing.B) {
	const n, groupRows, pageRows = 131072, 65536, 1000
	records := reuseRecords(n)
	dir := write(b, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: groupRows, PageBytes: 4096, IndexSizeLimit: 64}}})
	d := open(b, dir, Options{})
	columns := []string{"size", "etag", "class"}
	for i := 0; i < n; i += groupRows {
		_, err := d.Range(ctx, Range{Start: records[i].Key, Limit: pageRows, Columns: columns})
		must(b, err)
	}
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		rows, err := d.Range(ctx, Range{Start: records[i].Key, Limit: pageRows, Columns: columns})
		must(b, err)
		if len(rows) != pageRows {
			b.Fatal(len(rows))
		}
		i = (i + pageRows) % (n - pageRows + 1)
	}
}
