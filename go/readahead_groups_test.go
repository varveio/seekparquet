// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// wideFolderDataset writes folders folders of perFolder keys, each written
// repeat times, as one key-only part of pageRows-row pages in row groups of
// groupRows rows, so that a folder spans many key pages, and a row group
// holds a few folders or a part of one.
func wideFolderDataset(t testing.TB, folders, perFolder, pageRows int, groupRows int64, repeat int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "crawl")
	schema := parquet.NewSchema("listing", parquet.Group{"key": parquet.String()})
	opt := local.DatasetOptions{
		Part:          write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: repeat <= 1}, SchemaID: "test/readahead/v1", PageRows: pageRows, RowGroupRows: groupRows},
		AllowUnsynced: runtime.GOOS == "windows",
	}
	if err := local.WriteDir(context.Background(), dir, schema, &folderRows{folders: folders, perFolder: perFolder, repeat: repeat}, opt); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestListReadAheadWideFolders lists folders that each span many key pages,
// across many row groups, over a source that is not local. A listing that
// skips from folder to folder decodes one key page for each folder: the
// read-ahead must read those pages and no others, however many pages and
// row groups lie between them, and the skips must not decode a row group's
// first key page to route. Before this was so, the read-ahead fell back to
// reading pages in file order at every row group and every page it had not
// predicted, and read nearly every key page under the listed prefix.
func TestListReadAheadWideFolders(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	const folders, perFolder, pageRows, groupRows = 120, 2000, 50, 9000
	dir := wideFolderDataset(t, folders, perFolder, pageRows, groupRows, 1)
	prefix := []byte("crawl-data/CC-MAIN-2024-18/segments/")
	keyPages := int64(folders * perFolder / pageRows)

	local, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	for _, limit := range []int{1000, 25} {
		req := seekparquet.ListQuery{Prefix: prefix, Delimiter: '/', Limit: limit}
		want, err := local.List(context.Background(), req, nil)
		if err != nil {
			t.Fatal(err)
		}
		entries := int64(min(limit, folders))
		if int64(len(want)) != entries {
			t.Fatalf("limit %d: %d entries, want %d", limit, len(want), entries)
		}
		off := measureList(t, dir, 0, req, seekparquet.OpenOptions{Read: seekparquet.ReadPolicy{DisableSpeculation: true}})
		sameEntries(t, "without speculation", off.entries, want)
		on := measureList(t, dir, 0, req, seekparquet.OpenOptions{})
		sameEntries(t, "read-ahead", on.entries, want)
		st := on.stats
		t.Logf("limit %d: %d entries, %d reads of %d bytes, %d key pages decoded, %d read ahead, %d unused; without speculation %d reads, %d key pages",
			limit, len(on.entries), on.reads, on.bytes, st.KeyPagesDecoded, st.ReadAheadReads, st.ReadAheadUnused, off.reads, off.stats.KeyPagesDecoded)
		if st.KeyPagesDecoded != entries {
			t.Errorf("limit %d: decoded %d key pages for %d folders, want one each", limit, st.KeyPagesDecoded, entries)
		}
		// A page limit below the folders leaves the pages predicted past
		// the last entry unread: at most the entries of one prediction.
		if st.ReadAheadReads > 2*entries || st.ReadAheadReads >= keyPages/4 {
			t.Errorf("limit %d: read %d key pages ahead for %d folders (the listed prefix has %d)", limit, st.ReadAheadReads, entries, keyPages)
		}
		if limit >= folders && st.ReadAheadUnused != 0 {
			t.Errorf("limit %d: %d of %d pages read ahead were not used", limit, st.ReadAheadUnused, st.ReadAheadReads)
		}
		// Without speculation a part's end is read in 64 KiB, not 2 MiB:
		// compare the reads of key pages, which is what the read-ahead adds.
		if on.reads > off.reads {
			t.Errorf("limit %d: %d reads, more than the %d without speculation", limit, on.reads, off.reads)
		}
	}

	// With a wait per read, the page reads are made together, not one
	// after another: many are in flight at once.
	req := seekparquet.ListQuery{Prefix: prefix, Delimiter: '/', Limit: 1000}
	on := measureList(t, dir, 2*time.Millisecond, req, seekparquet.OpenOptions{})
	if on.maxInFlight < 16 {
		t.Errorf("the listing of %d folders had at most %d reads in flight: the page reads are not made together", folders, on.maxInFlight)
	}
}

// sameRows requires the row spans of got to be want's: each boundary is
// established by both listings or by neither, and is the same row.
func sameRows(t *testing.T, what string, got, want []seekparquet.ListEntry) {
	t.Helper()
	for i := range want {
		for _, b := range []struct {
			name      string
			got, want seekparquet.RowPosition
		}{{"start", got[i].Rows.Start, want[i].Rows.Start}, {"end", got[i].Rows.End, want[i].Rows.End}} {
			g, gok := b.got.Ordinal()
			w, wok := b.want.Ordinal()
			if gok != wok || gok && g != w {
				t.Fatalf("%s: entry %d %q rows %s is %d (known %v), want %d (known %v)", what, i, want[i].Bytes(), b.name, g, gok, w, wok)
			}
		}
	}
}

// TestListReadAheadWideFoldersResults lists, over a source that is not
// local, folders smaller than a row group, folders that span more row
// groups than a prediction enters, and repeated keys, whole, clipped by an
// upper bound, and page by page from cursors, with the page indexes in the
// part's retained metadata and with a metadata read too short to hold them
// (the read-ahead then reads indexes ahead): the entries and their row
// spans are those of a local listing, which reads nothing ahead and routes
// by first keys.
func TestListReadAheadWideFoldersResults(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	ctx := context.Background()
	prefix := []byte("crawl-data/CC-MAIN-2024-18/segments/")
	for _, fx := range []struct {
		name                         string
		folders, perFolder, pageRows int
		groupRows                    int64
		repeat                       int
	}{
		{"folders within row groups", 60, 2000, 50, 9000, 1},
		{"folders across many row groups", 5, 30000, 50, 2000, 1},
		{"repeated keys", 30, 1000, 50, 4000, 3},
	} {
		t.Run(fx.name, func(t *testing.T) {
			dir := wideFolderDataset(t, fx.folders, fx.perFolder, fx.pageRows, fx.groupRows, fx.repeat)
			local, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			end := (&folderRows{folders: fx.folders, perFolder: fx.perFolder}).key(fx.folders * fx.perFolder * 2 / 3)
			for _, opt := range []struct {
				name string
				opt  seekparquet.OpenOptions
			}{
				{"retained indexes", seekparquet.OpenOptions{}},
				{"indexes read ahead", seekparquet.OpenOptions{Read: seekparquet.ReadPolicy{MetadataPrefetchBytes: 4096}}},
				{"no speculation", seekparquet.OpenOptions{Read: seekparquet.ReadPolicy{DisableSpeculation: true}}},
			} {
				remote, err := seekparquet.OpenDataset(ctx, newSlowSource(t, dir, 0), opt.opt)
				if err != nil {
					t.Fatal(err)
				}
				var total seekparquet.ReadStats
				for _, q := range []struct {
					name string
					req  seekparquet.ListQuery
				}{
					{"whole", seekparquet.ListQuery{Prefix: prefix, Delimiter: '/', Limit: 1000}},
					{"clipped", seekparquet.ListQuery{Prefix: prefix, Delimiter: '/', Limit: 1000, End: end}},
					{"one level down", seekparquet.ListQuery{Prefix: end[:len(prefix)+len("1712296817000.00/")], Delimiter: '/', Limit: 1000}},
					{"paged", seekparquet.ListQuery{Prefix: prefix, Delimiter: '/', Limit: 7}},
					{"no roll-up, paged", seekparquet.ListQuery{Prefix: prefix, Delimiter: 0x01, Limit: 997, End: end[:len(prefix)+len("1712296817000.00/")]}},
				} {
					what := opt.name + ", " + q.name
					got, want := q.req, q.req
					for page := 0; ; page++ {
						var st seekparquet.ReadStats
						g, err := remote.List(ctx, got, &st)
						if err != nil {
							t.Fatalf("%s: %v", what, err)
						}
						total.Add(st)
						w, err := local.List(ctx, want, nil)
						if err != nil {
							t.Fatal(err)
						}
						sameEntries(t, what, g, w)
						sameRows(t, what, g, w)
						if len(w) < q.req.Limit || page == 40 {
							if page == 0 && len(w) == 0 {
								t.Fatalf("%s: no entries", what)
							}
							break
						}
						got.Cursor, want.Cursor = g[len(g)-1].Cursor(), w[len(w)-1].Cursor()
					}
				}
				if err := remote.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("%s: %d reads, %d key pages decoded, %d read ahead (%d unused), %d index loads", opt.name, total.SourceReads, total.KeyPagesDecoded, total.ReadAheadReads, total.ReadAheadUnused, total.IndexLoads)
				if opt.name == "indexes read ahead" && total.ReadAheadReads == 0 {
					t.Errorf("%s: nothing was read ahead", opt.name)
				}
			}
		})
	}
}
