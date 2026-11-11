// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// The batch oracle's record with its key PLAIN, dictionary-encoded or
// DELTA_BYTE_ARRAY-encoded (the test writer encodes batchOracleRecord's
// DELTA_LENGTH_BYTE_ARRAY). Struct tags do not take part in conversions, so
// each converts from batchOracleRecord.
type (
	keyPlainRecord struct {
		Key   []byte  `parquet:"key,plain"`
		Dict  *string `parquet:"dict,optional,dict"`
		Plain string  `parquet:"plain,delta"`
		Num   int64   `parquet:"num"`
		Opt   *string `parquet:"opt,optional"`
	}
	keyDictRecord struct {
		Key   []byte  `parquet:"key,dict"`
		Dict  *string `parquet:"dict,optional,dict"`
		Plain string  `parquet:"plain,delta"`
		Num   int64   `parquet:"num"`
		Opt   *string `parquet:"opt,optional"`
	}
	keyDeltaRecord struct {
		Key   []byte  `parquet:"key,delta"`
		Dict  *string `parquet:"dict,optional,dict"`
		Plain string  `parquet:"plain,delta"`
		Num   int64   `parquet:"num"`
		Opt   *string `parquet:"opt,optional"`
	}
)

// keyReuseFixture is a batch oracle dataset whose keys are long and share a
// prefix: batchOracleKey(k) followed by a suffix of 40 to 120 bytes, so that
// batchOracleRanges' bounds select whole groups. Key groups repeat up to 429
// rows (crossing key pages, row groups and, with !unique, never parts), the
// keys are encoded as enc says ("plain", "dict", which falls back to PLAIN
// when the dictionary fills, "delta" or "length"), and pages are v1 or v2 by
// seed.
func keyReuseFixture(t *testing.T, seed int64, enc string, unique, indexed bool) (string, []batchOracleRow, int, []int) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	var rows []batchOracleRow
	var groupStarts []int
	for k := 0; len(rows) < 3000; k++ {
		n := 1
		if !unique {
			n = 1 + r.Intn(5)
			if r.Intn(6) == 0 {
				n = 30 + r.Intn(400)
			}
		}
		groupStarts = append(groupStarts, len(rows))
		key := fmt.Appendf(batchOracleKey(k), "/crawl-data/segments/%s/%d.warc.gz", strings.Repeat("s", r.Intn(80)), k%7)
		if enc == "dict" && k%3 != 0 {
			key = fmt.Appendf(batchOracleKey(k), "/d") // short keys, many sharing a page's dictionary
		}
		for o := range n {
			i := len(rows)
			rec := batchOracleRecord{Key: key, Plain: fmt.Sprintf("p-%d", i), Num: int64(i * 3)}
			if r.Intn(5) != 0 {
				v := fmt.Sprintf("d-%d", r.Intn(8))
				rec.Dict = &v
			}
			if r.Intn(3) == 0 {
				v := fmt.Sprintf("o-%d", i)
				rec.Opt = &v
			}
			rows = append(rows, batchOracleRow{rec: rec, offset: int64(o), ord: i})
		}
	}
	cut1, cut2 := groupStarts[len(groupStarts)/3], groupStarts[2*len(groupStarts)/3]
	spec := testutil.Dataset{RepeatedKeys: !unique}
	dictBytes := int64(4096)
	if enc == "dict" {
		dictBytes = 160 // small enough that key dictionaries fill
	}
	for _, b := range [][2]int{{0, cut1}, {cut1, cut2}, {cut2, len(rows)}} {
		var recs any
		switch enc {
		case "plain":
			s := make([]keyPlainRecord, 0, b[1]-b[0])
			for _, row := range rows[b[0]:b[1]] {
				s = append(s, keyPlainRecord(row.rec))
			}
			recs = s
		case "length":
			s := make([]batchOracleRecord, 0, b[1]-b[0])
			for _, row := range rows[b[0]:b[1]] {
				s = append(s, row.rec)
			}
			recs = s
		case "dict":
			s := make([]keyDictRecord, 0, b[1]-b[0])
			for _, row := range rows[b[0]:b[1]] {
				s = append(s, keyDictRecord(row.rec))
			}
			recs = s
		case "delta":
			s := make([]keyDeltaRecord, 0, b[1]-b[0])
			for _, row := range rows[b[0]:b[1]] {
				s = append(s, keyDeltaRecord(row.rec))
			}
			recs = s
		}
		spec.Parts = append(spec.Parts, testutil.Part{Rows: recs, GroupRows: 250 + r.Intn(200), PageBytes: 600 + r.Intn(1200), DictionaryMaxBytes: dictBytes, Compression: parquet.Compression(&zstd.Codec{}), DataPageVersion: 1 + int(seed%2)})
	}
	dir := write(t, spec)
	checkKeyEncoding(t, dir, enc)
	cuts := batchBoundaries(t, dir, len(spec.Parts))
	if !indexed {
		for p := range len(spec.Parts) {
			rewrite(t, dir, p, func(meta *format.FileMetaData) {
				for g := range meta.RowGroups {
					for c := 1; c < len(meta.RowGroups[g].Columns); c++ {
						col := &meta.RowGroups[g].Columns[c]
						col.OffsetIndexOffset, col.OffsetIndexLength = 0, 0
					}
				}
			})
		}
	}
	return dir, rows, len(groupStarts), cuts
}

// checkKeyEncoding requires the key chunks to use the encoding the fixture
// asked for, so that each case exercises its decoder.
func checkKeyEncoding(t *testing.T, dir, enc string) {
	t.Helper()
	want := map[string]format.Encoding{"plain": format.Plain, "dict": format.RLEDictionary, "delta": format.DeltaByteArray, "length": format.DeltaLengthByteArray}[enc]
	footer, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	for _, e := range footer.RowGroups[0].Columns[0].MetaData.Encoding {
		if e == want || enc == "dict" && e == format.PlainDictionary {
			return
		}
	}
	t.Fatalf("key chunk encodings %v, want %s", footer.RowGroups[0].Columns[0].MetaData.Encoding, want)
}

// keyDataPageEncodings counts the encodings of every key data page, from
// the page headers the OffsetIndexes locate.
func keyDataPageEncodings(t *testing.T, d *Dataset) map[format.Encoding]int {
	t.Helper()
	out := map[format.Encoding]int{}
	st := new(ReadStats)
	for i := range d.r.parts {
		p, err := d.r.part(ctx, i, st)
		must(t, err)
		for j := range p.groups {
			log := pageLog{unique: d.r.key.Unique}
			k, err := d.r.keyPages(ctx, groupRef{i, j}, &log, st)
			must(t, err)
			for pg, meta := range k.index.pages {
				h, _, err := k.chunk.indexedPage(pg, meta.pageLoc)
				must(t, err)
				if h.Type == format.DataPageV2 {
					out[h.DataPageHeaderV2.V.Encoding]++
				} else {
					out[h.DataPageHeader.V.Encoding]++
				}
			}
		}
	}
	return out
}

// batchTrace is everything a batch walk handed over, copied: per batch its
// length, then each row's key and offset, and the read statistics.
type batchTrace struct {
	lens  []int
	keys  [][]byte
	offs  []int64
	rows  []Row
	stats ReadStats
}

func traceBatches(t *testing.T, scan func(fn func(*Batch) error) (ReadStats, error)) batchTrace {
	t.Helper()
	var tr batchTrace
	st, err := scan(func(b *Batch) error {
		tr.lens = append(tr.lens, b.Len())
		for i := range b.Len() {
			tr.keys = append(tr.keys, bytes.Clone(b.Key(i)))
			tr.offs = append(tr.offs, b.Offset(i))
			tr.rows = append(tr.rows, b.Row(i))
		}
		return nil
	})
	must(t, err)
	tr.stats = st
	return tr
}

// TestBatchKeyReuseEquivalence compares the batch walker decoding every key
// page into one reused scratch with the same walker decoding each into
// storage of its own (as before key reuse, and as every other read does):
// the same batches, keys, offsets, rows and read statistics, for whole,
// ranged, cursored, limited and split scans; and both against the oracle.
func TestBatchKeyReuseEquivalence(t *testing.T) {
	cases := []struct {
		enc     string
		seed    int64
		unique  bool
		indexed bool
	}{
		{"plain", 1, false, true}, {"plain", 2, true, false},
		{"dict", 3, false, true}, {"dict", 4, false, false}, {"dict", 5, true, true},
		{"delta", 6, false, false}, {"delta", 7, false, true}, {"delta", 8, true, true},
		{"length", 9, false, true}, {"length", 10, true, false},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/seed=%d/unique=%t/indexed=%t", c.enc, c.seed, c.unique, c.indexed), func(t *testing.T) {
			dir, rows, nkeys, cuts := keyReuseFixture(t, c.seed, c.enc, c.unique, c.indexed)
			d := open(t, dir, OpenOptions{})
			if c.enc == "dict" {
				// The dictionary fills: chunks hold dictionary-encoded key pages,
				// then PLAIN ones, so reuse crosses that switch.
				encs := keyDataPageEncodings(t, d)
				if encs[format.RLEDictionary]+encs[format.PlainDictionary] == 0 || encs[format.Plain] == 0 {
					t.Fatalf("key data page encodings %v: no dictionary-to-PLAIN fallback", encs)
				}
			}
			r := rand.New(rand.NewSource(c.seed * 11))
			// Each comparison runs warm: a first scan has loaded and kept the
			// key page indexes and OffsetIndexes both then reuse.
			compare := func(label string, want []batchOracleRow, scan func(kr keyReuse, fn func(*Batch) error) (ReadStats, error)) {
				t.Helper()
				traceBatches(t, func(fn func(*Batch) error) (ReadStats, error) { return scan(keyReuse{}, fn) })
				on := traceBatches(t, func(fn func(*Batch) error) (ReadStats, error) { return scan(keyReuse{}, fn) })
				off := traceBatches(t, func(fn func(*Batch) error) (ReadStats, error) { return scan(keyReuse{off: true}, fn) })
				if !reflect.DeepEqual(on, off) {
					t.Fatalf("%s: reusing key pages differs: lens %v / %v, stats %+v / %+v", label, on.lens, off.lens, on.stats, off.stats)
				}
				if want != nil {
					checkBatchOracleRows(t, label, on.rows, want)
				}
			}
			for trial := range 6 {
				ranges := batchOracleRanges(r, nkeys, rows)
				if trial == 0 {
					ranges = []Selection{{}}
				}
				want := batchOracleExpected(ranges, rows)
				plan, err := d.Plan(ctx, ranges, batchOracleColumns, nil)
				must(t, err)
				compare(fmt.Sprintf("plan %d %v", trial, ranges), want, func(kr keyReuse, fn func(*Batch) error) (ReadStats, error) {
					var st ReadStats
					err := plan.scanBatches(ctx, &st, kr, fn)
					return st, err
				})
				shards, err := plan.Split(ctx, 1+r.Intn(5), nil)
				must(t, err)
				for i, sh := range shards {
					compare(fmt.Sprintf("plan %d shard %d", trial, i), nil, func(kr keyReuse, fn func(*Batch) error) (ReadStats, error) {
						var st ReadStats
						err := sh.scanBatches(ctx, &st, kr, fn)
						return st, err
					})
				}
				checkBatchOracleBatches(t, plan, shards, want, cuts)
				rng := Query{Selection: ranges[0]}
				rng.Columns, rng.Limit = batchOracleColumns, int64(1+r.Intn(len(rows)))
				if trial == 0 {
					rng.Limit = int64(len(rows))
				}
				compare(fmt.Sprintf("range %d %+v", trial, rng), nil, func(kr keyReuse, fn func(*Batch) error) (ReadStats, error) {
					var st ReadStats
					err := d.r.rangeBatches(ctx, rng, &st, kr, fn)
					return st, err
				})
				// Keys alone: batches are cut by key pages, groups and parts only.
				rng.Columns = nil
				compare(fmt.Sprintf("range keys %d", trial), nil, func(kr keyReuse, fn func(*Batch) error) (ReadStats, error) {
					var st ReadStats
					err := d.r.rangeBatches(ctx, rng, &st, kr, fn)
					return st, err
				})
			}
		})
	}
}

// overlaps reports whether b's bytes lie in buf's backing array.
func overlaps(b, buf []byte) bool {
	if len(b) == 0 || cap(buf) == 0 {
		return false
	}
	p, q := uintptr(unsafe.Pointer(unsafe.SliceData(b))), uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	return p < q+uintptr(cap(buf)) && q < p+uintptr(len(b))
}

// TestBatchKeyReusePoisonRetired poisons the key scratch each time the batch
// walker is about to overwrite it, after the scan kept what it needs of the
// page: no key the scan, its page log or the reader's first-key cache still
// uses may alias it, and every owning row and cursor taken in a callback, and
// every later read of the same Dataset, must read the oracle's keys and
// offsets.
func TestBatchKeyReusePoisonRetired(t *testing.T) {
	for _, enc := range []string{"plain", "dict", "delta", "length"} {
		t.Run(enc, func(t *testing.T) {
			dir, rows, nkeys, _ := keyReuseFixture(t, 21, enc, false, true)
			d := open(t, dir, OpenOptions{})
			retired := 0
			retire := func(s *scan) {
				ks := s.keyScratch
				bufs := [][]byte{ks.raw, ks.plain, ks.data}
				for _, buf := range bufs {
					if overlaps(s.groupKey, buf) {
						t.Fatal("the group key aliases a retired key page")
					}
					for _, sp := range s.log.spans {
						if overlaps(sp.first, buf) || overlaps(sp.last, buf) {
							t.Fatal("the page log aliases a retired key page")
						}
					}
					for _, p := range d.r.parts {
						for i := range p.groups {
							if first := p.groups[i].first.Load(); first != nil && overlaps(*first, buf) {
								t.Fatal("a cached first key aliases a retired key page")
							}
						}
					}
					poisonBytes(buf[:cap(buf)])
				}
				for i := range ks.offsets[:cap(ks.offsets)] {
					ks.offsets[:cap(ks.offsets)][i] = 0xA5A5A5A5
				}
				for i := range ks.indexes[:cap(ks.indexes)] {
					ks.indexes[:cap(ks.indexes)][i] = -1
				}
				retired++
			}
			r := rand.New(rand.NewSource(5))
			for trial := range 8 {
				if trial == 4 {
					// The rest run with the list index built: routing then
					// takes row groups' first keys from it.
					stats, err := d.BuildListIndex(ctx, ListIndexOptions{})
					must(t, err)
					if !stats.HasIndex {
						t.Fatalf("no list index: %+v", stats)
					}
				}
				ranges := batchOracleRanges(r, nkeys, rows)
				if trial == 0 {
					ranges = []Selection{{}}
				}
				want := batchOracleExpected(ranges, rows)
				plan, err := d.Plan(ctx, ranges, batchOracleColumns, nil)
				must(t, err)
				var kept []Row
				var cursors []*Cursor
				shards, err := plan.Split(ctx, 1+r.Intn(4), nil)
				must(t, err)
				for _, sh := range shards {
					must(t, sh.scanBatches(ctx, nil, keyReuse{retire: retire}, func(b *Batch) error {
						for i := range b.Len() {
							kept = append(kept, b.Row(i))
							cursors = append(cursors, b.Cursor(i))
						}
						return nil
					}))
				}
				rng := Query{Selection: ranges[0], Columns: batchOracleColumns, Limit: int64(len(rows))}
				var ranged []Row
				must(t, d.r.rangeBatches(ctx, rng, nil, keyReuse{retire: retire}, func(b *Batch) error {
					for i := range b.Len() {
						ranged = append(ranged, b.Row(i))
					}
					return nil
				}))
				checkBatchOracleRows(t, fmt.Sprintf("poisoned plan %d", trial), kept, want)
				checkBatchOracleRows(t, fmt.Sprintf("poisoned range %d", trial), ranged, batchOracleExpected(ranges[:1], rows))
				for i, c := range cursors {
					if !bytes.Equal(c.Key, want[i].rec.Key) || c.Offset != want[i].offset {
						t.Fatal("poisoned cursor", i)
					}
				}
				// The first-key cache and part checks the scans left serve
				// ordinary reads.
				got, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: ranges[0].Start, End: ranges[0].End, StartExclusive: ranges[0].StartExclusive}, Cursor: ranges[0].Cursor}, Limit: int64(len(rows)), Columns: batchOracleColumns}, nil)
				must(t, err)
				checkBatchOracleRows(t, fmt.Sprintf("Query after %d", trial), got, batchOracleExpected(ranges[:1], rows))
			}
			if retired < 50 {
				t.Fatalf("only %d key pages retired", retired)
			}
		})
	}
}

// TestScanBatchesKeyPageAllocations bounds the whole borrowed scan's
// allocations over a keys-only plan of long keys, where the key column is
// all that is decoded: with one reused key scratch per scan, what remains
// is per page (headers, the page log) and per row group, not per key byte.
// It also measures the same walk decoding each key page into storage of its
// own, as before key reuse.
func TestScanBatchesKeyPageAllocations(t *testing.T) {
	const n = 40_000
	records := make([]reuseRecord, n)
	for i := range records {
		records[i].Key = fmt.Appendf(nil, "crawl-data/CC-MAIN-2024-10/segments/1707947470000.%02d/warc/CC-MAIN-20240220211055-%014d-%05d.warc.gz", i/4000, 20240221001055+i, i)
	}
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: records[:n/2], GroupRows: 10_000, PageBytes: 16 << 10, Compression: parquet.Compression(&zstd.Codec{})},
		{Rows: records[n/2:], GroupRows: 10_000, PageBytes: 16 << 10, Compression: parquet.Compression(&zstd.Codec{})},
	}}), OpenOptions{})
	plan, err := d.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	count := 0
	emit := func(b *Batch) error {
		for i := range b.Len() {
			count += len(b.Key(i)) & 1
		}
		count += b.Len()
		return nil
	}
	var st ReadStats
	must(t, plan.Scan(ctx, &st, emit))
	if st.KeyPagesDecoded < 200 {
		t.Fatalf("fixture geometry: %+v", st)
	}
	measure := func(kr keyReuse) (allocations, bytes float64) {
		const runs = 3
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range runs {
			must(t, plan.scanBatches(ctx, nil, kr, emit))
		}
		runtime.ReadMemStats(&after)
		return float64(after.Mallocs-before.Mallocs) / float64(runs*n), float64(after.TotalAlloc-before.TotalAlloc) / float64(runs*n)
	}
	allocations, bytes := measure(keyReuse{})
	ownAllocations, ownBytes := measure(keyReuse{off: true})
	t.Logf("keys-only ScanBatches over %d key pages: %.4f allocations/row, %.2f bytes/row (each page its own storage: %.4f, %.2f)", st.KeyPagesDecoded, allocations, bytes, ownAllocations, ownBytes)
	// Measured (Go 1.27.4, linux/amd64): 0.043 allocations and 10.5 bytes per
	// row, 0.071 and 82 under -race, which discards pool entries; each page
	// its own storage, about 145 bytes more in both.
	if allocations > 0.1 || bytes > 120 || ownBytes-bytes < 100 {
		t.Fatalf("key pages lost buffer reuse: %.4f allocations/row, %.2f bytes/row (ceilings 0.1, 120; at least 100 below %.2f)", allocations, bytes, ownBytes)
	}
}

// TestBatchKeyReuseRefusalKey requires the key a key_order refusal carries,
// found by the batch walker in a reused key page (in the page itself, or
// against the page before it), to be the refusal's own: it does not alias
// the key scratch, and poisoning the scratch after the call leaves it intact.
func TestBatchKeyReuseRefusalKey(t *testing.T) {
	keys := func(mutate func(k []string) []string) []string {
		k := make([]string, 300)
		for i := range k {
			k[i] = fmt.Sprintf("k%04d", i)
		}
		return mutate(k)
	}
	for _, c := range []struct {
		name string
		keys []string
	}{
		{"within a page", keys(func(k []string) []string { k[150], k[151] = k[151], k[150]; return k })},
		{"blocks", keys(func(k []string) []string {
			return append(append(append(append([]string{}, k[:100]...), k[150:200]...), k[100:150]...), k[200:]...)
		})},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(c.keys...), PageBytes: 256}}}), OpenOptions{})
			plan, err := d.Plan(ctx, []Selection{{}}, nil, nil)
			must(t, err)
			var ks *pageScratch
			err = plan.scanBatches(ctx, nil, keyReuse{retire: func(s *scan) { ks = s.keyScratch }}, func(*Batch) error { return nil })
			var refusal *RefusalError
			if !errors.As(err, &refusal) || refusal.Code != CodeKeyOrder || ks == nil {
				t.Fatalf("got %v", err)
			}
			want := bytes.Clone(refusal.Key)
			for _, buf := range [][]byte{ks.raw, ks.plain, ks.data} {
				if overlaps(refusal.Key, buf) {
					t.Fatal("the refusal's key aliases the key scratch")
				}
				poisonBytes(buf[:cap(buf)])
			}
			if !bytes.Equal(refusal.Key, want) || len(want) == 0 {
				t.Fatalf("refusal key %q changed to %q", want, refusal.Key)
			}
		})
	}
}
