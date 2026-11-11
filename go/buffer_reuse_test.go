// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

func reuseCodecs() []compress.Codec {
	return []compress.Codec{&uncompressed.Codec{}, &snappy.Codec{}, &gzip.Codec{}, &brotli.Codec{Quality: 1, LGWin: 16}, &zstd.Codec{}, &lz4.Codec{}}
}

type transitionRecord struct {
	Key    []byte  `parquet:"key"`
	Dict   *string `parquet:"dict,optional,dict"`
	Plain  string  `parquet:"plain,delta"`
	Null   *string `parquet:"null,optional"`
	Number int64   `parquet:"number,delta"`
}

func transitionRecords(n int, fallback bool) []transitionRecord {
	rows := make([]transitionRecord, n)
	for i := range rows {
		rows[i] = transitionRecord{Key: fmt.Appendf(nil, "k%05d", i), Plain: fmt.Sprintf("plain-value-%08d", i), Number: int64(i)}
		if i%7 != 0 {
			value := fmt.Sprintf("dictionary-%d-%d", i/512, i%4)
			if fallback && i%512 >= 128 {
				value = fmt.Sprintf("unique-dictionary-value-%08d", i)
			}
			rows[i].Dict = &value
		}
	}
	return rows
}

var transitionColumns = []string{"dict", "plain", "null", "number", "dict", "key", "missing"}

func checkTransitionRows(t *testing.T, got []Row, want []transitionRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, row := range got {
		w := want[i]
		if !bytes.Equal(row.Key, w.Key) || row.Offset != 0 || string(row.Values[1].Bytes()) != w.Plain || !row.Values[2].IsNull() || row.Values[3].Int64() != w.Number || !bytes.Equal(row.Values[5].Bytes(), w.Key) || !row.Values[6].Absent() {
			t.Fatalf("row %d: %+v", i, row)
		}
		for _, c := range []int{0, 4} {
			v := row.Values[c]
			if w.Dict == nil {
				if !v.IsNull() {
					t.Fatalf("row %d: expected null", i)
				}
			} else if v.IsNull() || string(v.Bytes()) != *w.Dict {
				t.Fatalf("row %d: dictionary %v, want %q", i, v, *w.Dict)
			}
		}
	}
}

// Every request retains its results across page, dictionary, group and part
// transitions; independent source rows are the oracle. Duplicate projections,
// projected keys and absent columns also exercise distinct scratch slots.
func TestBufferReuseTransitions(t *testing.T) {
	for _, codec := range reuseCodecs() {
		for _, version := range []int{1, 2} {
			for _, indexed := range []bool{false, true} {
				for _, fallback := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/v%d/index=%t/fallback=%t", codec, version, indexed, fallback), func(t *testing.T) {
						records := transitionRecords(2048, fallback)
						spec := testutil.Dataset{}
						for start := 0; start < len(records); start += 1024 {
							spec.Parts = append(spec.Parts, testutil.Part{Rows: records[start : start+1024], GroupRows: 512, PageBytes: 128, DataPageVersion: version, Compression: parquet.Compression(codec), DictionaryMaxBytes: 1024})
						}
						dir := write(t, spec)
						// Check actual page encodings, not merely the writer configuration.
						d := open(t, dir, OpenOptions{})
						must(t, d.r.checkParts(ctx, new(ReadStats)))
						cp, err := d.r.parts[0].openColumn(ctx, 0, "dict", new(ReadStats), nil)
						must(t, err)
						var dictPages, plainPages int
						for _, loc := range cp.locs {
							h, _, _, err := cp.c.header(loc.offset)
							must(t, err)
							enc := h.DataPageHeader.V.Encoding
							if h.DataPageHeaderV2.Valid {
								enc = h.DataPageHeaderV2.V.Encoding
							}
							if enc == format.RLEDictionary || enc == format.PlainDictionary {
								dictPages++
							} else if enc == format.Plain {
								plainPages++
							}
						}
						if dictPages < 2 || fallback && plainPages == 0 || !fallback && plainPages != 0 {
							t.Fatalf("dictionary=%d plain=%d", dictPages, plainPages)
						}
						must(t, d.Close())
						if !indexed {
							for p := range spec.Parts {
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
						d = open(t, dir, OpenOptions{})
						rows, err := d.Rows(ctx, Query{Limit: int64(len(records)), Columns: transitionColumns}, nil)
						must(t, err)
						entries, err := d.List(ctx, ListQuery{Limit: len(records), Columns: transitionColumns}, nil)
						must(t, err)
						listed := make([]Row, len(entries))
						for i, e := range entries {
							listed[i] = *e.Row
						}
						plan, err := d.Plan(ctx, []Selection{{}}, transitionColumns, nil)
						must(t, err)
						var scanned []Row
						must(t, scanRows(ctx, plan, nil, func(r Row) error { scanned = append(scanned, r); return nil }))
						checkTransitionRows(t, rows, records)
						checkTransitionRows(t, listed, records)
						checkTransitionRows(t, scanned, records)
					})
				}
			}
		}
	}
}

// Poison only when a complete page has been consumed, and a dictionary only
// when its chunk retires. The production owning boundary is projectColumn,
// shared by Query and List. Scan uses Batch.Row, tested separately.
// No global test hook changes concurrent scans.
func TestBufferReusePoisonRetiredPages(t *testing.T) {
	records := transitionRecords(1024, true)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 128, DictionaryMaxBytes: 1024, Compression: parquet.Compression(&zstd.Codec{})}}})
	d := open(t, dir, OpenOptions{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	for _, name := range []string{"dict", "plain"} {
		var scratch pageScratch
		var owning []*Row
		var cursors []*Cursor
		var sizes []int
		for g := range 2 {
			cp, err := d.r.parts[0].openColumn(ctx, g, name, new(ReadStats), &scratch)
			must(t, err)
			for _, loc := range cp.locs {
				rows := make([]*Row, int(loc.rows))
				at := make([]position, len(rows))
				for i := range rows {
					index := g*512 + int(loc.first) + i
					rows[i] = &Row{Key: bytes.Clone(records[index].Key), Values: make([]Value, 1)}
					at[i] = position{g: groupRef{0, g}, row: loc.first + int64(i)}
					cursors = append(cursors, rows[i].Cursor())
				}
				must(t, projectColumn(rows, at, 0, cp))
				owning = append(owning, rows...)
				poisonScratch(&scratch)
				// No row in this retired page will be read again.
				cp.values = nil
			}
			if cp.c.dict != nil {
				for i := 0; i < cp.c.dict.Len(); i++ {
					poisonBytes(cp.c.dict.Index(int32(i)).ByteArray())
				}
			}
			sizes = append(sizes, cap(scratch.raw)+cap(scratch.plain)+cap(scratch.data)+cap(scratch.levels)+4*cap(scratch.indexes)+4*cap(scratch.offsets)+24*(cap(scratch.values)+cap(scratch.out)))
		}
		for i, r := range owning {
			want := records[i]
			if !bytes.Equal(r.Key, want.Key) || !bytes.Equal(cursors[i].Key, want.Key) {
				t.Fatalf("key/cursor %d corrupted", i)
			}
			if name == "dict" {
				if want.Dict == nil {
					if !r.Values[0].IsNull() {
						t.Fatal(i)
					}
				} else if string(r.Values[0].Bytes()) != *want.Dict {
					t.Fatalf("dictionary row %d corrupted", i)
				}
			} else if string(r.Values[0].Bytes()) != want.Plain {
				t.Fatalf("plain row %d corrupted", i)
			}
		}
		t.Logf("%s retained buffer capacity after each chunk: %v bytes (excludes codec state/dictionary)", name, sizes)
	}
}

func poisonBytes(b []byte) {
	for i := range b {
		b[i] = 0xa5
	}
}
func poisonScratch(s *pageScratch) {
	for _, b := range [][]byte{s.raw, s.plain, s.data, s.levels} {
		poisonBytes(b[:cap(b)])
	}
	for i := range s.indexes {
		s.indexes[i] = -1
	}
	for i := range s.offsets {
		s.offsets[i] = 0xdeadbeef
	}
	for i := range s.values {
		s.values[i] = parquet.Int64Value(-1)
	}
	for i := range s.out {
		s.out[i] = parquet.Int64Value(-1)
	}
}

func TestBufferReuseCodecBounds(t *testing.T) {
	large := bytes.Repeat([]byte("abcdefgh"), 8192)
	for _, codec := range reuseCodecs() {
		t.Run(codec.String(), func(t *testing.T) {
			var scratch pageScratch
			encoded, err := codec.Encode(nil, large)
			must(t, err)
			out, err := scratch.decompress(codec.CompressionCodec(), encoded, len(large))
			must(t, err)
			if !bytes.Equal(out, large) {
				t.Fatal("large output")
			}
			gzipState, brotliState := scratch.codec.gzip, scratch.codec.brotli
			for _, n := range []int{256, 8, 0, 128} {
				small, err := codec.Encode(nil, large[:n])
				must(t, err)
				out, err = scratch.decompress(codec.CompressionCodec(), small, n)
				must(t, err)
				if !bytes.Equal(out, large[:n]) {
					t.Fatal("small output")
				}
				if cap(scratch.plain) > 0 {
					backing := scratch.plain[:cap(scratch.plain)]
					poisonBytes(backing)
					if _, err := scratch.decompress(codec.CompressionCodec(), encoded, n); err == nil {
						t.Fatal("surplus accepted")
					}
					if !bytes.Equal(backing[n:], bytes.Repeat([]byte{0xa5}, len(backing)-n)) {
						t.Fatal("codec wrote beyond this page's declared size")
					}
				}
				if _, err := scratch.decompress(codec.CompressionCodec(), small, n+1); err == nil {
					t.Fatal("short output accepted")
				}
			}
			if scratch.codec.gzip != gzipState || scratch.codec.brotli != brotliState {
				t.Fatal("reader state replaced")
			}
		})
	}
}

func TestBufferReuseConcurrentScans(t *testing.T) {
	records := transitionRecords(2048, true)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 128, DictionaryMaxBytes: 1024, Compression: parquet.Compression(&zstd.Codec{})}}})
	d := open(t, dir, OpenOptions{})
	plan, err := d.Plan(ctx, []Selection{{}}, transitionColumns, nil)
	must(t, err)
	shards, err := plan.Split(ctx, 2, nil)
	must(t, err)
	plans := append([]*Plan{plan, plan}, shards...)
	results := make([][]Row, len(plans))
	errs := make([]error, len(plans))
	var wg sync.WaitGroup
	for i, p := range plans {
		wg.Go(func() {
			errs[i] = scanRows(ctx, p, nil, func(r Row) error { results[i] = append(results[i], r); return nil })
		})
	}
	wg.Wait()
	for _, err := range errs {
		must(t, err)
	}
	checkTransitionRows(t, results[0], records)
	checkTransitionRows(t, results[1], records)
	checkTransitionRows(t, slices.Concat(results[2:]...), records)
	stop := errors.New("stop")
	if err := scanRows(ctx, plan, nil, func(Row) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	count := 0
	err = scanRows(cancelled, plan, nil, func(Row) error { count++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || count == 0 {
		t.Fatalf("cancelled: %d %v", count, err)
	}
	var again []Row
	must(t, scanRows(ctx, plan, nil, func(r Row) error { again = append(again, r); return nil }))
	checkTransitionRows(t, again, records)
}

func TestBufferReuseBooleanAndTails(t *testing.T) {
	c := testChunk(parquet.BooleanType)
	c.scratch = new(pageScratch)
	for _, bit := range []byte{1, 0, 1, 0} {
		got, err := c.values(format.RLE, booleanStream(34, bit), 17)
		must(t, err)
		for _, v := range got {
			if v.Boolean() != (bit == 1) {
				t.Fatal("stale Boolean bits")
			}
		}
	}
	c = testChunk(parquet.ByteArrayType)
	c.scratch = new(pageScratch)
	payload := plainByteArray("long", "old", "dictionary", "pointers")
	_, err := c.values(format.Plain, payload, 4)
	must(t, err)
	_, err = c.values(format.Plain, plainByteArray("x"), 1)
	must(t, err)
	for _, v := range c.scratch.values[1:cap(c.scratch.values)] {
		if !v.IsNull() {
			t.Fatal("stale pointer tail")
		}
	}
	_, err = c.values(format.Plain, nil, 0)
	must(t, err)
	if cap(c.scratch.offsets) < 5 || cap(c.scratch.data) < len(payload) {
		t.Fatal("shrinking page lost reusable capacity")
	}
}

// This serial decoder gate uses no shared codec initialization. Whole-request,
// multi-worker allocation evidence is in BenchmarkScanBufferReuse instead.
func TestBufferReusePageAllocations(t *testing.T) {
	payload := plainByteArray("alpha", "beta", "gamma", "delta")
	c := testChunk(parquet.ByteArrayType)
	cold := testing.AllocsPerRun(100, func() {
		_, err := c.values(format.Plain, payload, 4)
		if err != nil {
			panic(err)
		}
	})
	c.scratch = new(pageScratch)
	warm := testing.AllocsPerRun(100, func() {
		_, err := c.values(format.Plain, payload, 4)
		if err != nil {
			panic(err)
		}
	})
	t.Logf("plain BYTE_ARRAY decode: allocating %.0f, reused %.0f allocations/page", cold, warm)
	if warm > cold-3 {
		t.Fatalf("page buffers not reused: cold=%g warm=%g", cold, warm)
	}
}

// A large decoded payload makes an extra output allocation visible despite
// small allocations from parquet-go's pools (including under the race detector).
// Key pages need one new output buffer; warmed projected pages need none.
func TestDeltaByteArrayOutputAllocations(t *testing.T) {
	const n, width, pages = 128, 1024, 100
	data := bytes.Repeat([]byte("x"), n*width)
	offsets := make([]uint32, n+1)
	for i := range n {
		offsets[i] = uint32(i * width)
		data[(i+1)*width-1] = byte(i)
	}
	offsets[n] = uint32(len(data))
	payload, err := parquet.DeltaByteArray.EncodeByteArray(nil, data, offsets)
	must(t, err)
	for _, reuse := range []bool{false, true} {
		name := "key"
		if reuse {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			c := testChunk(parquet.ByteArrayType)
			if reuse {
				c.scratch = new(pageScratch)
			}
			// Headroom must not count toward the decoded PageBytes limit.
			c.p.r.limits.PageBytes = int64(len(data))
			check := func() {
				values, err := c.values(format.DeltaByteArray, payload, n)
				must(t, err)
				if len(values) != n {
					t.Fatalf("got %d values, want %d", len(values), n)
				}
				for i, v := range values {
					if !bytes.Equal(v.ByteArray(), data[i*width:(i+1)*width]) {
						t.Fatalf("value %d differs", i)
					}
				}
			}
			check() // warm the projected buffers before measuring repeated pages
			perPage := allocated(func() {
				for range pages {
					_, err := c.values(format.DeltaByteArray, payload, n)
					must(t, err)
				}
			}) / pages
			// Allow half a payload for allocator rounding, Values, offsets and
			// pooled delta lengths, but never another full output per page.
			budget := uint64(len(data) / 2)
			if !reuse {
				budget += uint64(len(data))
			}
			t.Logf("DELTA_BYTE_ARRAY: %d allocated bytes/page (budget %d)", perPage, budget)
			if perPage > budget {
				t.Fatalf("decoded output regrows per page: %d bytes/page > %d", perPage, budget)
			}
			check()
			c.p.r.limits.PageBytes--
			if _, err := c.values(format.DeltaByteArray, payload, n); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("decoded size above PageBytes: %v", err)
			}
		})
	}
}

func TestBufferReuseKeyPolicy(t *testing.T) {
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: transitionRecords(512, false), PageBytes: 128}}}), OpenOptions{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	var scratch pageScratch
	cp, err := d.r.parts[0].openColumn(ctx, 0, "key", new(ReadStats), &scratch)
	must(t, err)
	if cp.c.scratch != nil {
		t.Fatal("projected key uses scratch")
	}
	first, err := cp.c.indexed(0, cp.locs[0])
	must(t, err)
	key := bytes.Clone(first[0].ByteArray())
	_, err = cp.c.indexed(1, cp.locs[1])
	must(t, err)
	if !bytes.Equal(first[0].ByteArray(), key) {
		t.Fatal("retained key page overwritten")
	}
}

func TestBufferReuseSurplusDictionary(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: transitionRecords(512, false), PageBytes: 128}}})
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	off := meta.RowGroups[0].Columns[1].MetaData.DictionaryPageOffset
	if off == 0 {
		t.Fatal("missing dictionary")
	}
	editPageHeader(t, partFile(dir, 0), off, func(h *format.PageHeader) { h.DictionaryPageHeader.V.NumValues-- })
	d := open(t, dir, OpenOptions{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	var scratch pageScratch
	cp, err := d.r.parts[0].openColumn(ctx, 0, "dict", new(ReadStats), &scratch)
	must(t, err)
	_, err = cp.c.values(format.Plain, plainByteArray("a larger live data page"), 1)
	must(t, err)
	if _, err = cp.c.dictionary(); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("surplus dictionary: %v", err)
	}
}

// All-null numeric dictionary pages pass an empty output to parquet-go. Keep
// its base non-nil, as the original make([]parquet.Value, 0) did: numeric
// dictionaries compute field pointers even when there are no indexes.
func TestBufferReuseEmptyDictionary(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		c := testChunk(parquet.Int64Type)
		if reuse {
			c.scratch = new(pageScratch)
		}
		c.dict = parquet.Int64Type.NewDictionary(1, 1, parquet.Int64Type.NewValues(make([]byte, 8), nil))
		c.dictRead = true
		values, err := c.values(format.RLEDictionary, []byte{0}, 0)
		must(t, err)
		if len(values) != 0 || values == nil {
			t.Fatal("missing non-nil empty dictionary output")
		}
	}
}
