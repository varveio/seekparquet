// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package write

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/encoding/thrift"

	seekparquet "github.com/varveio/seekparquet/go"
)

// The parity test writes the parts of swath's TestFooterOnlyChange
// (swath go/internal/parquetout/footeronly_test.go, at a832c783) with
// swath's writer parameters, and compares them with the digests swath
// recorded: this writer must produce the bytes swath's did, so that swath
// can drop its own writer and keep its goldens.

// partDigests split a part's bytes as swath's test does: Data, every
// column chunk's pages; Meta, the footer without its key-value metadata,
// created_by and page index locations; Offsets, every OffsetIndex; Values,
// every ColumnIndex but the key column's; Keys, the key column's
// ColumnIndexes.
type partDigests struct{ Data, Meta, Offsets, Values, Keys string }

// swathGolden are swath's footerOnlyGolden. The long scenario's Keys are
// those of swath's current writer (exact bounds), recorded with File; the
// golden in swath's test predates them.
var swathGolden = map[string][]partDigests{
	"unique": {
		{Data: "2626b20b48e07c6363f58dae1c974e0d82619aa63834c42467be875eb40bcaea", Meta: "b2831a92f707b173044531061cf4092786ca4709bc79dc9e65c7695519906649", Offsets: "1f6bada2ee2378ea8b24bd445ecf75e188617c4ed5760ea7e764db80e72c4f3a", Values: "0bf0f8858dc591df5a10068543f218217565156794dea437af2363ad1b2975d3", Keys: "ed8e65dd97f16aa43ed9210936b012a947a5ee861776d6d00e75d9de8de9e35c"},
		{Data: "5302c228ead3bfd86e7b82ae9a9c5b3ca0747ad904d3ed77383bf6c9b8524e8c", Meta: "36b39ced2612dbff3c54569b720c9f8d693901b1ff38f3aa7dc826200f48ffda", Offsets: "51cbf6606faf6fc3593dde3ad79371fc7b849ad9e913e28a309845ba0342756e", Values: "a497c6dd0e323ba87d947fdfc2548d6e453fef5c5106afb9d254106d866632ba", Keys: "7a33dbf04cc2655e548d1c50bf64b4bf17c7ea07fe6522b8965a0e504de70b47"},
	},
	"grouped": {{Data: "16fe9928745fd00ed3d8b752ea9f2b38a4f35d32c1a82d7c4156c21460658c41", Meta: "5ffa53f7f84ea101b28d4b48b08f5e05ff1df28a85cc9581d15f7fb553cb42ce", Offsets: "b71219a3b31030e798ef434be128fca06326076ed9cbbe7775ac81546d357b4b", Values: "090823997d879c764370a4bf56853645ef4465d5eb279a76a1ce5d005a26e63e", Keys: "a33f329a55148003782ad9791913f4898943f88bb8bc143f1ee2756c47773353"}},
	"long":    {{Data: "fe7141fc6afc1b6346a876813a1d37d9505ce4c97bd12c5039ef95a52848af8a", Meta: "013d49896f6cd4129793134e13d19f4065b6675d3073ea494462c90821b90e03", Offsets: "4ee257c4e5d7e6eec8fdff7343869c5fc3c6bf0bb3fc6fac34a432929d6791ee", Values: "d882cc46a48207f5d6f0396b4aea0873a39c19867e56644bdeda53cbe0bdba68", Keys: "5a5a467a2766efff6da1bd8e2a33639f3a1bdf6f9a4c50725aa68b480114a130"}},
}

// swathFiles are the SHA-256 digests of the whole parts swath's writer
// (a832c783, linux/amd64) wrote for these scenarios with created_by
// parityCreatedBy: pages, indexes and footer, stamps included.
var swathFiles = map[string][]string{
	"unique":  {"cae30b3f214c9242b1670f1b07ffeb2cb84f2c830920b57dbf88abc366373da8", "e55c3d6851e829cbbf651993078e6c7d64749d30116257f73621f86743271a7a"},
	"grouped": {"4055a84c909fb076bf592f08e74c60c76291b4b83f415cac96fdf31ead31808f"},
	"long":    {"b7c6119ae34836c1f7f61a1e0835f34464ea8f586594ff1cc6b903263b827338"},
}

const parityCreatedBy = "seekparquet parity"

// parityPlatform is where the goldens were recorded. Compressed pages
// differ on other platforms (swath saw it on macOS/arm64), so Data, Meta,
// Offsets and whole files are compared here only.
const parityPlatform = "linux/amd64"

// swathRow is swath's test row: a key and one column of each type its
// binding supports, optional ones present per bit of has.
type swathRow struct {
	key                string
	size, mtime, inode int64
	mode               uint32
	small              int32
	flag               bool
	kind, link, etag   string
	has                uint8 // 1: mode, inode, small, flag; 2: link
}

// swathSchema is the Parquet schema swath's nodeOf builds for its test
// schema: the key (DELTA_BYTE_ARRAY; a string, or bytes when binary),
// then its columns, etag DELTA_BYTE_ARRAY. The writer adds the key's
// ASCENDING ColumnIndex itself.
func swathSchema(binary bool) *parquet.Schema {
	key := reflect.TypeFor[string]()
	if binary {
		key = reflect.TypeFor[[]byte]()
	}
	fields := []reflect.StructField{
		{Name: "C0", Type: key, Tag: `parquet:"key,delta"`},
		{Name: "C1", Type: reflect.TypeFor[*int64](), Tag: `parquet:"size,optional"`},
		{Name: "C2", Type: reflect.TypeFor[*int64](), Tag: `parquet:"last_modified,optional,timestamp(microsecond:utc)"`},
		{Name: "C3", Type: reflect.TypeFor[string](), Tag: `parquet:"kind"`},
		{Name: "C4", Type: reflect.TypeFor[*uint32](), Tag: `parquet:"mode,optional"`},
		{Name: "C5", Type: reflect.TypeFor[*uint64](), Tag: `parquet:"inode,optional"`},
		{Name: "C6", Type: reflect.TypeFor[*int32](), Tag: `parquet:"small,optional"`},
		{Name: "C7", Type: reflect.TypeFor[*bool](), Tag: `parquet:"flag,optional"`},
		{Name: "C8", Type: reflect.TypeFor[*[]byte](), Tag: `parquet:"link_target,optional"`},
		{Name: "C9", Type: reflect.TypeFor[*string](), Tag: `parquet:"etag,optional"`},
	}
	node := parquet.SchemaOf(reflect.New(reflect.StructOf(fields)).Interface())
	return parquet.NewSchema("swath_listing", deltaETag{node})
}

type deltaETag struct{ parquet.Node }

func (r deltaETag) Fields() []parquet.Field {
	fields := append([]parquet.Field(nil), r.Node.Fields()...)
	fields[9] = encoded{fields[9], &parquet.DeltaByteArray}
	return fields
}

type encoded struct {
	parquet.Field
	enc encoding.Encoding
}

func (f encoded) Encoding() encoding.Encoding { return f.enc }

// parquetRow is swath's fill for one row.
func (r *swathRow) parquetRow() parquet.Row {
	opt := func(ok bool, v parquet.Value, col int) parquet.Value {
		if !ok {
			return parquet.NullValue().Level(0, 0, col)
		}
		return v.Level(0, 1, col)
	}
	return parquet.Row{
		parquet.ByteArrayValue([]byte(r.key)).Level(0, 0, 0),
		opt(true, parquet.Int64Value(r.size), 1),
		opt(true, parquet.Int64Value(r.mtime), 2),
		parquet.ByteArrayValue([]byte(r.kind)).Level(0, 0, 3),
		opt(r.has&1 != 0, parquet.Int32Value(int32(r.mode)), 4),
		opt(r.has&1 != 0, parquet.Int64Value(r.inode), 5),
		opt(r.has&1 != 0, parquet.Int32Value(r.small), 6),
		opt(r.has&1 != 0, parquet.BooleanValue(r.flag), 7),
		opt(r.has&2 != 0, parquet.ByteArrayValue([]byte(r.link)), 8),
		opt(r.etag != "", parquet.ByteArrayValue([]byte(r.etag)), 9),
	}
}

// testRows is swath's testRows: n unique keys from a seeded generator,
// sorted.
func testRows(n int, seed int64) []swathRow {
	rng := rand.New(rand.NewSource(seed))
	alpha := []string{"a", "b", "z", "~", "/", ".", "-", "é", "ÿ", "日", "A", "0"}
	seen := map[string]bool{}
	rows := make([]swathRow, 0, n)
	for len(rows) < n {
		k := ""
		for range 1 + rng.Intn(14) {
			k += alpha[rng.Intn(len(alpha))]
		}
		if seen[k] {
			continue
		}
		seen[k] = true
		r := swathRow{key: k, kind: "FILE", size: int64(len(rows)), mtime: 1_700_000_000_000_000 + int64(len(rows)), etag: strconv.Itoa(len(rows) * 7919)}
		if len(rows)%7 == 0 {
			r.kind, r.link, r.has = "SYMLINK", "t-"+k, 2
		}
		if len(rows)%11 == 0 {
			r.has |= 1
			r.mode, r.inode, r.small, r.flag = 1<<31+5, -1, -7, true
		}
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b swathRow) int { return strings.Compare(a.key, b.key) })
	return rows
}

// groupedRows is swath's groupedRows: testRows' keys repeated, every
// seventh 40 times, the rest one to three times.
func groupedRows(n int) []swathRow {
	var out []swathRow
	for i, r := range testRows(n, 5) {
		reps := 1 + i%3
		if i%7 == 3 {
			reps = 40
		}
		for j := range reps {
			r.size = int64(j)
			out = append(out, r)
		}
	}
	return out
}

// longRows is the long scenario's rows: binary keys over 1,100 bytes,
// half of them beginning with 1,100 0xFF bytes.
func longRows() []swathRow {
	var long []swathRow
	for i := range 3000 {
		lead := strings.Repeat("a", 1100)
		if i >= 1500 {
			lead = strings.Repeat("\xff", 1100)
		}
		long = append(long, swathRow{key: lead + string(rune('A'+i/100%26)) + strings.Repeat("z", i%7) + string(rune('0'+i%10)) + strings.Repeat("\x00", i%100),
			kind: "FILE", size: int64(i), mtime: int64(i) << 20, etag: "e"})
	}
	slices.SortFunc(long, func(a, b swathRow) int { return strings.Compare(a.key, b.key) })
	return slices.CompactFunc(long, func(a, b swathRow) bool { return a.key == b.key })
}

// memFile is a PartFile in memory.
type memFile struct {
	bytes.Buffer
	synced, closed int
}

func (f *memFile) Sync() error  { f.synced++; return nil }
func (f *memFile) Close() error { f.closed++; return nil }

// writeSwathPart writes rows as swath's writer would, with the rows passed
// in batches of 1,500 and reused between batches, two column workers.
func writeSwathPart(t *testing.T, schema *parquet.Schema, schemaID string, unique bool, rows []swathRow) ([]byte, seekparquet.PartInfo) {
	t.Helper()
	f := &memFile{}
	w, err := NewPart(f, schema, PartOptions{Key: seekparquet.Key{Column: "key", Unique: unique}, SchemaID: schemaID,
		CreatedBy: parityCreatedBy, ColumnWorkers: func() int { return 2 }})
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]parquet.Row, 0, 1500)
	for i := 0; i < len(rows); i += 1500 {
		batch = batch[:0]
		for j := i; j < min(i+1500, len(rows)); j++ {
			batch = append(batch, rows[j].parquetRow())
		}
		if n, err := w.WriteRows(batch); err != nil || n != len(batch) {
			t.Fatalf("WriteRows: %d of %d, %v", n, len(batch), err)
		}
		for _, row := range batch { // the caller reuses its buffers
			for k := range row {
				if row[k].Kind() == parquet.ByteArray && !row[k].IsNull() {
					clear(row[k].ByteArray())
				}
			}
		}
	}
	info, err := w.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if f.synced != 1 || f.closed != 1 {
		t.Errorf("Seal synced %d and closed %d times", f.synced, f.closed)
	}
	return f.Bytes(), info
}

// TestSwathParity: the three scenarios of swath's TestFooterOnlyChange,
// written through this package with swath's parameters, give swath's
// bytes.
func TestSwathParity(t *testing.T) {
	unique := testRows(6000, 11)
	scenarios := []struct {
		name     string
		schemaID string
		binary   bool
		unique   bool
		parts    [][]swathRow
	}{
		{"unique", "test/all-types", false, true, [][]swathRow{unique[:4000], unique[4000:]}},
		{"grouped", "test/all-types-grouped", false, false, [][]swathRow{groupedRows(3000)}},
		{"long", "test/binary-key", true, true, [][]swathRow{longRows()}},
	}
	pinned := runtime.GOOS+"/"+runtime.GOARCH == parityPlatform
	for _, sc := range scenarios {
		for i, rows := range sc.parts {
			raw, info := writeSwathPart(t, swathSchema(sc.binary), sc.schemaID, sc.unique, rows)
			got, want := digestPart(t, raw), swathGolden[sc.name][i]
			if pinned && (got.Data != want.Data || got.Meta != want.Meta || got.Offsets != want.Offsets) || got.Values != want.Values || got.Keys != want.Keys {
				t.Errorf("%s part %d: %+v, swath %+v", sc.name, i, got, want)
			}
			sum := sha256.Sum256(raw)
			if file := hex.EncodeToString(sum[:]); pinned && file != swathFiles[sc.name][i] {
				t.Errorf("%s part %d: file %s, swath %s", sc.name, i, file, swathFiles[sc.name][i])
			}
			if info.SHA256 != hex.EncodeToString(sum[:]) || info.Bytes != int64(len(raw)) || info.Rows != int64(len(rows)) ||
				string(info.MinKey) != rows[0].key || string(info.MaxKey) != rows[len(rows)-1].key || info.Path != "" {
				t.Errorf("%s part %d: PartInfo %d rows %d bytes, digest %s", sc.name, i, info.Rows, info.Bytes, info.SHA256)
			}
		}
	}
}

func digestPart(t *testing.T, b []byte) partDigests {
	t.Helper()
	pf, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	md := *pf.Metadata()
	sum := func(h []byte) string { s := sha256.Sum256(h); return hex.EncodeToString(s[:]) }
	var data, offsets, values, keys []byte
	md.RowGroups = slices.Clone(md.RowGroups)
	for g := range md.RowGroups {
		rg := &md.RowGroups[g]
		rg.Columns = slices.Clone(rg.Columns)
		for c := range rg.Columns {
			cc := &rg.Columns[c]
			start := cc.MetaData.DataPageOffset
			if cc.MetaData.DictionaryPageOffset > 0 {
				start = min(start, cc.MetaData.DictionaryPageOffset)
			}
			data = append(data, b[start:start+cc.MetaData.TotalCompressedSize]...)
			offsets = append(offsets, b[cc.OffsetIndexOffset:cc.OffsetIndexOffset+int64(cc.OffsetIndexLength)]...)
			index := b[cc.ColumnIndexOffset : cc.ColumnIndexOffset+int64(cc.ColumnIndexLength)]
			if c == 0 {
				keys = append(keys, index...)
			} else {
				values = append(values, index...)
			}
			cc.OffsetIndexOffset, cc.ColumnIndexOffset, cc.ColumnIndexLength = 0, 0, 0
		}
	}
	md.KeyValueMetadata, md.CreatedBy = nil, ""
	meta, err := thrift.Marshal(new(thrift.CompactProtocol), &md)
	if err != nil {
		t.Fatal(err)
	}
	return partDigests{Data: sum(data), Meta: sum(meta), Offsets: sum(offsets), Values: sum(values), Keys: sum(keys)}
}
