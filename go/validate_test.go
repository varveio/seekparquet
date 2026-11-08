// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// validKeys returns n sorted distinct keys.
func validKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%05d", i)
	}
	return keys
}

// manyPages is a one-part dataset whose key and value columns each have
// several pages in each of two row groups.
func manyPages(geometry testutil.Part) testutil.Dataset {
	geometry.Rows = testutil.Rows(validKeys(600)...)
	if geometry.PageBytes == 0 {
		geometry.PageBytes = 256
	}
	geometry.GroupRows = 300
	return testutil.Dataset{Parts: []testutil.Part{geometry}}
}

// validate validates dir at level, failing the test on an error.
func validate(t *testing.T, dir string, opt ValidateOptions) *Report {
	t.Helper()
	report, err := ValidateDir(ctx, dir, opt)
	must(t, err)
	return report
}

// expectLevels checks each level's result, and the code of a failed one.
func expectLevels(t *testing.T, report *Report, want map[Level]string) {
	t.Helper()
	for level, w := range want {
		lr, ok := report.Level(level)
		if !ok {
			t.Fatalf("level %s was not attempted: %s", level, describeReport(report))
		}
		got := string(lr.Result)
		if lr.Result == Failed {
			got = string(lr.Failure.Code)
		}
		if got != w {
			t.Fatalf("level %s: got %s, want %s: %s", level, got, w, describeReport(report))
		}
		if lr.Result == Incomplete && lr.Reason == "" {
			t.Fatalf("level %s is incomplete without a reason", level)
		}
	}
}

func TestValidatePageVersionsAndCodecs(t *testing.T) {
	codecs := map[string]parquet.WriterOption{
		"uncompressed": parquet.Compression(&parquet.Uncompressed),
		"snappy":       parquet.Compression(&parquet.Snappy),
		"gzip":         parquet.Compression(&parquet.Gzip),
		"zstd":         parquet.Compression(&parquet.Zstd),
	}
	for _, version := range []int{1, 2} {
		for name, codec := range codecs {
			t.Run(fmt.Sprintf("v%d/%s", version, name), func(t *testing.T) {
				dir := write(t, manyPages(testutil.Part{DataPageVersion: version, Compression: codec}))
				report := validate(t, dir, ValidateOptions{})
				expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "complete"})
				profile, _ := report.Level(LevelProfile)
				full, _ := report.Level(LevelFull)
				if profile.Checked.Rows != 600 || profile.Checked.RowGroups != 2 || profile.Checked.Pages < 4 {
					t.Fatalf("profile checked %+v", profile.Checked)
				}
				if full.Checked.Rows != 600 || full.Checked.Chunks != 4 || full.Checked.Pages < 4 {
					t.Fatalf("full checked %+v", full.Checked)
				}
			})
		}
	}
}

// rewriteIndexes decodes the ColumnIndex and OffsetIndex of one column
// chunk of part 0, applies change, and writes them back after the column
// data, pointing the footer at them: an index no writer produced.
func rewriteIndexes(t *testing.T, dir string, group, column int, change func(*format.ColumnIndex, *format.OffsetIndex)) {
	t.Helper()
	path := partFile(dir, 0)
	raw, err := os.ReadFile(path)
	must(t, err)
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
	body := raw[:len(raw)-8-n]
	var meta format.FileMetaData
	must(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw[len(body):len(raw)-8], &meta))
	chunk := &meta.RowGroups[group].Columns[column]
	var ci format.ColumnIndex
	var oi format.OffsetIndex
	must(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw[chunk.ColumnIndexOffset:chunk.ColumnIndexOffset+int64(chunk.ColumnIndexLength)], &ci))
	must(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw[chunk.OffsetIndexOffset:chunk.OffsetIndexOffset+int64(chunk.OffsetIndexLength)], &oi))
	change(&ci, &oi)
	out := append([]byte(nil), body...)
	for _, idx := range []struct {
		v      any
		off    *int64
		length *int32
	}{{&ci, &chunk.ColumnIndexOffset, &chunk.ColumnIndexLength}, {&oi, &chunk.OffsetIndexOffset, &chunk.OffsetIndexLength}} {
		enc, err := thrift.Marshal(new(thrift.CompactProtocol), idx.v)
		must(t, err)
		*idx.off, *idx.length = int64(len(out)), int32(len(enc))
		out = append(out, enc...)
	}
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
	must(t, err)
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	out = append(out, "PAR1"...)
	must(t, os.WriteFile(path, out, 0o644))
	must(t, testutil.SyncSizes(dir))
}

// omitPage drops page i from an index pair, so that the page before it
// claims its rows: every remaining entry is a real page, in order, inside
// the chunk, and the lists agree in length.
func omitPage(i int) func(*format.ColumnIndex, *format.OffsetIndex) {
	return func(ci *format.ColumnIndex, oi *format.OffsetIndex) {
		oi.PageLocations = append(oi.PageLocations[:i:i], oi.PageLocations[i+1:]...)
		ci.NullPages = append(ci.NullPages[:i:i], ci.NullPages[i+1:]...)
		ci.MinValues = append(ci.MinValues[:i:i], ci.MinValues[i+1:]...)
		ci.MaxValues = append(ci.MaxValues[:i:i], ci.MaxValues[i+1:]...)
		if len(ci.NullCounts) > 0 {
			ci.NullCounts = append(ci.NullCounts[:i:i], ci.NullCounts[i+1:]...)
		}
	}
}

// TestValidateOmittedPage: an otherwise plausible key OffsetIndex that
// omits a physical page fails structural validation (the entry before it
// claims rows its page does not hold), and fails the profile walk on its
// own, which enumerates pages from the chunk's start without the index. A
// value column's index omitting a page fails full validation.
func TestValidateOmittedPage(t *testing.T) {
	dir := write(t, manyPages(testutil.Part{}))
	rewriteIndexes(t, dir, 0, 0, omitPage(1))
	report := validate(t, dir, ValidateOptions{})
	expectLevels(t, report, map[Level]string{LevelStructural: "invalid_page_index", LevelProfile: "incomplete", LevelFull: "incomplete"})
	if s, _ := report.Level(LevelStructural); s.At.Page != 0 || s.At.RowGroup != 0 || s.At.Column != "key" {
		t.Fatalf("structural failed at %s", s.At)
	}

	// The profile walk alone, without the structural pass before it.
	d := open(t, dir, Options{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	v := &validator{ctx: ctx, limits: Limits{}.withDefaults(), d: d}
	lr := LevelReport{Level: LevelProfile}
	if err := v.profile(&lr); CodeOf(err) != CodeInvalidPageIndex || lr.At.Page != 0 {
		t.Fatalf("profile alone: %v at %s", err, lr.At)
	}

	dir = write(t, manyPages(testutil.Part{}))
	rewriteIndexes(t, dir, 1, 1, omitPage(1))
	report = validate(t, dir, ValidateOptions{})
	expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "invalid_parquet"})
	if f, _ := report.Level(LevelFull); f.At.RowGroup != 1 || f.At.Column != "size" {
		t.Fatalf("full failed at %s", f.At)
	}
}

// TestValidateValuePageChecksum flips the last byte of a value column's last
// page, which a key read never visits: the page fails its checksum at the
// full level only.
func TestValidateValuePageChecksum(t *testing.T) {
	dir := write(t, manyPages(testutil.Part{}))
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	chunk := meta.RowGroups[1].Columns[1]
	raw, err := os.ReadFile(partFile(dir, 0))
	must(t, err)
	var oi format.OffsetIndex
	must(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw[chunk.OffsetIndexOffset:chunk.OffsetIndexOffset+int64(chunk.OffsetIndexLength)], &oi))
	last := oi.PageLocations[len(oi.PageLocations)-1]
	raw[last.Offset+int64(last.CompressedPageSize)-1] ^= 1
	must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))

	d := verified(t, dir, Options{})
	if rows, err := d.Range(ctx, Range{Limit: 1000}); err != nil || len(rows) != 600 {
		t.Fatalf("keys only: %d rows, %v", len(rows), err)
	}
	report := validate(t, dir, ValidateOptions{})
	expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "invalid_parquet"})
	f, _ := report.Level(LevelFull)
	if f.At.RowGroup != 1 || f.At.Column != "size" || f.At.Page != len(oi.PageLocations)-1 || !strings.Contains(f.Failure.Detail, "checksum") {
		t.Fatalf("full failed at %s: %v", f.At, f.Failure)
	}
}

// TestValidateUnsupported: a codec or a legacy encoding the reader does not
// decode leaves the level that needs it incomplete, never failed, and the
// levels below keep their result.
func TestValidateUnsupported(t *testing.T) {
	isUnsupported := func(t *testing.T, report *Report, level Level) {
		t.Helper()
		lr, _ := report.Level(level)
		var u *UnsupportedError
		if !errors.As(lr.Err, &u) || !strings.HasPrefix(lr.Reason, "unsupported") {
			t.Fatalf("level %s: %s", level, describeReport(report))
		}
	}
	t.Run("LZO key column", func(t *testing.T) {
		dir := write(t, manyPages(testutil.Part{}))
		rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Codec = format.LZO })
		report := validate(t, dir, ValidateOptions{})
		expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "incomplete", LevelFull: "incomplete"})
		isUnsupported(t, report, LevelProfile)
	})
	t.Run("LZO value column", func(t *testing.T) {
		dir := write(t, manyPages(testutil.Part{}))
		rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].Columns[2].MetaData.Codec = format.LZO })
		report := validate(t, dir, ValidateOptions{})
		expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "incomplete"})
		isUnsupported(t, report, LevelFull)
	})
	t.Run("BIT_PACKED value page", func(t *testing.T) {
		dir := write(t, manyPages(testutil.Part{DataPageVersion: 1}))
		meta, err := testutil.ReadFooter(partFile(dir, 0))
		must(t, err)
		off := meta.RowGroups[0].Columns[1].MetaData.DataPageOffset
		editPageHeader(t, partFile(dir, 0), off, func(h *format.PageHeader) { h.DataPageHeader.V.Encoding = format.BitPacked })
		report := validate(t, dir, ValidateOptions{})
		expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "incomplete"})
		isUnsupported(t, report, LevelFull)
	})
	t.Run("unsupported column, corrupt column", func(t *testing.T) {
		// A column the validator cannot decode does not hide a fault in one
		// it can.
		dir := write(t, manyPages(testutil.Part{}))
		rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[1].MetaData.Codec = format.LZO })
		meta, err := testutil.ReadFooter(partFile(dir, 0))
		must(t, err)
		chunk := meta.RowGroups[1].Columns[2].MetaData
		raw, err := os.ReadFile(partFile(dir, 0))
		must(t, err)
		raw[chunk.DataPageOffset+chunk.TotalCompressedSize-1] ^= 1
		must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
		expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelFull: "invalid_parquet"})
	})
}

// nestedRow has a repeated column (tags, a list), a nested one (meta.a)
// and a repeated leaf (nums), besides the key.
type nestedRow struct {
	Key  []byte     `parquet:"key"`
	Tags []string   `parquet:"tags,list"`
	Meta *nestedSub `parquet:"meta,optional"`
	Nums []int64    `parquet:"nums"`
}

type nestedSub struct {
	A *int32 `parquet:"a,optional"`
}

// nestedRows returns 400 rows with empty and non-empty lists, and null and
// present structs and fields.
func nestedRows() []nestedRow {
	rows := make([]nestedRow, 400)
	for i := range rows {
		rows[i] = nestedRow{Key: []byte(fmt.Sprintf("k%04d", i))}
		for j := range i % 4 {
			rows[i].Tags = append(rows[i].Tags, fmt.Sprintf("t%d", j))
			rows[i].Nums = append(rows[i].Nums, int64(i*j))
		}
		if i%3 != 0 {
			rows[i].Meta = &nestedSub{A: ptr(int32(i))}
		} else if i%2 == 0 {
			rows[i].Meta = &nestedSub{}
		}
	}
	return rows
}

// nestedDataset writes nestedRows in two row groups of small pages.
func nestedDataset(t *testing.T, geometry testutil.Part) string {
	geometry.Rows, geometry.PageBytes, geometry.GroupRows = nestedRows(), 256, 200
	return write(t, testutil.Dataset{Parts: []testutil.Part{geometry}})
}

// TestValidateNested decodes the levels of repeated and nested columns,
// which the reader does not project, in V1 and V2 pages: they complete at
// the full level, and a corrupt page in one fails it.
func TestValidateNested(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			dir := nestedDataset(t, testutil.Part{DataPageVersion: version})
			report := validate(t, dir, ValidateOptions{})
			expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "complete"})
			if f, _ := report.Level(LevelFull); f.Checked.Chunks != 2*3 || f.Checked.Rows != 400 {
				t.Fatalf("full checked %+v", f.Checked)
			}
			meta, err := testutil.ReadFooter(partFile(dir, 0))
			must(t, err)
			chunk := meta.RowGroups[1].Columns[1].MetaData // tags.list.element
			raw, err := os.ReadFile(partFile(dir, 0))
			must(t, err)
			raw[chunk.DataPageOffset+chunk.TotalCompressedSize-1] ^= 1
			must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
			report = validate(t, dir, ValidateOptions{})
			expectLevels(t, report, map[Level]string{LevelProfile: "complete", LevelFull: "invalid_parquet"})
			if f, _ := report.Level(LevelFull); f.At.Column != "tags.list.element" || f.At.RowGroup != 1 {
				t.Fatalf("full failed at %s", f.At)
			}
		})
	}
}

// TestValidateLimits: limits and hostile declared sizes leave a level
// incomplete, or are refused as data before anything is allocated for them.
func TestValidateLimits(t *testing.T) {
	isLimit := func(t *testing.T, report *Report, level Level) {
		t.Helper()
		lr, _ := report.Level(level)
		if lr.Result != Incomplete || !errors.Is(lr.Err, ErrLimitExceeded) || !strings.HasPrefix(lr.Reason, "limit") {
			t.Fatalf("level %s: %s", level, describeReport(report))
		}
	}
	dir := write(t, manyPages(testutil.Part{}))
	for name, limits := range map[string]Limits{
		"footer":     {FooterBytes: 64},
		"page index": {PageIndexBytes: 8},
		"page bytes": {PageBytes: 32},
	} {
		t.Run(name, func(t *testing.T) {
			report := validate(t, dir, ValidateOptions{Limits: limits})
			isLimit(t, report, LevelStructural)
			expectLevels(t, report, map[Level]string{LevelProfile: "incomplete", LevelFull: "incomplete"})
		})
	}
	t.Run("page values", func(t *testing.T) {
		// Key pages hold more than 8 values: the page headers structural
		// reads declare more than the limit.
		report := validate(t, dir, ValidateOptions{Limits: Limits{PageValues: 8}})
		isLimit(t, report, LevelStructural)
	})

	// Index lengths of 1 GiB in a footer: refused as data, not allocated.
	for name, column := range map[string]int{"key": 0, "value": 1} {
		t.Run("hostile index length "+name, func(t *testing.T) {
			dir := write(t, manyPages(testutil.Part{}))
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[0].Columns[column].OffsetIndexLength = 1 << 30 })
			var report *Report
			if n := allocated(func() { report = validate(t, dir, ValidateOptions{Limits: Limits{PageIndexBytes: 2 << 30}}) }); n > 16<<20 {
				t.Fatalf("allocated %d bytes", n)
			}
			if column == 0 {
				expectLevels(t, report, map[Level]string{LevelStructural: "invalid_page_index"})
			} else {
				expectLevels(t, report, map[Level]string{LevelProfile: "complete", LevelFull: "invalid_parquet"})
			}
		})
	}
}

// TestValidateManifest: a malformed manifest fails structural validation,
// and the digest level, with its refusal.
func TestValidateManifest(t *testing.T) {
	for name, raw := range map[string]string{
		"duplicate member": `{"format":"seekparquet","format":"seekparquet"}`,
		"fraction":         `{"format":"seekparquet","format_version":0.0,"draft_revision":1,"schema_id":"s","key":{"column":"key","unique":true},"parts":[]}`,
		"not json":         `{"format":`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			must(t, os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(raw), 0o644))
			expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelStructural: "invalid_manifest", LevelProfile: "incomplete", LevelFull: "incomplete"})
			expectLevels(t, validate(t, dir, ValidateOptions{Level: LevelDigest}), map[Level]string{LevelDigest: "invalid_manifest"})
		})
	}
	dir := t.TempDir()
	expectLevels(t, validate(t, dir, ValidateOptions{Level: LevelStructural}), map[Level]string{LevelStructural: "no_manifest"})
	if _, err := ValidateDir(ctx, dir, ValidateOptions{Level: "other"}); err == nil {
		t.Fatal("an unknown level was accepted")
	}
}

// TestValidateDigest: the digest level compares declared digests, completes
// with nothing checked when none is declared, and reads the parts as bytes,
// whether or not they are Parquet.
func TestValidateDigest(t *testing.T) {
	setDigest := func(t *testing.T, dir, sum string) {
		editManifest(t, dir, func(m map[string]any) { testutil.ManifestPart(m, 0)["sha256"] = sum })
	}
	sumOf := func(t *testing.T, dir string) string {
		raw, err := os.ReadFile(partFile(dir, 0))
		must(t, err)
		s := sha256.Sum256(raw)
		return hex.EncodeToString(s[:])
	}
	digest := func(t *testing.T, dir string) LevelReport {
		report := validate(t, dir, ValidateOptions{Level: LevelDigest})
		if len(report.Levels) != 1 {
			t.Fatalf("digest validation ran %s", describeReport(report))
		}
		return report.Levels[0]
	}

	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}, []string{"c"}))
	if lr := digest(t, dir); lr.Result != Complete || lr.Checked.Digests != 0 || !strings.Contains(lr.Reason, "nothing checked") {
		t.Fatalf("no digests: %+v", lr)
	}
	setDigest(t, dir, sumOf(t, dir))
	if lr := digest(t, dir); lr.Result != Complete || lr.Checked.Digests != 1 {
		t.Fatalf("matching digest: %+v", lr)
	}
	setDigest(t, dir, strings.Repeat("0", 64))
	if lr := digest(t, dir); lr.Result != Failed || lr.Failure.Code != CodeDigestMismatch || lr.At.Part != testutil.PartPath(0) {
		t.Fatalf("mismatched digest: %+v", lr)
	}

	// Not Parquet, with a matching digest: digest completes, structural fails.
	must(t, os.WriteFile(partFile(dir, 0), []byte("not a parquet file"), 0o644))
	must(t, testutil.SyncSizes(dir))
	setDigest(t, dir, sumOf(t, dir))
	if lr := digest(t, dir); lr.Result != Complete || lr.Checked.Digests != 1 {
		t.Fatalf("digest of a file that is not Parquet: %+v", lr)
	}
	expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelStructural: "invalid_footer"})

	must(t, os.Remove(partFile(dir, 0)))
	if lr := digest(t, dir); lr.Result != Failed || lr.Failure.Code != CodeMissingPart {
		t.Fatalf("missing part: %+v", lr)
	}
}

// TestValidateWhatReadsSkip: datasets a reader opens, and answers queries
// over, whose faults lie in pages those queries skip (CHECK-4). Validation
// visits every page and fails them.
func TestValidateWhatReadsSkip(t *testing.T) {
	t.Run("key order inside a skipped page", func(t *testing.T) {
		keys := validKeys(600)
		keys[100], keys[101] = keys[101], keys[100]
		dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(keys...), PageBytes: 256, GroupRows: 300}}})
		d := verified(t, dir, Options{})
		if rows, err := d.Range(ctx, Range{Start: []byte(keys[550]), Limit: 10, Columns: []string{"size"}}); err != nil || len(rows) != 10 {
			t.Fatalf("a read past the fault: %d rows, %v", len(rows), err)
		}
		report := validate(t, dir, ValidateOptions{Level: LevelProfile})
		expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "key_order"})
		if lr, _ := report.Level(LevelProfile); lr.Failure.Row != 101 || string(lr.Failure.Key) != keys[101] {
			t.Fatalf("key_order at row %d key %q", lr.Failure.Row, lr.Failure.Key)
		}
	})
	t.Run("lying bounds on a skipped page", func(t *testing.T) {
		dir := write(t, manyPages(testutil.Part{}))
		// Page 1's maximum below its last key: not a bound (RG-5).
		rewriteIndexes(t, dir, 0, 0, func(ci *format.ColumnIndex, _ *format.OffsetIndex) {
			ci.MaxValues[1] = append([]byte(nil), ci.MinValues[1]...)
		})
		d := verified(t, dir, Options{})
		if rows, err := d.Range(ctx, Range{Start: []byte("k00250"), Limit: 10}); err != nil || len(rows) != 10 {
			t.Fatalf("a read past the fault: %d rows, %v", len(rows), err)
		}
		report := validate(t, dir, ValidateOptions{Level: LevelProfile})
		expectLevels(t, report, map[Level]string{LevelStructural: "complete", LevelProfile: "invalid_page_index"})
		if lr, _ := report.Level(LevelProfile); lr.At.Page != 1 || lr.At.RowGroup != 0 {
			t.Fatalf("invalid_page_index at %s", lr.At)
		}
	})
}

// failingSource fails every read of a part's bytes between its magic and
// its footer, as a storage failure.
type failingSource struct{ Source }

type failingFile struct {
	SourceFile
	footer int64
}

func (f failingFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= 4 && off < f.footer {
		return 0, errors.New("storage failure")
	}
	return f.SourceFile.ReadAt(p, off)
}

func (s failingSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil || path == ManifestName {
		return f, err
	}
	var tail [8]byte
	if _, err := f.ReadAt(tail[:], f.Size()-8); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return failingFile{f, f.Size() - 8 - int64(binary.LittleEndian.Uint32(tail[:4]))}, nil
}

// TestValidateStorageAndContext: a storage failure leaves the level
// incomplete; a cancelled context ends validation with its error.
func TestValidateStorageAndContext(t *testing.T) {
	dir := write(t, manyPages(testutil.Part{}))
	report, err := Validate(ctx, failingSource{dirOf(t, dir)}, ValidateOptions{})
	must(t, err)
	lr, _ := report.Level(LevelStructural)
	if lr.Result != Incomplete || !strings.HasPrefix(lr.Reason, "storage") || CodeOf(lr.Err) != "" {
		t.Fatalf("storage failure: %s", describeReport(report))
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ValidateDir(cancelled, dir, ValidateOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

// editPage decodes the page header at off in part 0 of dir, lets change
// edit the header and the page's bytes in place, and writes both back,
// with the checksum recomputed when the page has one. The header must keep
// its encoded length.
func editPage(t *testing.T, dir string, off int64, change func(h *format.PageHeader, data []byte)) {
	t.Helper()
	path := partFile(dir, 0)
	raw, err := os.ReadFile(path)
	must(t, err)
	var h format.PageHeader
	n, err := decodeBounded(raw[off:], &h, 1<<20)
	must(t, err)
	data := raw[off+int64(n) : off+int64(n)+int64(h.CompressedPageSize)]
	change(&h, data)
	if h.CRC != 0 {
		h.CRC = int32(crc32.ChecksumIEEE(data))
	}
	must(t, os.WriteFile(path, raw, 0o644))
	editPageHeader(t, path, off, func(e *format.PageHeader) { *e = h })
}

// chunkOf returns the metadata of a column chunk of part 0.
func chunkOf(t *testing.T, dir string, group, column int) format.ColumnMetaData {
	t.Helper()
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	return meta.RowGroups[group].Columns[column].MetaData
}

// TestValidateLevelEncodings: RLE is the only encoding Parquet allows for
// levels besides the legacy BIT_PACKED, which is unsupported; a page that
// declares PLAIN levels is corrupt, for a nested column and a flat one.
func TestValidateLevelEncodings(t *testing.T) {
	for name, c := range map[string]struct {
		dir    func(t *testing.T) string
		column int
	}{
		"nested meta.a": {func(t *testing.T) string { return nestedDataset(t, testutil.Part{DataPageVersion: 1}) }, 2},
		"flat name":     {func(t *testing.T) string { return write(t, manyPages(testutil.Part{DataPageVersion: 1})) }, 2},
	} {
		for enc, want := range map[format.Encoding]string{format.Plain: "invalid_parquet", format.BitPacked: "incomplete"} {
			t.Run(name+"/"+enc.String(), func(t *testing.T) {
				dir := c.dir(t)
				editPage(t, dir, chunkOf(t, dir, 0, c.column).DataPageOffset, func(h *format.PageHeader, _ []byte) {
					h.DataPageHeader.V.DefinitionLevelEncoding = enc
				})
				expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelProfile: "complete", LevelFull: want})
			})
		}
	}
}

// dictRow has a dictionary-encoded column.
type dictRow struct {
	Key  []byte `parquet:"key"`
	Name string `parquet:"name,dict"`
}

// TestValidateDictionaryPage: a dictionary page is checked when the walk
// finds it, whether or not a data page uses it: a payload failing its
// checksum fails full validation before any data page is decoded.
func TestValidateDictionaryPage(t *testing.T) {
	rows := make([]dictRow, 200)
	for i := range rows {
		rows[i] = dictRow{Key: []byte(fmt.Sprintf("k%04d", i)), Name: []string{"x", "y", "z"}[i%3]}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 128}}})
	chunk := chunkOf(t, dir, 0, 1)
	if chunk.DictionaryPageOffset == 0 {
		t.Fatal("the name column has no dictionary page")
	}
	raw, err := os.ReadFile(partFile(dir, 0))
	must(t, err)
	raw[chunk.DataPageOffset-1] ^= 1 // the dictionary page's last byte
	must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))

	report := validate(t, dir, ValidateOptions{})
	expectLevels(t, report, map[Level]string{LevelProfile: "complete", LevelFull: "invalid_parquet"})
	if f, _ := report.Level(LevelFull); f.At.Page != -1 || !strings.Contains(f.Failure.Detail, "checksum") {
		t.Fatalf("full failed at %s: %v", f.At, f.Failure)
	}
	// A walk that decodes no data page still checks the dictionary.
	d := open(t, dir, Options{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	v := &validator{ctx: ctx, limits: Limits{}.withDefaults(), d: d}
	p := d.r.parts[0]
	c, err := p.openChunk(ctx, 0, p.leaves["name"], &v.st, &v.pages)
	must(t, err)
	err = v.walk(c, 200, func(_ int, loc pageLoc, _ *format.PageHeader, _ int64, _ []byte) (int64, error) { return loc.rows, nil })
	if CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("walk without decoding: %v", err)
	}
}

// TestValidatePageHeaders: a V2 page's null count must match its decoded
// levels, nested or flat, and a chunk of a repeated column must begin a
// row, even when its row and value totals agree.
func TestValidatePageHeaders(t *testing.T) {
	t.Run("nested V2 null count", func(t *testing.T) {
		dir := nestedDataset(t, testutil.Part{DataPageVersion: 2})
		editPage(t, dir, chunkOf(t, dir, 0, 2).DataPageOffset, func(h *format.PageHeader, _ []byte) { h.DataPageHeaderV2.V.NumNulls++ })
		expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelProfile: "complete", LevelFull: "invalid_parquet"})
	})
	t.Run("flat V2 null count", func(t *testing.T) {
		dir := write(t, manyPages(testutil.Part{DataPageVersion: 2}))
		editPage(t, dir, chunkOf(t, dir, 0, 1).DataPageOffset, func(h *format.PageHeader, _ []byte) { h.DataPageHeaderV2.V.NumNulls++ })
		expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelProfile: "complete", LevelFull: "invalid_parquet"})
	})
	t.Run("first repetition level", func(t *testing.T) {
		dir := nestedDataset(t, testutil.Part{DataPageVersion: 1, Compression: parquet.Compression(&parquet.Uncompressed)})
		editPage(t, dir, chunkOf(t, dir, 0, 1).DataPageOffset, func(_ *format.PageHeader, data []byte) {
			// The repetition levels come first: a 4-byte length, then runs.
			// Swap the first level (0) with the first 1 in the leading
			// bit-packed group, keeping the page's rows and entries.
			run, k := binary.Uvarint(data[4:])
			group := data[4+k:]
			if run&1 == 0 || group[0]&1 != 0 || group[0] == 0 {
				t.Fatalf("the levels do not begin with a bit-packed group holding a 0 then a 1 (run %d, bits %08b)", run, group[0])
			}
			first := group[0] & -group[0] // its lowest set bit
			group[0] = group[0]&^first | 1
		})
		report := validate(t, dir, ValidateOptions{})
		expectLevels(t, report, map[Level]string{LevelProfile: "complete", LevelFull: "invalid_parquet"})
		if f, _ := report.Level(LevelFull); !strings.Contains(f.Failure.Detail, "repetition level") {
			t.Fatalf("full: %v", f.Failure)
		}
	})
}

// TestValidateKeyValueCount: the key chunk's metadata value count must
// equal the keys profile validation decodes.
func TestValidateKeyValueCount(t *testing.T) {
	dir := write(t, manyPages(testutil.Part{}))
	rewrite(t, dir, 0, func(m *format.FileMetaData) { m.RowGroups[1].Columns[0].MetaData.NumValues++ })
	expectLevels(t, validate(t, dir, ValidateOptions{}), map[Level]string{LevelStructural: "complete", LevelProfile: "invalid_parquet"})
}
