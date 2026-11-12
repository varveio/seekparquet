// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// footerPart writes one part of keys in row groups of groupRows rows and
// returns its bytes and its footer's length.
func footerPart(t *testing.T, keys []string, groupRows int) ([]byte, int64) {
	t.Helper()
	dir := write(t, parts(testutil.Part{GroupRows: groupRows, PageBytes: 512}, keys))
	raw, err := os.ReadFile(partFile(dir, 0))
	must(t, err)
	return raw, int64(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
}

// footerParts returns a part smaller than the footer window, one larger
// whose footer fits in it, and one whose footer (3,000 row groups) does
// not, with their footers' lengths.
func footerParts(t *testing.T) (small, large, huge []byte, smallFooter, largeFooter, hugeFooter int64) {
	t.Helper()
	small, smallFooter = footerPart(t, fixtureKeys(500), 100)
	large, largeFooter = footerPart(t, fixtureKeys(5000), 1000)
	huge, hugeFooter = footerPart(t, fixtureKeys(3000), 1)
	if int64(len(small)) > footerWindow || int64(len(large)) <= footerWindow || largeFooter+8 > 3000 || hugeFooter+8 <= footerWindow {
		t.Fatalf("fixture sizes: small %d, large %d (footer %d), huge footer %d", len(small), len(large), largeFooter, hugeFooter)
	}
	return small, large, huge, smallFooter, largeFooter, hugeFooter
}

// TestFooterWindowReads: readFooter reads a part's end in one window,
// which holds the footer when it fits, and reads the head magic apart only
// when the file is larger than the window. Each case states its exact
// reads and bytes, and opens the same footer as an exact read (maxWindow
// 0, InspectFooter's) does.
func TestFooterWindowReads(t *testing.T) {
	small, large, huge, smallFooter, _, hugeFooter := footerParts(t)
	lim := Limits{}.withDefaults()
	bounded := lim
	bounded.FooterBytes = 3000
	for _, tc := range []struct {
		name         string
		raw          []byte
		lim          Limits
		window       int64
		reads, bytes int64
	}{
		{"small file, read whole", small, lim, footerWindow, 1, int64(len(small))},
		{"large file, footer in the window", large, lim, footerWindow, 2, 4 + footerWindow},
		{"footer larger than the window", huge, lim, footerWindow, 3, 4 + hugeFooter + 8},
		{"window bounded by FooterBytes", large, bounded, footerWindow, 2, 4 + 3000 + 8},
		{"exact read", small, lim, 0, 3, 4 + 8 + smallFooter},
		{"exact read of a large footer", huge, lim, 0, 3, 4 + 8 + hugeFooter},
	} {
		var st ReadStats
		f, _, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &tc.lim, &st, tc.window, 64, false)
		must(t, err)
		if st.SourceReads != tc.reads || st.SourceBytes != tc.bytes {
			t.Errorf("%s: %d reads of %d bytes, want %d of %d", tc.name, st.SourceReads, st.SourceBytes, tc.reads, tc.bytes)
		}
		exact, _, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &lim, nil, 0, 64, false)
		must(t, err)
		if !reflect.DeepEqual(f.Metadata(), exact.Metadata()) {
			t.Errorf("%s: the footer differs from an exact read's", tc.name)
		}
	}
}

// TestFooterWindowRefusals: the checks of the head and the trailer hold
// whichever way they were read: a file read whole, a file larger than the
// window, and a footer larger than the window. A footer over
// Limits.FooterBytes is refused before anything beyond the bounded window
// is read.
func TestFooterWindowRefusals(t *testing.T) {
	small, large, huge, _, largeFooter, _ := footerParts(t)
	lim := Limits{}.withDefaults()
	tight := Limits{FooterBytes: 1000}.withDefaults()
	edit := func(raw []byte, at int, b []byte) []byte {
		out := bytes.Clone(raw)
		copy(out[at:], b)
		return out
	}
	length := func(raw []byte, n int) []byte {
		return edit(raw, len(raw)-8, binary.LittleEndian.AppendUint32(nil, uint32(n)))
	}
	limited := func(err error) bool { return errors.Is(err, ErrLimitExceeded) }
	for name, raw := range map[string][]byte{"small": small, "large": large, "huge": huge} {
		for _, tc := range []struct {
			what string
			raw  []byte
			lim  Limits
			want func(error) bool
		}{
			{"PARE head", edit(raw, 0, []byte("PARE")), lim, isEncrypted},
			{"PARE tail", edit(raw, len(raw)-4, []byte("PARE")), lim, isEncrypted},
			{"bad head", edit(raw, 0, []byte("PAR2")), lim, isInvalidFooter},
			{"bad tail", edit(raw, len(raw)-4, []byte("PAR2")), lim, isInvalidFooter},
			{"length past the file", length(raw, len(raw)-11), lim, isInvalidFooter},
			{"length past the window", length(raw, min(len(raw)-12, footerWindow+100)), lim, isInvalidFooter},
			{"length over FooterBytes", raw, tight, limited},
		} {
			var st ReadStats
			_, _, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &tc.lim, &st, footerWindow, 64, false)
			if !tc.want(err) {
				t.Errorf("%s, %s: %v", name, tc.what, err)
			}
			if tc.what == "length over FooterBytes" && st.SourceBytes > 4+1000+8 {
				t.Errorf("%s, %s: read %d bytes before refusing", name, tc.what, st.SourceBytes)
			}
		}
	}
	if largeFooter <= 1000 {
		t.Fatalf("the large part's footer, %d bytes, is within the tight limit", largeFooter)
	}
}

// failAt is a ReaderAt over raw that fails a read starting at off.
type failAt struct {
	*bytes.Reader
	off int64
}

var errBoom = errors.New("boom")

func (f failAt) ReadAt(p []byte, off int64) (int, error) {
	if off == f.off {
		return 0, errBoom
	}
	return f.Reader.ReadAt(p, off)
}

// TestFooterWindowEdgeRefusals: a file shorter than 12 bytes, a declared
// footer length of 0, and a storage error on the read of the bytes before
// the window end as they do when the footer is read exactly (maxWindow 0).
func TestFooterWindowEdgeRefusals(t *testing.T) {
	small, _, huge, _, _, hugeFooter := footerParts(t)
	lim := Limits{}.withDefaults()
	zero := bytes.Clone(small)
	copy(zero[len(zero)-8:], []byte{0, 0, 0, 0})
	for _, tc := range []struct {
		name string
		raw  []byte
		at   func([]byte) io.ReaderAt
		want func(error) bool
	}{
		{"shorter than 12", small[:11], nil, isInvalidFooter},
		{"empty", nil, nil, isInvalidFooter},
		{"footer length 0", zero, nil, isInvalidFooter},
		{"storage error before the window", huge, func(raw []byte) io.ReaderAt {
			return failAt{bytes.NewReader(raw), int64(len(raw)) - 8 - hugeFooter}
		}, func(err error) bool {
			var r *RefusalError
			return errors.Is(err, errBoom) && !errors.As(err, &r)
		}},
	} {
		var texts []string
		for _, window := range []int64{footerWindow, 0} {
			var at io.ReaderAt = bytes.NewReader(tc.raw)
			if tc.at != nil {
				at = tc.at(tc.raw)
			}
			_, _, err := readFooter(ctx, "p", at, int64(len(tc.raw)), &lim, nil, window, 64, false)
			if !tc.want(err) {
				t.Errorf("%s, window %d: %v", tc.name, window, err)
			}
			texts = append(texts, fmt.Sprint(err))
		}
		if texts[0] != texts[1] {
			t.Errorf("%s: window error %q, exact read's %q", tc.name, texts[0], texts[1])
		}
	}
}

// TestFooterWindowNotRetained: what the opened file keeps of the window
// read is exactly the footer with its trailer and the head magic, in
// allocations of their own, so that the 64 KiB window is not pinned for as
// long as the part is open.
func TestFooterWindowNotRetained(t *testing.T) {
	small, large, huge, smallFooter, largeFooter, hugeFooter := footerParts(t)
	lim := Limits{}.withDefaults()
	for _, tc := range []struct {
		name   string
		raw    []byte
		footer int64
	}{{"small", small, smallFooter}, {"large", large, largeFooter}, {"huge", huge, hugeFooter}} {
		var got []prefetched
		var window []byte
		testFooterRetained = func(p prefetched, win []byte) { got, window = append(got, p), win }
		_, _, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &lim, nil, footerWindow, 64, false)
		testFooterRetained = nil
		must(t, err)
		if len(got) != 1 {
			t.Fatalf("%s: %d readers", tc.name, len(got))
		}
		p := got[0]
		if int64(len(p.data)) != tc.footer+8 || cap(p.data) != len(p.data) || len(p.head) != 4 || cap(p.head) != 4 {
			t.Errorf("%s: retained footer len %d cap %d (want %d), head len %d cap %d", tc.name, len(p.data), cap(p.data), tc.footer+8, len(p.head), cap(p.head))
		}
		for what, b := range map[string][]byte{"footer": p.data, "head": p.head} {
			if overlaps(b, window) {
				t.Errorf("%s: the retained %s is a slice of the %d-byte window", tc.name, what, len(window))
			}
		}
	}
}

func isEncrypted(err error) bool {
	var u *UnsupportedError
	return errors.As(err, &u) && u.Code == UnsupportedEncryption
}

func isInvalidFooter(err error) bool {
	var r *RefusalError
	return errors.As(err, &r) && r.Code == CodeInvalidFooter
}
