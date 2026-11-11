// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"errors"
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
		f, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &tc.lim, &st, tc.window)
		must(t, err)
		if st.SourceReads != tc.reads || st.SourceBytes != tc.bytes {
			t.Errorf("%s: %d reads of %d bytes, want %d of %d", tc.name, st.SourceReads, st.SourceBytes, tc.reads, tc.bytes)
		}
		exact, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &lim, nil, 0)
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
			_, err := readFooter(ctx, "p", bytes.NewReader(tc.raw), int64(len(tc.raw)), &tc.lim, &st, footerWindow)
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

func isEncrypted(err error) bool {
	var u *UnsupportedError
	return errors.As(err, &u) && u.Code == UnsupportedEncryption
}

func isInvalidFooter(err error) bool {
	var r *RefusalError
	return errors.As(err, &r) && r.Code == CodeInvalidFooter
}
