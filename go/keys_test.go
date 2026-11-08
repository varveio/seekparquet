// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"testing"

	"github.com/varveio/seekparquet/go"
)

func TestSuccessorAndCommonPrefix(t *testing.T) {
	for in, want := range map[string]string{"a": "b", "a/": "a0", "a\xff": "b", "a\xfe\xff\xff": "a\xff", "\x00": "\x01"} {
		got, ok := seekparquet.Successor([]byte(in))
		if !ok || string(got) != want {
			t.Fatalf("Successor(%q) = %q, %v", in, got, ok)
		}
	}
	for _, in := range []string{"", "\xff", "\xff\xff"} {
		if _, ok := seekparquet.Successor([]byte(in)); ok {
			t.Fatalf("Successor(%q) must have none", in)
		}
	}
	in := []byte("ab")
	next, _ := seekparquet.Successor(in)
	next[0] = 'z'
	if string(in) != "ab" {
		t.Fatal("Successor aliases its input")
	}
	for _, c := range []struct {
		key, prefix string
		delim       byte
		want        string // "-" for none
	}{
		{"a/b/c", "a/", '/', "a/b/"},
		{"a/b", "a/", '/', "-"},
		{"a/", "a/", '/', "-"},    // a key equal to the prefix is a row
		{"a//", "a/", '/', "a//"}, // repeated delimiters
		{"b/c", "a/", '/', "-"},   // not under the prefix
		{"/x", "", '/', "/"},      // a delimiter first
		{"a\xffb\xffc", "a", 0xff, "a\xff"},
		{"a", "ab", '/', "-"},
	} {
		got := seekparquet.CommonPrefix([]byte(c.key), []byte(c.prefix), c.delim)
		if c.want == "-" && got != nil || c.want != "-" && string(got) != c.want {
			t.Fatalf("CommonPrefix(%q, %q, %q) = %q, want %s", c.key, c.prefix, c.delim, got, c.want)
		}
	}
}

// FuzzSuccessor checks Successor against the definition rather than an
// algorithm: the successor of p does not begin with p, is above every
// string that begins with p, and every string t with p <= t < successor
// begins with p; and p has none exactly when it is empty or all 0xFF.
func FuzzSuccessor(f *testing.F) {
	for _, seed := range [][2]string{{"a", "a\xff"}, {"", "x"}, {"\xff\xff", "\xff"}, {"a\xfe\xff", "a\xff"}, {"ab", "ab\x00"}, {"\x80", "0"}} {
		f.Add([]byte(seed[0]), []byte(seed[1]))
	}
	f.Fuzz(func(t *testing.T, p, probe []byte) {
		s, ok := seekparquet.Successor(p)
		allFF := true
		for _, c := range p {
			allFF = allFF && c == 0xff
		}
		if ok == allFF {
			t.Fatalf("Successor(%x) ok=%v", p, ok)
		}
		if !ok {
			return
		}
		if bytes.HasPrefix(s, p) || bytes.Compare(s, p) <= 0 {
			t.Fatalf("Successor(%x) = %x is not above the prefix range", p, s)
		}
		long := append(bytes.Clone(p), bytes.Repeat([]byte{0xff}, 16)...)
		if bytes.Compare(long, s) >= 0 {
			t.Fatalf("Successor(%x) = %x is not above %x", p, s, long)
		}
		withProbe := append(bytes.Clone(p), probe...)
		if bytes.Compare(withProbe, s) >= 0 {
			t.Fatalf("Successor(%x) = %x is not above %x", p, s, withProbe)
		}
		if bytes.Compare(p, probe) <= 0 && bytes.Compare(probe, s) < 0 && !bytes.HasPrefix(probe, p) {
			t.Fatalf("%x lies between %x and its successor %x without beginning with it", probe, p, s)
		}
	})
}

// commonPrefixOracle is LIST-2 spelled out byte by byte.
func commonPrefixOracle(key, prefix []byte, delim byte) []byte {
	if len(key) < len(prefix) {
		return nil
	}
	for i := range prefix {
		if key[i] != prefix[i] {
			return nil
		}
	}
	for i := len(prefix); i < len(key); i++ {
		if key[i] == delim {
			return key[:i+1]
		}
	}
	return nil
}

func FuzzCommonPrefix(f *testing.F) {
	f.Add([]byte("a/b/c"), []byte("a/"), byte('/'))
	f.Add([]byte("a/"), []byte("a/"), byte('/'))
	f.Add([]byte("\xff\xff"), []byte(""), byte(0xff))
	f.Fuzz(func(t *testing.T, key, prefix []byte, delim byte) {
		got := seekparquet.CommonPrefix(key, prefix, delim)
		want := commonPrefixOracle(key, prefix, delim)
		if (got == nil) != (want == nil) || !bytes.Equal(got, want) {
			t.Fatalf("CommonPrefix(%x, %x, %x) = %x, want %x", key, prefix, delim, got, want)
		}
	})
}
