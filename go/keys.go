// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "bytes"

// Successor returns successor(prefix) (spec §1): the least byte string
// greater than every string that begins with prefix, which is prefix
// without its trailing 0xFF bytes and with its last remaining byte
// incremented. A prefix that is empty or only 0xFF bytes has none (ok is
// false): the keys that begin with it are unbounded above. next is a new
// slice.
func Successor(prefix []byte) (next []byte, ok bool) {
	last := len(prefix) - 1
	for last >= 0 && prefix[last] == 0xff {
		last--
	}
	if last < 0 {
		return nil, false
	}
	next = append([]byte(nil), prefix[:last+1]...)
	next[last]++
	return next, true
}

// CommonPrefix returns the common prefix key rolls up to in a listing of
// prefix with the delimiter byte delim (LIST-2): key up to and including
// the first delim after len(prefix). It returns nil when key does not
// begin with prefix, or has no delim after it (key is then a row of the
// listing, LIST-3). The result aliases key.
func CommonPrefix(key, prefix []byte, delim byte) []byte {
	if !bytes.HasPrefix(key, prefix) {
		return nil
	}
	if i := bytes.IndexByte(key[len(prefix):], delim); i >= 0 {
		return key[:len(prefix)+i+1 : len(prefix)+i+1]
	}
	return nil
}
