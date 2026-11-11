// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"os"
	"strconv"
	"testing"
)

// SEEKPARQUET_TEST_READAHEAD=n runs every listing of the package's tests
// that asks for no read-ahead with n slots (testListReadAhead), so that the
// whole suite can be run with read-ahead on. Tests that count reads then
// see the read-ahead's reads too.
func init() {
	if n, err := strconv.Atoi(os.Getenv("SEEKPARQUET_TEST_READAHEAD")); err == nil && n > 0 {
		testListReadAhead = n
	}
}

// noTestReadAhead makes a listing that asks for no read-ahead read none
// for the rest of t, whatever SEEKPARQUET_TEST_READAHEAD says: for the
// tests that compare listings with and without it. It changes nothing
// when the variable is not set, so that it races with no parallel test.
func noTestReadAhead(t testing.TB) {
	if saved := testListReadAhead; saved != 0 {
		testListReadAhead = 0
		t.Cleanup(func() { testListReadAhead = saved })
	}
}

// NoTestReadAhead is noTestReadAhead for the package's external tests.
var NoTestReadAhead = noTestReadAhead
