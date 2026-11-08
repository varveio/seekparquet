// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command samples writes the sample datasets of internal/samples through
// local.WriteDir, one directory each under the directory it is given,
// which must not exist: for checks by other implementations
// (conformance/harness/check_datasets.py).
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/varveio/seekparquet/go/internal/samples"
	"github.com/varveio/seekparquet/go/local"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: samples DIR")
		os.Exit(2)
	}
	out := os.Args[1]
	if err := os.Mkdir(out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, s := range samples.All() {
		if err := local.WriteDir(context.Background(), filepath.Join(out, s.Name), s.Schema, s.Reader(), s.Options); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", s.Name, err)
			os.Exit(1)
		}
	}
}
