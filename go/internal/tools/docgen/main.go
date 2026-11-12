// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command docgen renders the generated parts of the documentation and
// checks that the committed files match them. It renders the reference
// pages under docs/reference (limits, read statistics, refusal codes, Go
// and Java names) from the code and docs/data, the generated regions of
// hand-written pages (<!-- generated: NAME --> to <!-- /generated -->),
// and the snippets they embed (<!-- snippet: FILE#NAME --> to
// <!-- /snippet -->): a Go function's body, or a region of another file
// between "tag::NAME[]" and "end::NAME[]" comments.
//
// From the repository root:
//
//	go -C go run ./internal/tools/docgen      # check; exit 1 on drift
//	go -C go run ./internal/tools/docgen -w   # rewrite what differs
//
// It also checks the data against the code: every Limits and ReadStats
// field, LimitName, refusal and unsupported code is described, and every
// Go and Java name the docs give exists. Java names are checked against
// java/ unless docs/data/parity.json marks them pending; -java DIR checks
// them against another checkout's java/ directory.
//
// See docs/dev/docs.md.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	write := flag.Bool("w", false, "write the files that differ instead of failing")
	root := flag.String("root", "", "the repository root (default: found from the working directory)")
	java := flag.String("java", "", "check Java names against this java/ directory, even while they are marked pending")
	flag.Parse()
	if err := run(*root, *java, *write); err != nil {
		fmt.Fprintln(os.Stderr, "docgen:", err)
		os.Exit(1)
	}
}

func run(root, java string, write bool) error {
	if root == "" {
		var err error
		if root, err = findRoot(); err != nil {
			return err
		}
	}
	g, err := newGenerator(root, java)
	if err != nil {
		return err
	}
	changed, err := g.run(write)
	if err != nil {
		return err
	}
	for _, f := range changed {
		if write {
			fmt.Println("wrote", f)
		} else {
			fmt.Fprintln(os.Stderr, "out of date:", f)
		}
	}
	if !write && len(changed) > 0 {
		return errors.New("generated documentation is out of date: run go -C go run ./internal/tools/docgen -w")
	}
	return nil
}

// findRoot returns the nearest directory at or above the working
// directory that holds spec/status.json.
func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "spec", "status.json")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("no repository root (spec/status.json) at or above the working directory")
		}
		dir = parent
	}
}
