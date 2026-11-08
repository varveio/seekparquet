// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command seekparquet works with local SeekParquet datasets. Its one
// subcommand validates a dataset at one of the specification's levels
// (§8) and reports each level it ran:
//
//	seekparquet validate [--level structural|profile|full|digest] [--json] DIR
//
// The level defaults to full, which runs structural and profile first;
// digest runs alone. The report goes to stdout, as text or, with --json,
// as one JSON object. The exit status is 0 when every level completed, 1
// when a level failed (the dataset breaks the specification), and 2 when a
// level is incomplete (an unsupported feature, a limit or a storage error,
// which say nothing about validity), when validation could not run, or on
// a usage error.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/varveio/seekparquet/go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

const usage = "usage: seekparquet validate [--level structural|profile|full|digest] [--json] DIR"

// run runs the command with args and returns its exit status.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "validate" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	level := flags.String("level", string(seekparquet.LevelFull), "the level to reach: structural, profile, full or digest")
	asJSON := flags.Bool("json", false, "write the report as JSON")
	flags.Usage = func() {
		fmt.Fprintln(stderr, usage)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	switch seekparquet.Level(*level) {
	case seekparquet.LevelStructural, seekparquet.LevelProfile, seekparquet.LevelFull, seekparquet.LevelDigest:
	default:
		fmt.Fprintf(stderr, "seekparquet: unknown level %q\n%s\n", *level, usage)
		return 2
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	dir := flags.Arg(0)
	report, err := seekparquet.ValidateDir(ctx, dir, seekparquet.ValidateOptions{Level: seekparquet.Level(*level)})
	if err != nil {
		fmt.Fprintln(stderr, "seekparquet:", err)
		return 2
	}
	if *asJSON {
		err = writeJSON(stdout, dir, report)
	} else {
		err = writeText(stdout, dir, report)
	}
	if err != nil {
		fmt.Fprintln(stderr, "seekparquet:", err)
		return 2
	}
	switch report.Result() {
	case seekparquet.Complete:
		return 0
	case seekparquet.Failed:
		return 1
	}
	return 2
}

// writeText writes one line per level, then the result.
func writeText(w io.Writer, dir string, report *seekparquet.Report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "dataset %s\n", dir)
	for _, lr := range report.Levels {
		fmt.Fprintf(&b, "%-10s  %-10s  ", lr.Level, lr.Result)
		switch lr.Result {
		case seekparquet.Failed:
			b.WriteString(string(lr.Failure.Code))
			if at := location(lr); at != "" {
				b.WriteString(" at " + at)
			}
			if lr.Failure.Detail != "" {
				b.WriteString(": " + lr.Failure.Detail)
			}
		case seekparquet.Incomplete:
			b.WriteString(lr.Reason)
			if at := location(lr); at != "" && lr.Err != nil {
				b.WriteString(" (at " + at + ")")
			}
		default:
			b.WriteString(checked(lr))
			if lr.Reason != "" {
				b.WriteString("; " + lr.Reason)
			}
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "result: %s\n", report.Result())
	_, err := io.WriteString(w, b.String())
	return err
}

// location spells where a level stopped, with the row of a failure.
func location(lr seekparquet.LevelReport) string {
	at := lr.At.String()
	if lr.Failure != nil && lr.Failure.Row >= 0 {
		at = strings.TrimSpace(at + fmt.Sprintf(" row %d", lr.Failure.Row))
	}
	return at
}

// checked summarises what a complete level checked.
func checked(lr seekparquet.LevelReport) string {
	c := lr.Checked
	switch lr.Level {
	case seekparquet.LevelStructural:
		return fmt.Sprintf("%d parts, %d row groups, %d key pages located", c.Parts, c.RowGroups, c.Pages)
	case seekparquet.LevelProfile:
		return fmt.Sprintf("%d key column chunks, %d pages, %d keys", c.Chunks, c.Pages, c.Rows)
	case seekparquet.LevelFull:
		return fmt.Sprintf("%d other column chunks, %d pages, %d rows", c.Chunks, c.Pages, c.Rows)
	}
	return fmt.Sprintf("%d digests", c.Digests)
}

// jsonLevel is one level of the JSON report.
type jsonLevel struct {
	Level    string        `json:"level"`
	Result   string        `json:"result"`
	Checked  jsonChecked   `json:"checked"`
	Code     string        `json:"code,omitempty"`
	Location *jsonLocation `json:"location,omitempty"`
	Detail   string        `json:"detail,omitempty"`
	Reason   string        `json:"reason,omitempty"`
}

type jsonChecked struct {
	Parts     int64 `json:"parts"`
	RowGroups int64 `json:"row_groups"`
	Chunks    int64 `json:"chunks"`
	Pages     int64 `json:"pages"`
	Rows      int64 `json:"rows"`
	Digests   int64 `json:"digests"`
}

type jsonLocation struct {
	Part     string `json:"part,omitempty"`
	RowGroup *int   `json:"row_group,omitempty"`
	Column   string `json:"column,omitempty"`
	Page     *int   `json:"page,omitempty"`
	Row      *int64 `json:"row,omitempty"`
}

// writeJSON writes the report as one JSON object.
func writeJSON(w io.Writer, dir string, report *seekparquet.Report) error {
	out := struct {
		Dataset string      `json:"dataset"`
		Result  string      `json:"result"`
		Levels  []jsonLevel `json:"levels"`
	}{Dataset: dir, Result: string(report.Result()), Levels: []jsonLevel{}}
	for _, lr := range report.Levels {
		c := lr.Checked
		l := jsonLevel{Level: string(lr.Level), Result: string(lr.Result), Reason: lr.Reason,
			Checked: jsonChecked{c.Parts, c.RowGroups, c.Chunks, c.Pages, c.Rows, c.Digests}}
		if lr.Failure != nil {
			l.Code, l.Detail = string(lr.Failure.Code), lr.Failure.Detail
		}
		if lr.Result != seekparquet.Complete && lr.At.Part != "" {
			loc := &jsonLocation{Part: lr.At.Part, Column: lr.At.Column}
			if lr.At.RowGroup >= 0 {
				loc.RowGroup = &lr.At.RowGroup
			}
			if lr.At.Page >= 0 {
				loc.Page = &lr.At.Page
			}
			if lr.Failure != nil && lr.Failure.Row >= 0 {
				loc.Row = &lr.Failure.Row
			}
			l.Location = loc
		}
		out.Levels = append(out.Levels, l)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
