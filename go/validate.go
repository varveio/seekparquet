// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Level is a validation level of the specification (§8).
type Level string

// The validation levels. Structural, profile and full each require the
// level before them; digest stands alone (VAL-1).
const (
	// LevelStructural checks the manifest and every part, as the first
	// read to touch each part does (CHECK-1), and RG-2 to
	// RG-4 for every non-empty row group: the key column's ColumnIndex and
	// OffsetIndex, and the page header each OffsetIndex entry locates,
	// reading page headers but not page data.
	LevelStructural Level = "structural"
	// LevelProfile enumerates the pages of every key column chunk from the
	// chunk's start, without its OffsetIndex, and requires the OffsetIndex
	// to list exactly those pages (RG-3); decodes every key and checks
	// KEY-2, KEY-3 and KEY-6 over every row and RG-5 for every page; and
	// requires each part's first and last keys to equal its manifest bounds
	// (PART-3, PART-4).
	LevelProfile Level = "profile"
	// LevelFull enumerates and decodes every page of every other stored
	// column the same way: each page decompresses, passes its CRC when its
	// header carries one, decodes to exactly the values its header
	// declares, and the chunk holds exactly the row group's rows;
	// a column's OffsetIndex and ColumnIndex, when present, must describe
	// exactly those pages.
	LevelFull Level = "full"
	// LevelDigest compares the SHA-256 of every part that declares one with
	// its manifest entry (PART-5), reading the parts as bytes, whether or
	// not they open as Parquet.
	LevelDigest Level = "digest"
)

// Result is the outcome of one level (VAL-1).
type Result string

// The outcomes of a level.
const (
	// Complete: the level checked everything it covers and found no fault.
	Complete Result = "complete"
	// Failed: the dataset breaks the specification; the level's Failure
	// carries the refusal code.
	Failed Result = "failed"
	// Incomplete: the level could not check everything, for a reason that
	// is not a fault of the data (an unsupported feature, a limit, a
	// storage error) or because its prerequisite did not complete. It says
	// nothing about validity.
	Incomplete Result = "incomplete"
)

// ValidateOptions configures Validate.
type ValidateOptions struct {
	// Level is the level to reach; "" is LevelFull. Structural, profile
	// and full run the levels they require first; digest runs alone.
	Level Level
	// Limits bounds what the dataset can make the validator allocate, as
	// it bounds a reader (Options.Limits). Exceeding one leaves the level
	// incomplete; it is not a fault of the data. IndexMemory does not
	// apply: the validator keeps no page index after it checks it.
	Limits Limits
}

// Location is where a level found a fault or stopped: a part, a row group
// of it, a column chunk of that, and a data page in the chunk, numbered
// from 0 in file order (the OffsetIndex's numbering). Fields that do not
// apply are "" or -1.
type Location struct {
	Part     string
	RowGroup int
	Column   string
	Page     int
}

// Checked counts what a level checked, up to its end or the point where
// it stopped.
type Checked struct {
	// Parts is the number of parts checked.
	Parts int64
	// RowGroups is the number of non-empty row groups checked.
	RowGroups int64
	// Chunks is the number of column chunks whose pages were enumerated
	// (profile: key columns; full: the other columns).
	Chunks int64
	// Pages is the number of data pages checked: located through the
	// OffsetIndex (structural), or enumerated and decoded (profile, full).
	Pages int64
	// Rows is the number of rows checked: keys (profile), or rows whose
	// every other column was decoded (full).
	Rows int64
	// Digests is the number of SHA-256 digests compared (digest).
	Digests int64
}

// LevelReport is the outcome of one level.
type LevelReport struct {
	Level  Level
	Result Result
	// Checked counts what the level checked.
	Checked Checked
	// At locates a failure, or where an incomplete level stopped, when
	// the level got that far.
	At Location
	// Failure is the refusal, with its Code, when Result is Failed.
	Failure *RefusalError
	// Reason says, for people, why an incomplete level is incomplete, or
	// that a complete digest level compared no digest (VAL-2).
	Reason string
	// Err is the cause of an incomplete level: an *UnsupportedError, an
	// error wrapping ErrLimitExceeded, or an error of the storage. It is
	// nil when the level is incomplete because its prerequisite is.
	Err error
}

// Report is the outcome of a validation: one LevelReport per level
// attempted, in order.
type Report struct {
	Levels []LevelReport
}

// Level returns the report of level l and whether it was attempted.
func (r *Report) Level(l Level) (LevelReport, bool) {
	for _, lr := range r.Levels {
		if lr.Level == l {
			return lr, true
		}
	}
	return LevelReport{}, false
}

// Result returns Failed when any level failed, else Incomplete when any
// level is incomplete, else Complete: the dataset is valid at the level
// requested.
func (r *Report) Result() Result {
	out := Complete
	for _, lr := range r.Levels {
		switch lr.Result {
		case Failed:
			return Failed
		case Incomplete:
			out = Incomplete
		}
	}
	return out
}

// ValidateDir validates the dataset in the local directory at path, read
// through an os.Root of it as OpenDir reads it. An error is returned only
// when the directory cannot be opened, or as Validate returns one.
func ValidateDir(ctx context.Context, path string, opt ValidateOptions) (*Report, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return Validate(ctx, dirSource{root}, opt)
}

// Validate validates the dataset at src up to opt.Level (§8). It never
// lets an index it checks decide which pages or rows it visits: every
// chunk's pages are enumerated from the chunk's start, and the indexes are
// compared with what was found. A fault in the data fails the level that
// detects it, and leaves the levels that require it incomplete; levels
// below keep their result. An unsupported feature, a limit or a storage
// error leaves the level incomplete, never failed (VAL-1).
//
// The error is non-nil only for an unknown level, or when ctx ends; the
// report then holds the levels that finished.
func Validate(ctx context.Context, src Source, opt ValidateOptions) (*Report, error) {
	opt.Limits = opt.Limits.withDefaults()
	var levels []Level
	switch opt.Level {
	case LevelStructural:
		levels = []Level{LevelStructural}
	case LevelProfile:
		levels = []Level{LevelStructural, LevelProfile}
	case LevelFull, "":
		levels = []Level{LevelStructural, LevelProfile, LevelFull}
	case LevelDigest:
		levels = []Level{LevelDigest}
	default:
		return nil, fmt.Errorf("seekparquet: unknown validation level %q", opt.Level)
	}
	v := &validator{ctx: ctx, src: src, limits: opt.Limits}
	defer v.close()
	report := &Report{}
	var previous *LevelReport
	for _, level := range levels {
		lr := LevelReport{Level: level, At: Location{RowGroup: -1, Page: -1}}
		if previous != nil && previous.Result != Complete {
			lr.Result, lr.Reason = Incomplete, fmt.Sprintf("its prerequisite %s is %s", previous.Level, previous.Result)
			lr.At = Location{RowGroup: -1, Page: -1}
		} else if err := v.run(&lr, v.check(level)); err != nil {
			return report, err
		}
		report.Levels = append(report.Levels, lr)
		previous = &report.Levels[len(report.Levels)-1]
	}
	return report, nil
}

// check returns the check of a level.
func (v *validator) check(level Level) func(*LevelReport) error {
	switch level {
	case LevelStructural:
		return v.structural
	case LevelProfile:
		return v.profile
	case LevelFull:
		return v.full
	}
	return v.digest
}

// run runs one level's check and records its outcome. It returns an error
// only when the context ended.
func (v *validator) run(lr *LevelReport, check func(*LevelReport) error) error {
	err := check(lr)
	var refusal *RefusalError
	switch {
	case err == nil:
		lr.Result, lr.At = Complete, Location{RowGroup: -1, Page: -1}
	case v.ctx.Err() != nil:
		return v.ctx.Err()
	case errors.As(err, &refusal):
		lr.Result, lr.Failure = Failed, refusal
		if lr.At.Part == "" || lr.At.Part != refusal.Part {
			lr.At = Location{Part: refusal.Part, RowGroup: refusal.RowGroup, Page: -1}
		}
	default:
		lr.Result, lr.Err, lr.Reason = Incomplete, err, incompleteReason(err)
	}
	return nil
}

// incompleteReason classifies the cause of an incomplete level.
func incompleteReason(err error) string {
	var unsupported *UnsupportedError
	switch {
	case errors.As(err, &unsupported):
		return "unsupported: " + err.Error()
	case errors.Is(err, ErrLimitExceeded):
		return "limit: " + err.Error()
	}
	return "storage: " + err.Error()
}

// String formats a location as "part P row group G column C page N",
// leaving out what does not apply.
func (l Location) String() string {
	var parts []string
	if l.Part != "" {
		parts = append(parts, "part "+l.Part)
	}
	if l.RowGroup >= 0 {
		parts = append(parts, "row group "+strconv.Itoa(l.RowGroup))
	}
	if l.Column != "" {
		parts = append(parts, "column "+l.Column)
	}
	if l.Page >= 0 {
		parts = append(parts, "page "+strconv.Itoa(l.Page))
	}
	return strings.Join(parts, " ")
}
