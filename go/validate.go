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

// ValidationLevel is a validation level of the specification (§8).
type ValidationLevel string

// The validation levels. Structural, profile and full each require the
// level before them; digest stands alone (VAL-1).
const (
	// LevelStructural checks the manifest and every part, as the first
	// read to touch each part does (CHECK-1), and RG-2 to
	// RG-4 for every non-empty row group: the key column's ColumnIndex and
	// OffsetIndex, and the page header each OffsetIndex entry locates,
	// reading page headers but not page data.
	LevelStructural ValidationLevel = "structural"
	// LevelProfile enumerates the pages of every key column chunk from the
	// chunk's start, without its OffsetIndex, and requires the OffsetIndex
	// to list exactly those pages (RG-3); decodes every key and checks
	// KEY-2, KEY-3 and KEY-6 over every row and RG-5 for every page; and
	// requires each part's first and last keys to equal its manifest bounds
	// (PART-3, PART-4).
	LevelProfile ValidationLevel = "profile"
	// LevelFull enumerates and decodes every page of every other stored
	// column the same way: each page decompresses, passes its CRC when its
	// header carries one, decodes to exactly the values its header
	// declares, and the chunk holds exactly the row group's rows;
	// a column's OffsetIndex and ColumnIndex, when present, must describe
	// exactly those pages.
	LevelFull ValidationLevel = "full"
	// LevelDigest compares the SHA-256 of every part that declares one with
	// its manifest entry (PART-5), reading the parts as bytes, whether or
	// not they open as Parquet.
	LevelDigest ValidationLevel = "digest"
)

// ValidationResult is the outcome of one level (VAL-1).
type ValidationResult string

// The outcomes of a level.
const (
	// ResultComplete: the level checked everything it covers and found no
	// fault.
	ResultComplete ValidationResult = "complete"
	// ResultFailed: the dataset breaks the specification; the level's
	// Failure carries the refusal code.
	ResultFailed ValidationResult = "failed"
	// ResultIncomplete: the level could not check everything, for a reason
	// that is not a fault of the data (an unsupported feature, a limit, a
	// storage error) or because its prerequisite did not complete. It says
	// nothing about validity.
	ResultIncomplete ValidationResult = "incomplete"
)

// ValidateOptions configures Validate.
type ValidateOptions struct {
	// Level is the level to reach; "" is LevelFull. Structural, profile
	// and full run the levels they require first; digest runs alone.
	Level ValidationLevel
	// Limits bounds what the dataset can make the validator allocate, as
	// it bounds a reader (OpenOptions.Limits). Exceeding one leaves the level
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

// ValidationCounts counts what a level checked, up to its end or the point
// where it stopped.
type ValidationCounts struct {
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
	Level  ValidationLevel
	Result ValidationResult
	// Checked counts what the level checked.
	Checked ValidationCounts
	// At locates a failure, or where an incomplete level stopped, when
	// the level got that far.
	At Location
	// Failure is the refusal, with its Code, when Result is ResultFailed.
	Failure *RefusalError
	// Reason says, for people, why an incomplete level is incomplete, or
	// that a complete digest level compared no digest (VAL-2).
	Reason string
	// Err is the cause of an incomplete level: an *UnsupportedError, an
	// error wrapping ErrLimitExceeded, or an error of the storage. It is
	// nil when the level is incomplete because its prerequisite is.
	Err error
}

// ValidationReport is the outcome of a validation: one LevelReport per
// level attempted, in order.
type ValidationReport struct {
	Levels []LevelReport
}

// Level returns the report of level l and whether it was attempted.
func (r *ValidationReport) Level(l ValidationLevel) (LevelReport, bool) {
	for _, lr := range r.Levels {
		if lr.Level == l {
			return lr, true
		}
	}
	return LevelReport{}, false
}

// Result returns ResultFailed when any level failed, else ResultIncomplete
// when any level is incomplete, else ResultComplete: the dataset is valid at the level
// requested.
func (r *ValidationReport) Result() ValidationResult {
	out := ResultComplete
	for _, lr := range r.Levels {
		switch lr.Result {
		case ResultFailed:
			return ResultFailed
		case ResultIncomplete:
			out = ResultIncomplete
		}
	}
	return out
}

// ValidateDir validates the dataset in the local directory at path, read
// through an os.Root of it as OpenDir reads it. An error is returned only
// when the directory cannot be opened, or as Validate returns one.
func ValidateDir(ctx context.Context, path string, opt ValidateOptions) (*ValidationReport, error) {
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
// The error is non-nil only for an unknown level or a negative limit, both
// ErrInvalidRequest and returned before any I/O, or when ctx ends; the
// report then holds the levels that finished.
func Validate(ctx context.Context, src Source, opt ValidateOptions) (*ValidationReport, error) {
	if err := opt.Limits.check(); err != nil {
		return nil, err
	}
	opt.Limits = opt.Limits.withDefaults()
	var levels []ValidationLevel
	switch opt.Level {
	case LevelStructural:
		levels = []ValidationLevel{LevelStructural}
	case LevelProfile:
		levels = []ValidationLevel{LevelStructural, LevelProfile}
	case LevelFull, "":
		levels = []ValidationLevel{LevelStructural, LevelProfile, LevelFull}
	case LevelDigest:
		levels = []ValidationLevel{LevelDigest}
	default:
		return nil, invalidRequest("unknown validation level %q", opt.Level)
	}
	v := &validator{ctx: ctx, src: src, limits: opt.Limits}
	defer v.close()
	report := &ValidationReport{}
	var previous *LevelReport
	for _, level := range levels {
		lr := LevelReport{Level: level, At: Location{RowGroup: -1, Page: -1}}
		if previous != nil && previous.Result != ResultComplete {
			lr.Result, lr.Reason = ResultIncomplete, fmt.Sprintf("its prerequisite %s is %s", previous.Level, previous.Result)
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
func (v *validator) check(level ValidationLevel) func(*LevelReport) error {
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
		lr.Result, lr.At = ResultComplete, Location{RowGroup: -1, Page: -1}
	case v.ctx.Err() != nil:
		return v.ctx.Err()
	case errors.As(err, &refusal):
		lr.Result, lr.Failure = ResultFailed, refusal
		if lr.At.Part == "" || lr.At.Part != refusal.Part {
			lr.At = Location{Part: refusal.Part, RowGroup: refusal.RowGroup, Page: -1}
		}
	default:
		lr.Result, lr.Err, lr.Reason = ResultIncomplete, err, incompleteReason(err)
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
