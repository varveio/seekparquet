// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// corpus is the conformance suite's fixture directory.
var corpus = filepath.Join("..", "conformance")

// TestValidateCorpus validates every fixture dataset of the conformance
// suite: each valid one completes at the full level, and each invalid one
// fails at the lowest level its fault declares (generators/faults.json),
// with one of its codes, every level below completing; a fault whose
// outcome is unsupported (an encrypted part) leaves that level incomplete
// with its unsupported code.
func TestValidateCorpus(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join(corpus, "datasets"))
	must(t, err)
	valid := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "invalid" {
			continue
		}
		valid++
		t.Run("valid/"+e.Name(), func(t *testing.T) {
			report, err := ValidateDir(ctx, filepath.Join(corpus, "datasets", e.Name()), ValidateOptions{})
			must(t, err)
			if report.Result() != Complete {
				t.Fatalf("got %s", describeReport(report))
			}
		})
	}
	if valid != 26 {
		t.Errorf("%d valid datasets, want 26", valid)
	}
	raw, err := os.ReadFile(filepath.Join(corpus, "generators", "faults.json"))
	must(t, err)
	var faults struct {
		Faults []struct {
			ID              string   `json:"id"`
			Dataset         string   `json:"dataset"`
			ValidationLevel Level    `json:"validation_level"`
			Outcome         string   `json:"outcome"`
			Codes           []string `json:"codes"`
			Secondary       []struct {
				Code string `json:"code"`
			} `json:"secondary_faults"`
		} `json:"faults"`
	}
	must(t, json.Unmarshal(raw, &faults))
	if len(faults.Faults) != 145 {
		t.Errorf("%d invalid datasets, want 145", len(faults.Faults))
	}
	for _, f := range faults.Faults {
		t.Run("invalid/"+f.ID, func(t *testing.T) {
			report, err := ValidateDir(ctx, filepath.Join(corpus, filepath.FromSlash(f.Dataset)), ValidateOptions{Level: f.ValidationLevel})
			must(t, err)
			codes := f.Codes
			for _, s := range f.Secondary {
				codes = append(codes, s.Code)
			}
			for _, lr := range report.Levels {
				var unsupported *UnsupportedError
				if lr.Level == f.ValidationLevel && f.Outcome == "unsupported" {
					if lr.Result != Incomplete || !errors.As(lr.Err, &unsupported) || string(unsupported.Code) != codes[0] {
						t.Fatalf("want %s incomplete with %v, got %s", f.ValidationLevel, codes, describeReport(report))
					}
				} else if lr.Level == f.ValidationLevel {
					if lr.Result != Failed || !slices.Contains(codes, string(lr.Failure.Code)) {
						t.Fatalf("want %s failed with %v, got %s", f.ValidationLevel, codes, describeReport(report))
					}
				} else if lr.Result != Complete {
					t.Fatalf("want every level below %s complete, got %s", f.ValidationLevel, describeReport(report))
				}
			}
		})
	}
}

// describeReport spells a report for failure messages.
func describeReport(r *Report) string {
	out := ""
	for _, lr := range r.Levels {
		out += string(lr.Level) + "=" + string(lr.Result)
		switch {
		case lr.Failure != nil:
			out += "(" + lr.Failure.Error() + " at " + lr.At.String() + ")"
		case lr.Reason != "":
			out += "(" + lr.Reason + ")"
		}
		out += " "
	}
	return out
}
