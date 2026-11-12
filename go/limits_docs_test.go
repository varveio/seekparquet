// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// limitsDocRow is one row of the settings table of
// docs/reference/limits.md: Go field, Java method, default, kind.
var limitsDocRow = regexp.MustCompile("(?m)^\\| \\[`(\\w+)`\\]\\(#\\w+\\) \\| (?:`(\\w+)`|Go only) \\| ([^|]+) \\| ([^|]+) \\|$")

// limitsDocAmount is the amount at the end of a default cell.
var limitsDocAmount = regexp.MustCompile(`([0-9][0-9,]*)(?: (KiB|MiB|GiB))?(?: at their defaults)?\s*$`)

// parseDocAmount reads "64 MiB", "1,000,000" or "...: 832 MiB at their
// defaults" as a number.
func parseDocAmount(t *testing.T, cell string) int64 {
	t.Helper()
	m := limitsDocAmount.FindStringSubmatch(strings.TrimSpace(cell))
	if m == nil {
		t.Fatalf("no amount in %q", cell)
	}
	n, err := strconv.ParseInt(strings.ReplaceAll(m[1], ",", ""), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n << map[string]uint{"": 0, "KiB": 10, "MiB": 20, "GiB": 30}[m[2]]
}

// TestLimitsDoc checks the defaults docs/reference/limits.md states (which
// go/internal/tools/docgen renders from this package's source) against the
// defaults withDefaults actually sets, derived ones included, and that the
// table lists every field. Java's LimitsDocTest checks the same page.
func TestLimitsDoc(t *testing.T) {
	raw, err := os.ReadFile("../docs/reference/limits.md")
	if err != nil {
		t.Fatal(err)
	}
	defaults := reflect.ValueOf(Limits{}.withDefaults())
	rows := limitsDocRow.FindAllStringSubmatch(string(raw), -1)
	if len(rows) != defaults.NumField() {
		t.Fatalf("limits.md lists %d settings, Limits has %d fields", len(rows), defaults.NumField())
	}
	for _, row := range rows {
		field := defaults.FieldByName(row[1])
		if !field.IsValid() {
			t.Errorf("limits.md lists %s, which Limits does not have", row[1])
			continue
		}
		if got, want := parseDocAmount(t, row[3]), field.Int(); got != want {
			t.Errorf("limits.md states %s defaults to %d, withDefaults sets %d", row[1], got, want)
		}
	}
}
