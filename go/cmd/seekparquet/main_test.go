// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// dataset writes a one-part dataset of n rows with keys in the order
// given, into a new temporary directory.
func dataset(t *testing.T, keys ...string) string {
	t.Helper()
	dir := t.TempDir()
	if err := testutil.Write(dir, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(keys...), PageBytes: 128}}}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func sorted(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%04d", i)
	}
	return keys
}

func TestValidateCommand(t *testing.T) {
	valid := dataset(t, sorted(100)...)
	unordered := sorted(100)
	unordered[40], unordered[41] = unordered[41], unordered[40]
	invalid := dataset(t, unordered...)
	lzo := dataset(t, sorted(100)...)
	part := filepath.Join(lzo, filepath.FromSlash(testutil.PartPath(0)))
	if err := testutil.RewriteFooter(part, func(m *format.FileMetaData) error {
		m.RowGroups[0].Columns[1].MetaData.Codec = format.LZO
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := testutil.SyncSizes(lzo); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		args   []string
		status int
		out    []string
	}{
		{"valid", []string{"validate", valid}, 0, []string{"structural  complete", "profile     complete", "full        complete", "result: complete"}},
		{"structural", []string{"validate", "--level", "structural", valid}, 0, []string{"structural  complete", "result: complete"}},
		{"digest", []string{"validate", "--level", "digest", valid}, 0, []string{"digest      complete", "nothing checked"}},
		{"failed", []string{"validate", invalid}, 1, []string{"profile     failed      key_order at part data/part-00000.parquet row group 0 column key page", "row 41", "full        incomplete"}},
		{"incomplete", []string{"validate", lzo}, 2, []string{"full        incomplete  unsupported", "result: incomplete"}},
		{"no dataset", []string{"validate", t.TempDir()}, 1, []string{"no_manifest"}},
		{"missing directory", []string{"validate", filepath.Join(t.TempDir(), "absent")}, 2, nil},
		{"no subcommand", nil, 2, nil},
		{"unknown level", []string{"validate", "--level", "deep", valid}, 2, nil},
		{"two directories", []string{"validate", valid, valid}, 2, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if status := run(context.Background(), c.args, &stdout, &stderr); status != c.status {
				t.Fatalf("status %d, want %d\nstdout:\n%s\nstderr:\n%s", status, c.status, stdout.String(), stderr.String())
			}
			for _, want := range c.out {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout lacks %q:\n%s", want, stdout.String())
				}
			}
		})
	}
}

func TestValidateCommandJSON(t *testing.T) {
	unordered := sorted(100)
	unordered[40], unordered[41] = unordered[41], unordered[40]
	dir := dataset(t, unordered...)
	var stdout, stderr bytes.Buffer
	if status := run(context.Background(), []string{"validate", "--json", dir}, &stdout, &stderr); status != 1 {
		t.Fatalf("status %d: %s", status, stderr.String())
	}
	var report struct {
		Result string `json:"result"`
		Levels []struct {
			Level    string `json:"level"`
			Result   string `json:"result"`
			Code     string `json:"code"`
			Reason   string `json:"reason"`
			Location *struct {
				Part     string `json:"part"`
				RowGroup *int   `json:"row_group"`
				Column   string `json:"column"`
				Page     *int   `json:"page"`
				Row      *int64 `json:"row"`
			} `json:"location"`
			Checked struct {
				Rows int64 `json:"rows"`
			} `json:"checked"`
		} `json:"levels"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("%v:\n%s", err, stdout.String())
	}
	if report.Result != "failed" || len(report.Levels) != 3 {
		t.Fatalf("report: %s", stdout.String())
	}
	profile := report.Levels[1]
	if profile.Code != "key_order" || profile.Location == nil || profile.Location.Column != "key" ||
		profile.Location.Page == nil || profile.Location.Row == nil || *profile.Location.Row != 41 {
		t.Fatalf("profile: %s", stdout.String())
	}
	if report.Levels[0].Result != "complete" || report.Levels[2].Result != "incomplete" || report.Levels[2].Reason == "" {
		t.Fatalf("levels: %s", stdout.String())
	}
}
