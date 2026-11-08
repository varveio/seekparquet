// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
)

func run(t *testing.T, lines ...string) []map[string]any {
	t.Helper()
	a := &adapter{open: map[string]*seekparquet.Dataset{}}
	defer a.closeAll()
	var out bytes.Buffer
	if err := a.serve(context.Background(), strings.NewReader(strings.Join(lines, "\n")+"\n"), &out); err != nil {
		t.Fatal(err)
	}
	var resps []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var r map[string]any
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		resps = append(resps, r)
	}
	if len(resps) != len(lines) {
		t.Fatalf("%d responses to %d requests", len(resps), len(lines))
	}
	return resps
}

func TestAdapter(t *testing.T) {
	dir := t.TempDir()
	if err := seekparquettest.Write(dir, seekparquettest.Dataset{Parts: []seekparquettest.Part{
		{Rows: seekparquettest.Rows("a", "a/b", "a/c/d")},
	}}); err != nil {
		t.Fatal(err)
	}
	refused := t.TempDir()
	if err := os.WriteFile(filepath.Join(refused, "manifest.json"), []byte(`{"format":"parquet"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	ds := `"dataset":` + quote(dir)
	resps := run(t,
		`{"id":"1","op":"hello"}`,
		`{"id":"2","op":"open",`+ds+`}`,
		`{"id":"3","op":"range",`+ds+`,"limit":10,"columns":["size","name","nope"]}`,
		`{"id":"4","op":"range",`+ds+`,"start_base64":"YQ==","start_exclusive":true,"end_base64":null,"limit":1}`,
		`{"id":"5","op":"list",`+ds+`,"prefix_base64":"YQ==","delimiter_base64":"Lw==","limit":10}`,
		`{"id":"6","op":"range",`+ds+`,"limit":1.5}`,
		`{"id":"7","op":"range",`+ds+`,"limit":0}`,
		`{"id":"8","op":"range",`+ds+`,"limit":1,"start_base64":"!!"}`,
		`{"id":"9","op":"list",`+ds+`,"delimiter_base64":"Ly8=","limit":1}`,
		`{"id":"10","op":"open","dataset":`+quote(refused)+`}`,
		`{"id":"11","op":"validate",`+ds+`,"level":"profile"}`,
		`{"id":"12","op":"range",`+ds+`,"end_base64":"","limit":5}`,
		`not json`,
	)
	hello := resps[0]
	if hello["id"] != "1" || hello["status"] != "ok" || hello["implementation"] != "seekparquet-go" ||
		jsonString(hello["targets"]) != `["reader"]` || jsonString(hello["capabilities"]) != `["open","range","list","projection"]` ||
		jsonString(hello["supports"]) != `[{"draft_revision":1,"format_version":0}]` {
		t.Fatalf("hello %v", hello)
	}
	if resps[1]["status"] != "ok" {
		t.Fatalf("open %v", resps[1])
	}
	want3 := `[{"key_base64":"YQ==","kind":"row","values":[{"int":"0"},{"bytes_base64":"bmFtZS0w"},{"absent":true}]},` +
		`{"key_base64":"YS9i","kind":"row","values":[{"int":"1"},{"bytes_base64":"bmFtZS0x"},{"absent":true}]},` +
		`{"key_base64":"YS9jL2Q=","kind":"row","values":[{"int":"2"},{"null":true},{"absent":true}]}]`
	if got := jsonString(resps[2]["entries"]); got != want3 {
		t.Fatalf("range:\n got %s\nwant %s", got, want3)
	}
	if got := jsonString(resps[3]["entries"]); got != `[{"key_base64":"YS9i","kind":"row","values":[]}]` {
		t.Fatalf("exclusive start: %s", got)
	}
	if got := jsonString(resps[4]["entries"]); got != `[{"key_base64":"YS9i","kind":"row","values":[]},{"key_base64":"YS9jLw==","kind":"prefix"}]` {
		t.Fatalf("list: %s", got)
	}
	for _, i := range []int{5, 6, 7, 8, 12} {
		if resps[i]["status"] != "invalid_request" {
			t.Fatalf("request %d: %v", i+1, resps[i])
		}
	}
	if resps[9]["status"] != "refused" || resps[9]["code"] != "invalid_manifest" {
		t.Fatalf("refused open: %v", resps[9])
	}
	if resps[10]["status"] != "unsupported" {
		t.Fatalf("validate: %v", resps[10])
	}
	if resps[11]["status"] != "ok" || jsonString(resps[11]["entries"]) != "[]" {
		t.Fatalf("empty end: %v", resps[11])
	}
}

func quote(s string) string { b, _ := json.Marshal(s); return string(b) }

func jsonString(v any) string { b, _ := json.Marshal(v); return string(b) }
