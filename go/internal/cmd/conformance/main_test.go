// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil"
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
	if err := testutil.Write(dir, testutil.Dataset{Parts: []testutil.Part{
		{Rows: testutil.Rows("a", "a/b", "a/c/d")},
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
		`{"id":"5","op":"list",`+ds+`,"prefix_base64":"YS8=","delimiter_base64":"Lw==","limit":10}`,
		`{"id":"6","op":"range",`+ds+`,"limit":1.5}`,
		`{"id":"7","op":"range",`+ds+`,"limit":0}`,
		`{"id":"8","op":"range",`+ds+`,"limit":1,"start_base64":"!!"}`,
		`{"id":"9","op":"list",`+ds+`,"delimiter_base64":"Ly8=","limit":1}`,
		`{"id":"10","op":"open","dataset":`+quote(refused)+`}`,
		`{"id":"11","op":"validate",`+ds+`,"level":"profile"}`,
		`{"id":"12","op":"range",`+ds+`,"end_base64":"","limit":5}`,
		`{"id":"13","op":"validate","dataset":`+quote(refused)+`,"level":"full"}`,
		`{"id":"14","op":"validate",`+ds+`,"level":"digest"}`,
		`{"id":"15","op":"validate",`+ds+`,"level":"deep"}`,
		`not json`,
		`{"id":"17","op":"verify",`+ds+`}`,
		`{"id":"18","op":"verify","dataset":`+quote(refused)+`}`,
	)
	hello := resps[0]
	if hello["id"] != "1" || hello["status"] != "ok" || hello["implementation"] != "seekparquet-go" ||
		jsonString(hello["targets"]) != `["reader","validator-structural","validator-profile","validator-full","validator-digest"]` ||
		jsonString(hello["capabilities"]) != `["open","range","list","projection","verify","validate:structural","validate:profile","validate:full","validate:digest"]` ||
		jsonString(hello["supports"]) != `[{"draft_revision":1,"format_version":0}]` {
		t.Fatalf("hello %v", hello)
	}
	if resps[1]["status"] != "ok" {
		t.Fatalf("open %v", resps[1])
	}
	want3 := `[{"key_base64":"YQ==","kind":"row","offset":0,"values":[{"int":"0"},{"bytes_base64":"bmFtZS0w"},{"absent":true}]},` +
		`{"key_base64":"YS9i","kind":"row","offset":0,"values":[{"int":"1"},{"bytes_base64":"bmFtZS0x"},{"absent":true}]},` +
		`{"key_base64":"YS9jL2Q=","kind":"row","offset":0,"values":[{"int":"2"},{"null":true},{"absent":true}]}]`
	if got := jsonString(resps[2]["entries"]); got != want3 {
		t.Fatalf("range:\n got %s\nwant %s", got, want3)
	}
	if got := jsonString(resps[3]["entries"]); got != `[{"key_base64":"YS9i","kind":"row","offset":0,"values":[]}]` {
		t.Fatalf("exclusive start: %s", got)
	}
	if got := jsonString(resps[4]["entries"]); got != `[{"key_base64":"YS9i","kind":"row","offset":0,"values":[]},{"key_base64":"YS9jLw==","kind":"prefix"}]` {
		t.Fatalf("list: %s", got)
	}
	for _, i := range []int{5, 6, 7, 8, 14, 15} {
		if resps[i]["status"] != "invalid_request" {
			t.Fatalf("request %d: %v", i+1, resps[i])
		}
	}
	if resps[16]["status"] != "ok" || resps[17]["status"] != "refused" || resps[17]["code"] != "invalid_manifest" {
		t.Fatalf("verify: %v, %v", resps[16], resps[17])
	}
	if resps[9]["status"] != "refused" || resps[9]["code"] != "invalid_manifest" {
		t.Fatalf("refused open: %v", resps[9])
	}
	if got := jsonString(resps[10]); got != `{"id":"11","levels":{"profile":{"result":"complete"},"structural":{"result":"complete"}},"status":"ok"}` {
		t.Fatalf("validate: %s", got)
	}
	if got := jsonString(resps[12]["levels"]); !strings.Contains(got, `"structural":{"code":"invalid_manifest"`) ||
		!strings.Contains(got, `"full":{"reason":"its prerequisite profile is incomplete","result":"incomplete"}`) {
		t.Fatalf("validate refused: %s", got)
	}
	if got := jsonString(resps[13]["levels"]); got != `{"digest":{"checked":0,"result":"complete"}}` {
		t.Fatalf("validate digest: %s", got)
	}
	if resps[11]["status"] != "ok" || jsonString(resps[11]["entries"]) != "[]" {
		t.Fatalf("empty end: %v", resps[11])
	}
}

// TestCursorField: the tagged cursor is parsed strictly; whether an
// operation accepts its kind is the library's check.
func TestCursorField(t *testing.T) {
	dir := t.TempDir()
	if err := testutil.Write(dir, testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{
		{Rows: testutil.Rows("a", "b", "b", "b", "c/d")},
	}}); err != nil {
		t.Fatal(err)
	}
	ds := `"dataset":` + quote(dir)
	cases := []struct {
		after, op, want string // want: the status, or the keys and offsets returned
	}{
		{`{"key_base64":"Yg==","offset":0}`, "range", "b1 b2 c/d0"},
		{`{"key_base64":"Yg==","offset":1}`, "list", "b2 c/"},
		{`{"key_base64":"Yg==","offset":9}`, "range", "c/d0"},
		{`{"prefix_base64":"Yy8="}`, "list", ""},
		{`null`, "range", "a0 b0 b1 b2 c/d0"},
		{`{"prefix_base64":"Yy8="}`, "range", "invalid_request"},
		{`{"key_base64":"Yg==","offset":-0}`, "range", "invalid_request"},
		{`{"key_base64":"Yg==","offset":1.0}`, "range", "invalid_request"},
		{`{"key_base64":"Yg==","offset":1e0}`, "range", "invalid_request"},
		{`{"key_base64":"Yg==","offset":"1"}`, "range", "invalid_request"},
		{`{"key_base64":"Yg==","offset":9223372036854775808}`, "range", "invalid_request"},
		{`{"key_base64":"Yg=="}`, "range", "invalid_request"},
		{`{"key_base64":"Yg==","offset":0,"prefix_base64":"Yg=="}`, "list", "invalid_request"},
		{`{"key_base64":"Yg","offset":0}`, "range", "invalid_request"},
		{`[]`, "range", "invalid_request"},
	}
	for _, c := range cases {
		t.Run(c.op+" after "+c.after, func(t *testing.T) {
			resp := run(t, `{"id":"1","op":"`+c.op+`",`+ds+`,"delimiter_base64":"Lw==","limit":10,"after":`+c.after+`}`)[0]
			got, _ := resp["status"].(string)
			if got == "ok" {
				var keys []string
				for _, e := range resp["entries"].([]any) {
					e := e.(map[string]any)
					key, _ := base64.StdEncoding.DecodeString(e["key_base64"].(string))
					if off, ok := e["offset"]; ok {
						keys = append(keys, fmt.Sprint(string(key), off))
					} else {
						keys = append(keys, string(key))
					}
				}
				got = strings.Join(keys, " ")
			}
			if got != c.want {
				t.Fatalf("got %q, want %q (%v)", got, c.want, resp)
			}
		})
	}
}

func quote(s string) string { b, _ := json.Marshal(s); return string(b) }

func jsonString(v any) string { b, _ := json.Marshal(v); return string(b) }
