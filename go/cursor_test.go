// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

type vectorCursor struct {
	Key    *string `json:"key_base64"`
	Prefix *string `json:"prefix_base64"`
	Offset *string `json:"offset"`
}

func (v vectorCursor) cursor(t *testing.T) Cursor {
	var c Cursor
	var err error
	if v.Key != nil {
		c.Key, err = base64.StdEncoding.DecodeString(*v.Key)
		must(t, err)
		c.Key = append([]byte{}, c.Key...)
	}
	if v.Prefix != nil {
		c.Prefix, err = base64.StdEncoding.DecodeString(*v.Prefix)
		must(t, err)
		c.Prefix = append([]byte{}, c.Prefix...)
	}
	if v.Offset != nil {
		c.Offset, err = strconv.ParseInt(*v.Offset, 10, 64)
		must(t, err)
	}
	return c
}

// TestCursorTokenVectors runs the language-neutral golden vectors: each
// valid token parses to its cursor and its cursor encodes to the token;
// each invalid token is an invalid request; each unencodable cursor is
// refused by Encode.
func TestCursorTokenVectors(t *testing.T) {
	raw, err := os.ReadFile("../conformance/vectors/cursor-token-1.json")
	must(t, err)
	var doc struct {
		Valid []struct {
			Token  string       `json:"token"`
			Cursor vectorCursor `json:"cursor"`
		} `json:"valid"`
		Invalid []struct {
			Token string `json:"token"`
			Note  string `json:"note"`
		} `json:"invalid"`
		Unencodable []struct {
			Cursor vectorCursor `json:"cursor"`
		} `json:"unencodable"`
	}
	must(t, json.Unmarshal(raw, &doc))
	if len(doc.Valid) == 0 || len(doc.Invalid) == 0 || len(doc.Unencodable) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range doc.Valid {
		want := v.Cursor.cursor(t)
		got, err := ParseCursor(v.Token)
		if err != nil || !reflect.DeepEqual(*got, want) {
			t.Errorf("%q: %+v, %v; want %+v", v.Token, got, err, want)
			continue
		}
		if (got.Key == nil) == (got.Prefix == nil) {
			t.Errorf("%q: an empty payload parsed as no payload", v.Token)
		}
		if token, err := want.Encode(); err != nil || token != v.Token {
			t.Errorf("%+v: %q, %v; want %q", want, token, err, v.Token)
		}
	}
	for _, v := range doc.Invalid {
		if c, err := ParseCursor(v.Token); !errors.Is(err, ErrInvalidRequest) || c != nil || RefusalCodeOf(err) != "" {
			t.Errorf("%q (%s): %+v, %v", v.Token, v.Note, c, err)
		}
	}
	for _, v := range doc.Unencodable {
		c := v.Cursor.cursor(t)
		if token, err := c.Encode(); !errors.Is(err, ErrInvalidRequest) || token != "" {
			t.Errorf("%+v: %q, %v", c, token, err)
		}
	}
}

// TestCursorTokenLength: the longest key a token holds encodes and parses
// back; a longer one, or a longer token, is an invalid request, refused
// before the token is decoded.
func TestCursorTokenLength(t *testing.T) {
	const most = 786_411
	for _, n := range []int{most - 1, most} {
		c := Cursor{Key: bytes.Repeat([]byte{0xff}, n), Offset: 1<<63 - 1}
		token, err := c.Encode()
		if err != nil || len(token) > MaxCursorToken {
			t.Fatalf("%d bytes: %d, %v", n, len(token), err)
		}
		back, err := ParseCursor(token)
		if err != nil || !bytes.Equal(back.Key, c.Key) || back.Offset != c.Offset {
			t.Fatalf("%d bytes back: %v", n, err)
		}
	}
	for _, n := range []int{most + 2, MaxCursorToken + 1} {
		if _, err := (Cursor{Key: make([]byte, n), Offset: 1<<63 - 1}).Encode(); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%d bytes: %v", n, err)
		}
	}
	if _, err := (Cursor{Prefix: make([]byte, 786_426)}).Encode(); err != nil {
		t.Fatal(err)
	}
	long := "spc1.p." + strings.Repeat("A", MaxCursorToken)
	if _, err := ParseCursor(long); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
}

// TestCursorTokenPagination pages a range and a listing of a repeated-key
// dataset carrying only tokens between pages: the pages concatenate to the
// unpaginated result, including pages ending inside a group and after a
// common prefix.
func TestCursorTokenPagination(t *testing.T) {
	keys := []string{"a/1", "a/1", "a/1", "a/2", "b", "b", "c/x/1", "c/x/2", "c/y", "d"}
	doc := testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: testutil.Rows(keys...), PageBytes: 64}}}
	d := open(t, write(t, doc), OpenOptions{})
	all, err := d.Rows(ctx, Query{Limit: 100, Columns: []string{"size"}}, nil)
	must(t, err)
	for limit := int64(1); limit <= 4; limit++ {
		var got []Row
		token := ""
		for {
			q := Query{Limit: limit, Columns: []string{"size"}}
			if token != "" {
				c, err := ParseCursor(token)
				must(t, err)
				q.Cursor = c
			}
			page, err := d.Rows(ctx, q, nil)
			must(t, err)
			got = append(got, page...)
			if int64(len(page)) < limit {
				break
			}
			token, err = page[len(page)-1].Cursor().Encode()
			must(t, err)
		}
		if !reflect.DeepEqual(got, all) {
			t.Fatalf("limit %d: %d rows of %d", limit, len(got), len(all))
		}
		want, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 100}, nil)
		must(t, err)
		var entries []ListEntry
		token = ""
		for {
			q := ListQuery{Delimiter: '/', Limit: int(limit)}
			if token != "" {
				c, err := ParseCursor(token)
				must(t, err)
				q.Cursor = c
			}
			page, err := d.List(ctx, q, nil)
			must(t, err)
			entries = append(entries, page...)
			if len(page) < int(limit) {
				break
			}
			token, err = page[len(page)-1].Cursor().Encode()
			must(t, err)
		}
		if !reflect.DeepEqual(entriesOf(entries), entriesOf(want)) {
			t.Fatalf("list limit %d: %v, want %v", limit, entriesOf(entries), entriesOf(want))
		}
	}
}
