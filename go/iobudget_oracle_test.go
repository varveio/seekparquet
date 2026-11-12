// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
)

// TestReadBudgetsOracle checks the budget operations' results that follow
// from the fixture's rows alone against those rows, generated again from
// the seed: the lookups, Get, the 100-row ranges and the listing pages.
// The budget tests compare the fake store's results with a local open of
// the same reader; this compares that local open with what was written,
// independently of the reader.
func TestReadBudgetsOracle(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	for _, spec := range []ioFixtureSpec{ioSmall, ioRepeated} {
		fx := ioFixtureOf(t, spec)
		rows, keys := ioRows(spec)
		o := ioOracle{written: rows, keys: keys}
		want := map[string]string{}
		for _, l := range []struct {
			name string
			key  []byte
		}{{"first", fx.first}, {"mid", fx.mid}, {"last", fx.last}, {"other-group", fx.other}} {
			one := o.rows(o.at(l.key), 1, ioColumns)
			group := o.rows(o.at(l.key), o.groupLen(l.key), ioColumns)
			for _, temp := range []string{"cold", "warm"} {
				want["rows-key/"+l.name+"/"+temp] = withErr(rowsText(one, nil))
				want["get/"+l.name+"/"+temp] = withErr(rowsText(group, nil))
			}
		}
		for _, temp := range []string{"cold", "warm"} {
			want["rows-100/2-cols/"+temp] = withErr(rowsText(o.rows(o.at(fx.mid), 100, ioColumns), nil))
			want["rows-100/keys/"+temp] = withErr(rowsText(o.rows(o.at(fx.mid), 100, nil), nil))
		}
		if !spec.repeated {
			page1 := o.list(fx.listPrefix, nil, 1000)
			want["list-ra0/page1-cold"] = withErr(listText(page1, nil))
			want["list-ra0/page1-warm"] = want["list-ra0/page1-cold"]
			want["list-ra0/page2"] = withErr(listText(o.list(fx.listPrefix, page1[len(page1)-1].Cursor(), 1000), nil))
		}
		got := map[string]string{}
		for _, sc := range ioScenarios(fx) {
			switch sc.name {
			case "rows-key/first", "rows-key/mid", "rows-key/last", "get/first", "get/mid", "get/last", "rows-100/2-cols", "rows-100/keys", "list-ra0":
				sc.run(&ioEnv{t: t, fx: fx, results: got})
			}
		}
		checked := 0
		for step, w := range want {
			g, ok := got[step]
			if !ok {
				continue
			}
			checked++
			if g != w {
				t.Errorf("%+v %s: the reader\n%.1500s\nthe written rows\n%.1500s", spec, step, g, w)
			}
		}
		if checked < 16 {
			t.Fatalf("%+v: %d steps checked", spec, checked)
		}
	}
}

// ioOracle answers from the written rows: key, size, last_modified, etag,
// storage_class, in that column order.
type ioOracle struct {
	written []parquet.Row
	keys    [][]byte
}

// at returns the first row whose key is at or after key.
func (o ioOracle) at(key []byte) int {
	return sort.Search(len(o.keys), func(i int) bool { return bytes.Compare(o.keys[i], key) >= 0 })
}

// groupLen returns the rows of key's group.
func (o ioOracle) groupLen(key []byte) int {
	i := o.at(key)
	n := 0
	for i+n < len(o.keys) && bytes.Equal(o.keys[i+n], key) {
		n++
	}
	return n
}

// row returns row i with columns projected, as Rows returns it.
func (o ioOracle) row(i int, columns []string) seekparquet.Row {
	offset := 0
	for i-offset > 0 && bytes.Equal(o.keys[i-offset-1], o.keys[i]) {
		offset++
	}
	r := seekparquet.Row{Key: o.keys[i], Offset: int64(offset), Columns: columns}
	for _, c := range columns {
		switch c {
		case "size":
			r.Values = append(r.Values, seekparquet.Int64Value(o.written[i][1].Int64()))
		case "last_modified":
			r.Values = append(r.Values, seekparquet.Int64Value(o.written[i][2].Int64()))
		default:
			panic("column " + c)
		}
	}
	return r
}

// rows returns n rows from row i on.
func (o ioOracle) rows(i, n int, columns []string) []seekparquet.Row {
	var out []seekparquet.Row
	for ; i < len(o.keys) && len(out) < n; i++ {
		out = append(out, o.row(i, columns))
	}
	return out
}

// list returns the entries of a '/'-delimited listing of prefix after
// cursor, unique keys: a common prefix for each child folder, a row for
// each object directly under prefix.
func (o ioOracle) list(prefix []byte, cursor *seekparquet.Cursor, limit int) []seekparquet.ListEntry {
	var out []seekparquet.ListEntry
	for i := o.at(prefix); i < len(o.keys) && bytes.HasPrefix(o.keys[i], prefix) && len(out) < limit; i++ {
		k := o.keys[i]
		if cursor != nil && cursor.Prefix != nil && bytes.HasPrefix(k, cursor.Prefix) ||
			cursor != nil && cursor.Prefix == nil && bytes.Compare(k, cursor.Key) <= 0 ||
			cursor != nil && cursor.Prefix != nil && bytes.Compare(k, cursor.Prefix) < 0 {
			continue
		}
		if j := bytes.IndexByte(k[len(prefix):], '/'); j >= 0 {
			cp := k[:len(prefix)+j+1]
			if n := len(out); n > 0 && bytes.Equal(out[n-1].CommonPrefix, cp) {
				continue
			}
			out = append(out, seekparquet.ListEntry{CommonPrefix: bytes.Clone(cp)})
			continue
		}
		r := o.row(i, nil)
		out = append(out, seekparquet.ListEntry{Row: &r})
	}
	return out
}
