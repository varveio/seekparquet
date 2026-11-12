// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package thriftcheck

import (
	"errors"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestCheck(t *testing.T) {
	meta := format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{Name: "root", NumChildren: thrift.New[int32](1)},
			{Name: "key", Type: thrift.New(format.ByteArray), LogicalType: format.LogicalType{Value: &format.StringType{}}},
		},
		NumRows:          3,
		KeyValueMetadata: []format.KeyValue{{Key: "k", Value: "v"}},
		ColumnOrders:     []format.ColumnOrder{{Value: &format.TypeDefinedOrder{}}},
	}
	raw, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
	if err != nil {
		t.Fatal(err)
	}
	typ := reflect.TypeOf(&meta)
	n, cost, err := Check(raw, typ, 1<<20)
	if err != nil || n != len(raw) || cost < int64(reflect.TypeOf(meta).Size()) {
		t.Fatalf("valid footer: n %d of %d, cost %d, %v", n, len(raw), cost, err)
	}
	if _, _, err := Check(raw, typ, cost-1); !errors.Is(err, ErrBudget) {
		t.Fatalf("budget: %v", err)
	}
	for i := range raw {
		if _, _, err := Check(raw[:i], typ, 1<<20); !errors.Is(err, ErrMalformed) {
			t.Fatalf("truncated at %d: %v", i, err)
		}
	}
	// A list declaring 2^31-1 structs in eight bytes.
	if _, _, err := Check([]byte{0x29, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00}, typ, 1<<40); !errors.Is(err, ErrMalformed) {
		t.Fatalf("huge list: %v", err)
	}
	// A string declaring more bytes than follow.
	if _, _, err := Check([]byte{0x18, 0x7f, 'a', 0x00}, typ, 1<<40); !errors.Is(err, ErrMalformed) {
		t.Fatalf("long string: %v", err)
	}
	// Nesting beyond the bound.
	deep := make([]byte, 0, 300)
	for range 100 {
		deep = append(deep, 0x1c) // field 1, struct
	}
	if _, _, err := Check(deep, nil, 1<<40); !errors.Is(err, ErrMalformed) {
		t.Fatalf("deep nesting: %v", err)
	}
}

// TestContainerDepth: well-formed input nesting lists, maps or both deeper
// than the bound inside an unknown field is refused, as nested structs are,
// and the same shapes within the bound are accepted.
func TestContainerDepth(t *testing.T) {
	const (
		listOfList = 0x19 // a list of one element of type list
		listOfMap  = 0x1b // a list of one element of type map
	)
	mapOf := func(value byte) []byte { return []byte{0x01, 0x30 | value, 0x00} } // one i8 key 0
	shapes := map[string]func(depth int) []byte{
		"list in list": func(depth int) []byte {
			raw := []byte{0x19} // field 1, a list
			for range depth {
				raw = append(raw, listOfList)
			}
			return append(raw, 0x03, 0x00) // an empty list of i8, then stop
		},
		"map in map": func(depth int) []byte {
			raw := []byte{0x1b} // field 1, a map
			for range depth {
				raw = append(raw, mapOf(0x0b)...)
			}
			return append(raw, 0x00, 0x00) // an empty map, then stop
		},
		"list and map": func(depth int) []byte {
			raw := []byte{0x19}
			for range depth / 2 {
				raw = append(raw, listOfMap)
				raw = append(raw, mapOf(0x09)...)
			}
			return append(raw, 0x03, 0x00)
		},
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Check(shape(40), nil, 1<<40); err != nil {
				t.Fatalf("depth 40: %v", err)
			}
			if _, _, err := Check(shape(200), nil, 1<<40); !errors.Is(err, ErrMalformed) {
				t.Fatalf("depth 200: %v, want ErrMalformed", err)
			}
		})
	}
}

// TestUUID: a UUID (compact type 13, 16 raw bytes) in a field or as the
// elements of a container is walked, and charged, as parquet-go's decoder
// skips it; a truncated one, too deep a nesting, or one beyond the budget
// is refused.
func TestUUID(t *testing.T) {
	meta := format.FileMetaData{Version: 1, NumRows: 3, Schema: []format.SchemaElement{{Name: "root"}}}
	raw, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
	if err != nil {
		t.Fatal(err)
	}
	body := raw[:len(raw)-1] // before the stop
	uuid := make([]byte, 16)
	for i := range uuid {
		uuid[i] = byte(0xA0 + i)
	}
	// Field 100, long-form header: type 13, then the zigzag i16 id.
	field := append([]byte{0x0d, 0xc8, 0x01}, uuid...)
	// Field 101, a list of three UUIDs.
	list := append([]byte{0x09, 0xca, 0x01, 0x3d}, append(append(append([]byte{}, uuid...), uuid...), uuid...)...)
	// Field 102, a list of one list of two UUIDs.
	nested := append([]byte{0x09, 0xcc, 0x01, 0x19, 0x2d}, append(append([]byte{}, uuid...), uuid...)...)
	typ := reflect.TypeOf(&meta)
	for name, extra := range map[string][]byte{"field": field, "list": list, "nested list": nested} {
		in := append(append(append([]byte{}, body...), extra...), 0x00)
		n, cost, err := Check(in, typ, 1<<20)
		if err != nil || n != len(in) {
			t.Fatalf("%s: n %d of %d, %v", name, n, len(in), err)
		}
		var got format.FileMetaData
		if err := thrift.Unmarshal(new(thrift.CompactProtocol), in, &got); err != nil || got.NumRows != 3 {
			t.Fatalf("%s: parquet-go: %v, %d rows", name, err, got.NumRows)
		}
		if _, _, err := Check(in, typ, cost-1); !errors.Is(err, ErrBudget) {
			t.Fatalf("%s: under budget: %v", name, err)
		}
		for i := len(body); i < len(in); i++ {
			if _, _, err := Check(in[:i], typ, 1<<20); !errors.Is(err, ErrMalformed) {
				t.Fatalf("%s: truncated at %d: %v", name, i, err)
			}
		}
	}
	// UUID lists nested beyond the bound.
	deep := []byte{0x19}
	for range 200 {
		deep = append(deep, 0x19)
	}
	deep = append(deep, 0x1d)
	deep = append(deep, uuid...)
	if _, _, err := Check(append(deep, 0x00), nil, 1<<40); !errors.Is(err, ErrMalformed) {
		t.Fatalf("deep UUID lists: %v", err)
	}
}

func FuzzCheck(f *testing.F) {
	f.Add([]byte{0x15, 0x02, 0x29, 0x1c, 0x18, 0x01, 'a', 0x00, 0x00})
	f.Fuzz(func(t *testing.T, raw []byte) {
		var meta format.FileMetaData
		n, _, err := Check(raw, reflect.TypeOf(&meta), 64<<20)
		if err != nil {
			return
		}
		// What Check accepts decodes without exceeding the input.
		_ = thrift.Unmarshal(new(thrift.CompactProtocol), raw[:n], &meta)
	})
}
