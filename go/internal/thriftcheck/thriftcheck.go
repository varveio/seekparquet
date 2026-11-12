// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package thriftcheck walks Thrift compact-protocol bytes before a decoder
// allocates from them. A decoder that trusts a declared list or string
// length allocates for it before it finds the input too short, so a few
// hostile bytes can ask for gigabytes. Check reads the same bytes without
// allocating, refuses any length that the remaining input cannot hold, and
// estimates the memory a decoder into a given Go type will use, so that a
// caller can refuse input whose decoded form would exceed its budget.
package thriftcheck

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

// Compact-protocol type codes.
const (
	tStop   = 0
	tTrue   = 1
	tFalse  = 2
	tI8     = 3
	tI16    = 4
	tI32    = 5
	tI64    = 6
	tDouble = 7
	tBinary = 8
	tList   = 9
	tSet    = 10
	tMap    = 11
	tStruct = 12
	// tUUID is 16 raw bytes. parquet-go's decoder skips it (a field or
	// element it does not know), so it is walked, and charged, too.
	tUUID = 13
)

// maxDepth bounds nesting, so that hostile input cannot exhaust the stack
// of the walker or of the decoder after it.
const maxDepth = 64

// unknownElement is the memory charged for an element of a container whose
// Go type is unknown (a field the decoder skips, or a union member).
const unknownElement = 64

// ErrMalformed reports bytes that are not well-formed Thrift compact
// protocol, or whose declared lengths exceed the remaining input.
var ErrMalformed = errors.New("malformed thrift")

// ErrBudget reports input whose estimated decoded size exceeds the budget.
var ErrBudget = errors.New("thrift input exceeds its memory budget")

// Check walks one struct of Go type typ (a struct or a pointer to one, such
// as *format.FileMetaData; nil when unknown) at the start of data. It
// returns the number of bytes the struct occupies and the estimated memory
// a decoder will allocate for it. It fails with ErrMalformed when the bytes are not a
// well-formed struct whose lengths fit in data, and with ErrBudget as soon
// as the estimate exceeds budget. Types in nonempty must have at least one wire
// field when present, including an unknown field (for extensible unions).
func Check(data []byte, typ reflect.Type, budget int64, nonempty ...reflect.Type) (n int, cost int64, err error) {
	if typ != nil && typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	w := walker{data: data, budget: budget, nonempty: nonempty}
	if typ != nil {
		w.cost = int64(typ.Size())
	}
	if err := w.structure(typ, 0); err != nil {
		return 0, 0, err
	}
	return w.pos, w.cost, nil
}

type walker struct {
	data     []byte
	pos      int
	cost     int64
	budget   int64
	nonempty []reflect.Type
}

func (w *walker) malformed(what string) error {
	return fmt.Errorf("%w: %s at byte %d", ErrMalformed, what, w.pos)
}

func (w *walker) charge(n int64) error {
	w.cost += n
	if n < 0 || w.cost > w.budget {
		return ErrBudget
	}
	return nil
}

func (w *walker) byte() (byte, error) {
	if w.pos >= len(w.data) {
		return 0, w.malformed("truncated")
	}
	b := w.data[w.pos]
	w.pos++
	return b, nil
}

func (w *walker) uvarint() (uint64, error) {
	var x uint64
	for shift := uint(0); shift < 64; shift += 7 {
		b, err := w.byte()
		if err != nil {
			return 0, err
		}
		x |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return x, nil
		}
	}
	return 0, w.malformed("varint too long")
}

// length reads a container or binary length that must fit in the
// remaining input, each element taking at least min bytes.
func (w *walker) length(n uint64, min int) (int, error) {
	if n > uint64((len(w.data)-w.pos)/min) {
		return 0, w.malformed("length beyond the input")
	}
	return int(n), nil
}

// structure walks a struct; typ is its Go type, nil when unknown.
func (w *walker) structure(typ reflect.Type, depth int) error {
	fields := fieldsOf(typ)
	var id int64
	fieldsSeen := 0
	for {
		b, err := w.byte()
		if err != nil {
			return err
		}
		if b == tStop {
			if fieldsSeen == 0 && slices.Contains(w.nonempty, typ) {
				return w.malformed("empty annotation union")
			}
			return nil
		}
		fieldsSeen++
		kind := b & 0x0f
		if delta := b >> 4; delta != 0 {
			id += int64(delta)
		} else {
			v, err := w.uvarint()
			if err != nil {
				return err
			}
			id = int64(v>>1) ^ -int64(v&1)
		}
		if kind == tTrue || kind == tFalse {
			continue
		}
		var ft reflect.Type
		if fields != nil {
			ft = fields[int16(id)]
		}
		if err := w.value(kind, ft, depth+1); err != nil {
			return err
		}
	}
}

// value walks one value of compact type kind, decoded into Go type typ
// (nil when unknown). Every nested struct, list, set and map element is
// walked through value, so depth bounds the recursion of every container.
func (w *walker) value(kind byte, typ reflect.Type, depth int) error {
	if depth > maxDepth {
		return w.malformed("nesting too deep")
	}
	typ = unwrap(typ)
	switch kind {
	case tTrue, tFalse, tI8:
		_, err := w.byte()
		return err
	case tI16, tI32, tI64:
		_, err := w.uvarint()
		return err
	case tDouble:
		if len(w.data)-w.pos < 8 {
			return w.malformed("truncated")
		}
		w.pos += 8
		return nil
	case tUUID:
		if len(w.data)-w.pos < 16 {
			return w.malformed("truncated")
		}
		w.pos += 16
		return w.charge(16)
	case tBinary:
		v, err := w.uvarint()
		if err != nil {
			return err
		}
		n, err := w.length(v, 1)
		if err != nil {
			return err
		}
		w.pos += n
		return w.charge(int64(n))
	case tStruct:
		if typ != nil && typ.Kind() == reflect.Pointer {
			if err := w.charge(int64(typ.Elem().Size())); err != nil {
				return err
			}
			typ = typ.Elem()
		}
		if typ != nil && typ.Kind() == reflect.Interface {
			typ = nil
		}
		return w.structure(typ, depth)
	case tList, tSet:
		b, err := w.byte()
		if err != nil {
			return err
		}
		size := uint64(b >> 4)
		if size == 0x0f {
			if size, err = w.uvarint(); err != nil {
				return err
			}
		}
		n, err := w.length(size, 1)
		if err != nil {
			return err
		}
		var elem reflect.Type
		per := int64(unknownElement)
		if typ != nil && typ.Kind() == reflect.Slice {
			elem = typ.Elem()
			per = int64(elem.Size())
		}
		if err := w.charge(int64(n) * per); err != nil {
			return err
		}
		for range n {
			if err := w.value(b&0x0f, elem, depth+1); err != nil {
				return err
			}
		}
		return nil
	case tMap:
		v, err := w.uvarint()
		if err != nil {
			return err
		}
		n, err := w.length(v, 2)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		b, err := w.byte()
		if err != nil {
			return err
		}
		if err := w.charge(int64(n) * 2 * unknownElement); err != nil {
			return err
		}
		for range n {
			if err := w.value(b>>4, nil, depth+1); err != nil {
				return err
			}
			if err := w.value(b&0x0f, nil, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return w.malformed("unknown type " + strconv.Itoa(int(kind)))
}

// unwrap returns the Go type a thrift.Null[T] holds.
func unwrap(typ reflect.Type) reflect.Type {
	if typ != nil && typ.Kind() == reflect.Struct && typ.NumField() == 2 &&
		typ.Field(0).Name == "V" && typ.Field(1).Name == "Valid" {
		return typ.Field(0).Type
	}
	return typ
}

var (
	unionType = reflect.TypeFor[thrift.Union]()
	fieldMaps sync.Map // reflect.Type -> map[int16]reflect.Type
)

// fieldsOf maps the thrift field ids of a struct type to their Go types:
// struct tags `thrift:"id,..."`, or a union's members by FieldID.
func fieldsOf(typ reflect.Type) map[int16]reflect.Type {
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil
	}
	if m, ok := fieldMaps.Load(typ); ok {
		return m.(map[int16]reflect.Type)
	}
	m := map[int16]reflect.Type{}
	if reflect.PointerTo(typ).Implements(unionType) {
		u := reflect.New(typ).Interface().(thrift.Union)
		for _, member := range u.UnionMembers() {
			m[member.FieldID()] = reflect.TypeOf(member)
		}
	} else {
		for i := range typ.NumField() {
			tag, ok := typ.Field(i).Tag.Lookup("thrift")
			if !ok {
				continue
			}
			idText, _, _ := strings.Cut(tag, ",")
			id, err := strconv.ParseInt(idText, 10, 16)
			if err == nil {
				m[int16(id)] = typ.Field(i).Type
			}
		}
	}
	fieldMaps.Store(typ, m)
	return m
}
