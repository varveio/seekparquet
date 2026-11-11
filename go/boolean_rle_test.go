// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func booleanStream(runs ...byte) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(len(runs)))
	return append(out, runs...)
}

func TestBooleanRLEBoundaries(t *testing.T) {
	for _, n := range []int{1, 2, 7, 8, 9, 15, 16, 17} {
		for _, b := range []byte{0, 1} {
			t.Run(fmt.Sprintf("%d/%d", n, b), func(t *testing.T) {
				values, err := testChunk(parquet.BooleanType).values(format.RLE, booleanStream(byte(n<<1), b), n)
				must(t, err)
				for i, v := range values {
					if v.Boolean() != (b == 1) {
						t.Fatalf("value %d: %v", i, v)
					}
				}
			})
		}
	}
	// Three true, two false, eight alternating packed bits, then nine true.
	data := booleanStream(6, 1, 4, 0, 3, 0x55, 18, 1)
	got, err := testChunk(parquet.BooleanType).values(format.RLE, data, 22)
	must(t, err)
	for i, v := range got {
		want := i < 3 || i >= 13 || (i >= 5 && (i-5)%2 == 0)
		if v.Boolean() != want {
			t.Fatalf("value %d", i)
		}
	}
	// A final bit-packed group may contain seven padding bits.
	_, err = testChunk(parquet.BooleanType).values(format.RLE, booleanStream(3, 0xff), 1)
	must(t, err)
}

func TestBooleanRLEMalformedAndLimits(t *testing.T) {
	for _, data := range [][]byte{nil, {1, 0, 0, 0}, booleanStream(0, 1), booleanStream(4), booleanStream(0x80), booleanStream(2, 1), booleanStream(6, 1), booleanStream(3, 1, 2, 1)} {
		if _, err := testChunk(parquet.BooleanType).values(format.RLE, data, 2); RefusalCodeOf(err) != CodeInvalidParquet {
			t.Fatalf("%x: %v", data, err)
		}
	}
	c := testChunk(parquet.BooleanType)
	c.p.r.limits.PageBytes = 1
	if _, err := c.decodeValues(format.RLE, booleanStream(18, 1), 9); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal(err)
	}
	// A hostile count must be refused before allocating its claimed bitmap.
	huge := append(binary.AppendUvarint(nil, 1<<40), 1)
	data := booleanStream(huge...)
	if n := allocated(func() { _, _ = c.decodeValues(format.RLE, data, 2) }); n > 1<<20 {
		t.Fatalf("allocated %d", n)
	}
}
