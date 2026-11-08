// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package write

import (
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/format"
)

// zstdCodec is the default codec: pure-Go ZSTD at its default speed, with
// entropy coding of all-literal blocks, no frame checksum, and the lower
// encoder memory. A page's compressed bytes do not depend on which state
// compresses it (EncodeAll resets the state), so pages compress in
// parallel on pooled states and stay byte-identical.
type zstdCodec struct {
	states  sync.Pool
	decoder sync.Pool
}

var defaultCodec compress.Codec = &zstdCodec{}

func (c *zstdCodec) String() string { return "ZSTD" }

func (c *zstdCodec) CompressionCodec() format.CompressionCodec { return format.Zstd }

func (c *zstdCodec) Encode(dst, src []byte) ([]byte, error) {
	e, _ := c.states.Get().(*zstd.Encoder)
	if e == nil {
		var err error
		e, err = zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderCRC(false),
			zstd.WithZeroFrames(true),
			zstd.WithAllLitEntropyCompression(true),
			zstd.WithLowerEncoderMem(true),
		)
		if err != nil {
			return nil, err
		}
	}
	defer c.states.Put(e)
	return e.EncodeAll(src, dst[:0]), nil
}

func (c *zstdCodec) Decode(dst, src []byte) ([]byte, error) {
	d, _ := c.decoder.Get().(*zstd.Decoder)
	if d == nil {
		var err error
		if d, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1)); err != nil {
			return nil, err
		}
	}
	defer c.decoder.Put(d)
	return d.DecodeAll(src, dst[:0])
}
