// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	"github.com/pierrec/lz4/v4"
)

// zstdDecoder decodes whole zstd buffers, never into more than the
// capacity of the buffer it is given (WithDecodeAllCapLimit).
var zstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true))
})

// decompress returns src decompressed with codec, which must give exactly
// size bytes: the size a page header declares. Decompression stops as soon
// as the output would exceed size, so a small page cannot expand beyond
// what its header declares (and Limits.PageBytes allowed).
func decompress(codec format.CompressionCodec, src []byte, size int) ([]byte, error) {
	wrong := func(n int) error {
		return fmt.Errorf("the page decompresses to %d bytes, its header declares %d", n, size)
	}
	switch codec {
	case format.Uncompressed:
		if len(src) != size {
			return nil, wrong(len(src))
		}
		return src, nil
	case format.Snappy:
		n, err := snappy.DecodedLen(src)
		if err != nil {
			return nil, err
		}
		if n != size {
			return nil, wrong(n)
		}
		return snappy.Decode(make([]byte, size), src)
	case format.Lz4Raw:
		out := make([]byte, size)
		n, err := lz4.UncompressBlock(src, out)
		if err != nil {
			return nil, fmt.Errorf("the page does not decompress to its declared %d bytes: %w", size, err)
		}
		if n != size {
			return nil, wrong(n)
		}
		return out, nil
	case format.Zstd:
		dec, err := zstdDecoder()
		if err != nil {
			return nil, err
		}
		out, err := dec.DecodeAll(src, make([]byte, 0, size))
		if err != nil {
			return nil, fmt.Errorf("the page does not decompress to its declared %d bytes: %w", size, err)
		}
		if len(out) != size {
			return nil, wrong(len(out))
		}
		return out, nil
	case format.Gzip:
		r, err := gzip.NewReader(bytes.NewReader(src))
		if err != nil {
			return nil, err
		}
		return readExactly(r, size)
	case format.Brotli:
		return readExactly(brotli.NewReader(bytes.NewReader(src)), size)
	}
	return nil, fmt.Errorf("codec %s", codec)
}

// readExactly reads exactly size bytes from r, which must then end.
func readExactly(r io.Reader, size int) ([]byte, error) {
	out := make([]byte, size)
	if n, err := io.ReadFull(r, out); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("the page decompresses to %d bytes, its header declares %d", n, size)
		}
		return nil, err
	}
	var more [1]byte
	switch _, err := io.ReadFull(r, more[:]); {
	case err == nil:
		return nil, fmt.Errorf("the page decompresses to more than the %d bytes its header declares", size)
	case !errors.Is(err, io.EOF):
		return nil, err
	}
	return out, nil
}
