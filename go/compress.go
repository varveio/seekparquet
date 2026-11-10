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

// codecScratch is request-local. The shared Zstd decoder already pools its state.
type codecScratch struct {
	source bytes.Reader
	gzip   *gzip.Reader
	brotli *brotli.Reader
	more   [1]byte
}

func (s *pageScratch) decompress(codec format.CompressionCodec, src []byte, size int) ([]byte, error) {
	if s == nil {
		return decompress(codec, src, size)
	}
	if codec != format.Uncompressed {
		s.plain = resize(s.plain, size)
	}
	return s.codec.decompress(codec, src, size, s.plain)
}

// decompress returns src decompressed with codec, which must give exactly
// size bytes: the size a page header declares. Decompression stops as soon
// as the output would exceed size, so a small page cannot expand beyond
// what its header declares (and Limits.PageBytes allowed).
func decompress(codec format.CompressionCodec, src []byte, size int) ([]byte, error) {
	return (*codecScratch)(nil).decompress(codec, src, size, nil)
}

func (s *codecScratch) decompress(codec format.CompressionCodec, src []byte, size int, dst []byte) ([]byte, error) {
	// Cap to this page, even after a larger page used the same allocation.
	if codec != format.Uncompressed {
		dst = resize(dst, size)[:size:size]
	}
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
		return snappy.Decode(dst, src)
	case format.Lz4Raw:
		out := dst
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
		out, err := dec.DecodeAll(src, dst[:0:size])
		if err != nil {
			return nil, fmt.Errorf("the page does not decompress to its declared %d bytes: %w", size, err)
		}
		if len(out) != size {
			return nil, wrong(len(out))
		}
		return out, nil
	case format.Gzip:
		if s == nil {
			s = new(codecScratch)
		}
		s.source.Reset(src)
		var err error
		if s.gzip == nil {
			s.gzip, err = gzip.NewReader(&s.source)
		} else {
			err = s.gzip.Reset(&s.source)
		}
		if err != nil {
			return nil, err
		}
		return s.readExactly(s.gzip, dst)
	case format.Brotli:
		if s == nil {
			s = new(codecScratch)
		}
		s.source.Reset(src)
		if s.brotli == nil {
			s.brotli = brotli.NewReader(&s.source)
		} else if err := s.brotli.Reset(&s.source); err != nil {
			return nil, err
		}
		return s.readExactly(s.brotli, dst)
	}
	return nil, fmt.Errorf("codec %s", codec)
}

// readExactly reads exactly size bytes from r, which must then end.
func (s *codecScratch) readExactly(r io.Reader, out []byte) ([]byte, error) {
	size := len(out)
	if n, err := io.ReadFull(r, out); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("the page decompresses to %d bytes, its header declares %d", n, size)
		}
		return nil, err
	}
	switch _, err := io.ReadFull(r, s.more[:]); {
	case err == nil:
		return nil, fmt.Errorf("the page decompresses to more than the %d bytes its header declares", size)
	case !errors.Is(err, io.EOF):
		return nil, err
	}
	return out, nil
}
