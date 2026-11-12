// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	seekparquet "github.com/varveio/seekparquet/go"
)

// browseTrace records calls at the storage boundary as well as the fake
// store's physical requests. In particular a sized open has no physical
// request, and a batch's input order is lost in its physical request log.
type browseTrace struct {
	mu    sync.Mutex
	lines []string
}

func (tr *browseTrace) add(line string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.lines = append(tr.lines, line)
}

func (tr *browseTrace) reset() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.lines = nil
}

func (tr *browseTrace) text() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	// Scheduling of independent calls is immaterial; preserve each batch's
	// input order within its line, and preserve repeated calls.
	slices.Sort(tr.lines)
	return strings.Join(tr.lines, "\n")
}

type browseSource struct {
	seekparquet.Source
	tr *browseTrace
}

func (browseSource) ReportsIO() bool { return true }

func (s browseSource) Open(ctx context.Context, name string) (seekparquet.SourceFile, error) {
	s.tr.add("open " + name)
	f, err := s.Source.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return browseFile{SourceFile: f, name: name, tr: s.tr}, nil
}

type browseCapableSource struct{ browseSource }

func (s browseCapableSource) OpenSized(ctx context.Context, name string, size int64) (seekparquet.SourceFile, error) {
	s.tr.add(fmt.Sprintf("open-sized %s %d", name, size))
	f, err := s.Source.(seekparquet.SizedSource).OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return browseRangeFile{browseFile{SourceFile: f, name: name, tr: s.tr}}, nil
}

func (s browseCapableSource) ReadManifest(ctx context.Context, maxBytes int64) ([]byte, string, error) {
	s.tr.add(fmt.Sprintf("manifest %d", maxBytes))
	return s.Source.(seekparquet.ManifestReader).ReadManifest(ctx, maxBytes)
}

type browseFile struct {
	seekparquet.SourceFile
	name string
	tr   *browseTrace
}

func (browseFile) ReportsIO() bool { return true }

func (f browseFile) ReadAt(p []byte, off int64) (int, error) {
	return browseReadAt{f.SourceFile, f.name, f.tr}.ReadAt(p, off)
}

func (f browseFile) WithContext(ctx context.Context) io.ReaderAt {
	return browseReadAt{f.SourceFile.(seekparquet.ContextBinder).WithContext(ctx), f.name, f.tr}
}

type browseReadAt struct {
	r    io.ReaderAt
	name string
	tr   *browseTrace
}

func (r browseReadAt) ReadAt(p []byte, off int64) (int, error) {
	r.tr.add(fmt.Sprintf("read %s %d+%d", r.name, off, len(p)))
	return r.r.ReadAt(p, off)
}

type browseRangeFile struct{ browseFile }

func (f browseRangeFile) ReadRanges(ctx context.Context, ranges []seekparquet.ReadRange) error {
	var b strings.Builder
	fmt.Fprintf(&b, "batch %s", f.name)
	for _, rg := range ranges {
		fmt.Fprintf(&b, " %d+%d", rg.Off, len(rg.Buf))
	}
	f.tr.add(b.String())
	return f.SourceFile.(seekparquet.RangeReader).ReadRanges(ctx, ranges)
}
