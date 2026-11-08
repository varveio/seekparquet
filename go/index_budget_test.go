// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

type indexBarrierReader struct {
	io.ReaderAt
	at      int64
	mu      sync.Mutex
	arrived int
	gate    chan struct{}
}

func (r *indexBarrierReader) ReadAt(b []byte, off int64) (int, error) {
	if off == r.at {
		r.mu.Lock()
		r.arrived++
		if r.arrived == 2 {
			close(r.gate)
		}
		r.mu.Unlock()
		<-r.gate
	}
	return r.ReaderAt.ReadAt(b, off)
}

func (r *indexBarrierReader) Close() error {
	if c, ok := r.ReaderAt.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func TestConcurrentIndexAdmission(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	warm := open(t, dir, Options{})
	idx, err := warm.r.parts[0].keyIndex(ctx, 0)
	must(t, err)
	d := open(t, dir, Options{Limits: Limits{IndexMemory: idx.size}})
	p := d.r.parts[0]
	offset := p.footer.Metadata().RowGroups[0].Columns[p.key.index].OffsetIndexOffset
	p.file = &indexBarrierReader{ReaderAt: p.file, at: offset, gate: make(chan struct{})}
	errs := make(chan error, 2)
	for range 2 {
		go func() { _, err := p.keyIndex(ctx, 0); errs <- err }()
	}
	successes, failures := 0, 0
	for range 2 {
		err := <-errs
		if errors.Is(err, ErrLimitExceeded) {
			failures++
		} else if err != nil {
			t.Fatal(err)
		} else {
			successes++
		}
	}
	t.Logf("index size=%d, configured budget=%d, retained=%d; successes=%d, ErrLimitExceeded=%d", idx.size, d.r.limits.IndexMemory, d.r.indexMemory.Load(), successes, failures)
	if failures > 0 {
		t.Errorf("concurrent same-index loads fail although retained index fits")
	}
}

// First keys are retained routing information, even if page bounds are
// shortened to only a few bytes. They share the index budget.
func TestFirstKeysShareIndexBudget(t *testing.T) {
	const groups = 32
	const keyBytes = 128 * 1024
	keys := make([]string, groups)
	for i := range keys {
		keys[i] = fmt.Sprintf("%04d", i) + strings.Repeat("x", keyBytes-4)
	}
	dir := write(t, parts(testutil.Part{GroupRows: 1, PageBytes: 256 * 1024, IndexSizeLimit: 2}, keys))
	if err := openErr(dir, Options{VerifyOnOpen: true, Limits: Limits{IndexMemory: 4096}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("VerifyOnOpen: %v, want ErrLimitExceeded", err)
	}
	warm := open(t, dir, Options{VerifyOnOpen: true})
	var want int64
	for _, p := range warm.r.parts {
		for i := range p.index {
			want += p.index[i].Load().size
		}
	}
	want += groups * keyBytes
	if got := warm.r.indexMemory.Load(); got != want {
		t.Fatalf("charged %d, want %d", got, want)
	}
	exact := open(t, dir, Options{VerifyOnOpen: true, Limits: Limits{IndexMemory: want}})
	if got := exact.r.indexMemory.Load(); got != want {
		t.Fatalf("exact budget charged %d, want %d", got, want)
	}
	if err := openErr(dir, Options{VerifyOnOpen: true, Limits: Limits{IndexMemory: want - 1}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("one byte short: %v, want ErrLimitExceeded", err)
	}
}

func TestConcurrentFirstKeyAdmission(t *testing.T) {
	key := []byte("first")
	r := &reader{limits: Limits{IndexMemory: int64(len(key))}, groups: []*group{{}}}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if err := r.noteFirst(0, key); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := r.indexMemory.Load(); got != int64(len(key)) {
		t.Fatalf("charged %d, want %d", got, len(key))
	}
	key[0] = 'X'
	if got := string(*r.groups[0].first.Load()); got != "first" {
		t.Fatalf("retained key aliases input: %q", got)
	}
}

func TestFirstKeyAdmissionFailureDoesNotPublish(t *testing.T) {
	r := &reader{limits: Limits{IndexMemory: 4}, groups: []*group{{}}}
	if err := r.noteFirst(0, []byte("first")); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("admit: %v", err)
	}
	if r.groups[0].first.Load() != nil || r.indexMemory.Load() != 0 {
		t.Fatal("failed admission retained or charged a key")
	}
	must(t, r.noteFirst(0, []byte("fits")))
	if r.indexMemory.Load() != 4 {
		t.Fatal("failed admission consumed budget")
	}
}
