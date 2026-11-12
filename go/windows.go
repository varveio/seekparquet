// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"cmp"
	"context"
	"slices"
	"sync"
)

// Windows hold complete reads ahead of decoding for one operation. The
// source merges, splits and schedules each wave's physical requests.
const (
	readWaveBytes   int64 = 64 << 20
	readWindowBytes int64 = 4 << 20
)

// extent is bytes an operation will read: size bytes at off of part p, of
// rows below the dataset ordinal rowEnd, once the operation has passed
// which a window holding it may be dropped.
type extent struct {
	p         *part
	off, size int64
	rowEnd    int64
}

// window is one range read ahead: the len(buf) bytes at off of part p,
// holding bytes of rows below rowEnd. err is set before done is closed.
// dropped is guarded by windows.mu.
type window struct {
	p       *part
	off     int64
	buf     []byte
	rowEnd  int64
	spec    bool
	done    chan struct{}
	err     error
	dropped bool
}

func (x *window) end() int64 { return x.off + int64(len(x.buf)) }

// windows owns at most readWaveBytes of fetched, undecoded buffers.
// Admission never waits. Workers for different handles run concurrently
// within a wave. Each fetch joins its own workers; wg joins all workers
// before close frees buffers. Admission and closing are serialized by mu.
type windows struct {
	r      *reader
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	held   int64
	list   []*window
	ahead  []aheadPage // listing pages fetched, for the existing ReadAhead counters
	closed bool
	st     ReadStats
}

// windows returns the operation's windows, made on first use.
func (o *operation) windows() *windows {
	if w := o.win.Load(); w != nil {
		return w
	}
	ctx, cancel := context.WithCancel(o)
	if !o.win.CompareAndSwap(nil, &windows{r: o.r, ctx: ctx, cancel: cancel}) {
		cancel()
	}
	return o.win.Load()
}

// windowsOf returns the windows of the operation ctx runs under, nil when
// it has none yet or runs under none.
func windowsOf(ctx context.Context) *windows {
	if o := operationOf(ctx); o != nil {
		return o.win.Load()
	}
	return nil
}

// fetch submits a prefix that fits the fixed operation bound. Adjacent
// extents become contiguous runs. An extent is admitted whole or omitted.
func (w *windows) fetch(exts []extent, spec bool) int {
	w.mu.Lock()
	n, planned := w.plan(exts, spec)
	wg := w.launch(planned)
	w.mu.Unlock()
	wg.Wait()
	return n
}

// aheadPage records a listing page independently of the windows containing
// it: adjacent pages may share a read, and one page may span several reads.
type aheadPage struct {
	extent
	used bool
}

func (w *windows) fetchAhead(exts []extent) {
	fresh := make([]bool, len(exts))
	w.mu.Lock()
	for i, e := range exts {
		fresh[i] = w.coverLocked(e.p, e.off, e.size) == nil
	}
	w.mu.Unlock()
	n := w.fetch(exts, true)
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, e := range exts[:n] {
		if fresh[i] {
			w.ahead = append(w.ahead, aheadPage{extent: e})
			w.st.ReadAheadReads++
		}
	}
}

// fetchWave submits demanded and optional runs together per handle.
func (w *windows) fetchWave(need, spec []extent) bool {
	w.mu.Lock()
	n, planned := w.plan(need, false)
	m, optional := w.plan(spec, true)
	planned = append(planned, optional...)
	slices.SortStableFunc(planned, func(a, b *window) int { return cmp.Compare(a.p.name, b.p.name) })
	wg := w.launch(planned)
	w.mu.Unlock()
	wg.Wait()
	return n == len(need) && m == len(spec)
}

// plan reserves buffers with mu held. Oversized runs fall back to demand.
func (w *windows) plan(exts []extent, spec bool) (int, []*window) {
	cut := readWindowBytes
	var planned []*window
	admitted := 0
	for i := 0; i < len(exts); {
		e := exts[i]
		if e.size <= 0 || w.coverLocked(e.p, e.off, e.size) != nil {
			i++
			admitted = i
			continue
		}
		start, end, rowEnd, j := e.off, e.off+e.size, e.rowEnd, i+1
		for ; j < len(exts) && exts[j].p == e.p && exts[j].off >= start && exts[j].off <= end; j++ {
			next := max(end, exts[j].off+exts[j].size)
			if next-start > cut || w.coverLocked(e.p, exts[j].off, exts[j].size) != nil {
				break
			}
			end, rowEnd = next, max(rowEnd, exts[j].rowEnd)
		}
		if w.closed || start < 0 || end < start || end-start > readWaveBytes-w.held ||
			len(w.list)+len(planned)+int((end-start+cut-1)/cut) > MaxReadRanges {
			break
		}
		w.held += end - start
		for off := start; off < end; off += cut {
			n := min(cut, end-off)
			planned = append(planned, &window{p: e.p, off: off, buf: make([]byte, n), rowEnd: rowEnd, spec: spec, done: make(chan struct{})})
		}
		i, admitted = j, j
	}
	return admitted, planned
}

// launch reads the windows, with mu held: those of each part in one wave.
func (w *windows) launch(planned []*window) *sync.WaitGroup {
	var wg sync.WaitGroup
	for len(planned) > 0 {
		n := 1
		for n < len(planned) && planned[n].p == planned[0].p {
			n++
		}
		wave := planned[:n:n]
		planned = planned[n:]
		w.list = append(w.list, wave...)
		w.wg.Add(1)
		wg.Go(func() {
			defer w.wg.Done()
			w.read(wave)
		})
	}
	return &wg
}

// read reads one wave and closes each window's done. Its storage calls
// are counted in a ReadStats of its own, added to the windows' once it
// ends.
func (w *windows) read(wave []*window) {
	var st ReadStats
	var errs []error
	defer func() {
		for i, x := range wave {
			if errs != nil {
				x.err = errs[i]
			}
			close(x.done)
		}
		w.mu.Lock()
		w.st.Add(st)
		w.mu.Unlock()
	}()
	p := wave[0].p
	ranges := make([]ReadRange, len(wave))
	for i, x := range wave {
		ranges[i] = ReadRange{Off: x.off, Buf: x.buf}
	}
	errs = readRanges(w.ctx, rangeFile{file: p.file, name: p.name, concurrency: w.r.width()}, ranges, &st)
}

// coverLocked returns the windows not dropped that hold, or are reading,
// all of the size bytes at off of p, in order, or nil, with mu held.
func (w *windows) coverLocked(p *part, off, size int64) []*window {
	var out []*window
	for pos, end := off, off+size; pos < end; {
		var next *window
		for _, x := range w.list {
			if x.p == p && !x.dropped && x.off <= pos && pos < x.end() {
				next = x
				break
			}
		}
		if next == nil {
			return nil
		}
		out = append(out, next)
		pos = next.end()
	}
	return out
}

// holds reports whether the windows hold, or are reading, the size bytes
// at off of p.
func (w *windows) holds(p *part, off, size int64) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.coverLocked(p, off, size) != nil
}

// copy fills buf with the bytes at off of p when the windows hold all of
// them, waiting for the windows still reading, and reports whether it did.
// A window the operation needed that failed is the read's error; one read
// speculatively that failed is not, and the read goes to storage.
func (w *windows) copy(p *part, buf []byte, off int64) (bool, error) {
	if w == nil || len(buf) == 0 || w.ctx.Err() != nil {
		return false, nil
	}
	w.mu.Lock()
	cover := w.coverLocked(p, off, int64(len(buf)))
	w.mu.Unlock()
	if cover == nil {
		return false, nil
	}
	for _, x := range cover {
		select {
		case <-x.done:
		case <-w.ctx.Done():
			return false, nil // the read the caller makes fails with the operation
		}
	}
	for _, x := range cover {
		if x.err == nil {
			continue
		}
		if !x.spec && w.ctx.Err() == nil {
			return false, x.err
		}
		return false, nil
	}
	end := off + int64(len(buf))
	for _, x := range cover {
		from, to := max(off, x.off), min(end, x.end())
		copy(buf[from-off:to-off], x.buf[from-x.off:to-x.off])
	}
	w.mu.Lock()
	for i := range w.ahead {
		x := &w.ahead[i]
		if x.p == p && x.off < end && off < x.off+x.size {
			x.used = true
		}
	}
	w.mu.Unlock()
	return true, nil
}

// release drops the windows that hold only rows below ordinal, whose
// reads have ended: an operation reading forward that reached ordinal
// reads none of them again.
func (w *windows) release(ordinal int64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dropLocked(func(x *window) bool { return x.rowEnd <= ordinal })
}

// dropLocked drops the windows drop selects whose reads have ended, with
// mu held.
func (w *windows) dropLocked(drop func(*window) bool) {
	kept := w.list[:0]
	for _, x := range w.list {
		select {
		case <-x.done:
			if drop(x) {
				w.retire(x)
				continue
			}
		default:
		}
		kept = append(kept, x)
	}
	clear(w.list[len(kept):])
	w.list = kept
	pages := w.ahead[:0]
	for _, x := range w.ahead {
		if w.coverLocked(x.p, x.off, x.size) == nil {
			if !x.used {
				w.st.ReadAheadUnused++
				w.st.ReadAheadUnusedBytes += x.size
			}
		} else {
			pages = append(pages, x)
		}
	}
	clear(w.ahead[len(pages):])
	w.ahead = pages
}

// retire gives back a finished window's bytes and counts it, with mu
// held.
func (w *windows) retire(x *window) {
	n := int64(len(x.buf))
	x.dropped = true
	w.held -= n
	x.buf = nil
}

// close ends the waves still reading, waits for them, gives back every
// byte held and adds the windows' counts to st (nil for none).
func (w *windows) close(st *ReadStats) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dropLocked(func(*window) bool { return true })
	if st != nil {
		st.Add(w.st)
	}
	w.st = ReadStats{}
}

// sortExtents orders the extents of each part, kept together in the order
// given, by offset: the order windows are read in.
func sortExtents(exts []extent) {
	for i := 0; i < len(exts); {
		j := i + 1
		for j < len(exts) && exts[j].p == exts[i].p {
			j++
		}
		slices.SortStableFunc(exts[i:j], func(a, b extent) int { return cmp.Compare(a.off, b.off) })
		i = j
	}
}
