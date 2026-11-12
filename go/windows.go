// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
)

// This file holds the bytes an operation reads ahead of decoding from a
// remote Source, in windows: the store every page read of the operation
// looks in before it reads storage (windows), and its memory bounds
// (Limits.ReadMemory, Limits.DatasetReadMemory). The planners that fill it
// are in readplan.go. A window is one range of a checked part's file, read
// with the other windows of its part planned with it in one readRanges
// call (a wave), on a goroutine of its own. A read is served from windows
// only when adjacent windows that were read completely and without error
// hold all of it, and its bytes are then checked as any read's are. A
// window read speculatively that failed is never served: the read goes to
// storage itself, and fails as it would have. A window the operation
// needed that failed fails the read that needed it with its error: it is
// not read again.

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
// used and dropped are guarded by windows.mu.
type window struct {
	p       *part
	off     int64
	buf     []byte
	rowEnd  int64
	spec    bool
	done    chan struct{}
	err     error
	used    int64
	dropped bool
}

func (x *window) end() int64 { return x.off + int64(len(x.buf)) }

// windows is one operation's bytes read ahead of decoding. Its waves run
// under a context of their own, the operation's, which close cancels
// before it waits for them. Every byte of a window is charged, before it
// is allocated, to Limits.ReadMemory (held) and to the reader's
// DatasetReadMemory (readBudget), and given back once the window is
// dropped, which waits for its wave to end.
type windows struct {
	r      *reader
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	held   int64
	list   []*window
	closed bool
	// inCallback is set while the operation runs a caller's callback
	// holding bytes (readBudget.callbacks).
	inCallback bool
	// st counts the waves' storage calls; the other counts are those of
	// the windows, added to the operation's statistics when it ends.
	st                             ReadStats
	specBytes, specRanges          int64
	specUnusedBytes, specFallbacks int64
	peak                           int64
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

// windowBytes is the most bytes one window holds: ReadPolicy.WindowBytes,
// within both memory bounds.
func (r *reader) windowBytes() int64 {
	return min(r.policy.WindowBytes, r.limits.ReadMemory, r.limits.DatasetReadMemory)
}

// readBudget is the reader's DatasetReadMemory: a byte semaphore, and the
// number of operations holding some of it while they run a caller's
// callback. A demand waits for capacity only when none does: such
// capacity is released only once a callback returns, which may itself be
// waiting for the operation that waits.
type readBudget struct {
	opLimit
	callbacks int // guarded by opLimit.mu
}

// mayWait reports whether a demand may wait for the bound's capacity: no
// holder runs a callback.
func (b *readBudget) mayWait() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.callbacks == 0
}

// enterCallback and leaveCallback bracket a caller's callback run by an
// operation that may hold windows (a scan): while its windows hold bytes,
// no demand of another operation waits for the dataset's bound.
func (w *windows) enterCallback() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.held > 0 && !w.inCallback {
		w.inCallback = true
		b := &w.r.readMemory
		b.mu.Lock()
		b.callbacks++
		b.mu.Unlock()
	}
}

func (w *windows) leaveCallback() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.leaveLocked()
}

func (w *windows) leaveLocked() {
	if w.inCallback {
		w.inCallback = false
		b := &w.r.readMemory
		b.mu.Lock()
		b.callbacks--
		b.mu.Unlock()
	}
}

// fetch reads ahead the extents, in the order given, as many as the
// memory bounds, and for speculation (spec) the speculative allowances
// (ReadPolicy.SpeculativeBytes and SpeculativeRanges), admit now, and
// reports how many it admitted: a prefix of them. Extents of one part that
// touch are read as one window, up to windowBytes, cut where an extent
// ends; a larger extent is cut at windowBytes. A window that cannot be
// admitted stops the admission. Speculation never waits; a demand waits
// for the dataset's bound only while the operation holds nothing and no
// holder runs a callback, and until its context ends. Extents the windows
// hold already are not read again.
func (w *windows) fetch(exts []extent, spec bool) int {
	cut := w.r.windowBytes()
	var planned []*window
	admitted := 0
	w.mu.Lock()
	defer func() {
		w.launch(planned)
		w.mu.Unlock()
	}()
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
		ok := true
		for off := start; off < end && ok; off += cut {
			n := min(cut, end-off)
			if ok = w.admit(n, spec); ok {
				planned = append(planned, &window{p: e.p, off: off, buf: make([]byte, n), rowEnd: rowEnd, spec: spec, done: make(chan struct{})})
			}
		}
		if !ok {
			break
		}
		i, admitted = j, j
	}
	return admitted
}

// admit charges n bytes of a window, with mu held: to the speculative
// allowances for speculation, to ReadMemory, and to the dataset's bound.
func (w *windows) admit(n int64, spec bool) bool {
	pol := &w.r.policy
	if w.closed || n > w.r.limits.ReadMemory-w.held ||
		spec && (n > pol.SpeculativeBytes-w.specBytes || w.specRanges >= int64(pol.SpeculativeRanges)) {
		return false
	}
	b := &w.r.readMemory
	if b.tryAcquire(n, 0) {
		w.held += n
	} else {
		if spec || w.held > 0 || !b.mayWait() {
			return false
		}
		w.held += n // so that no other goroutine of the operation waits too
		w.mu.Unlock()
		err := b.acquire(w.ctx, n)
		w.mu.Lock()
		if err != nil || w.closed {
			w.held -= n
			if err == nil {
				b.release(n)
			}
			return false
		}
	}
	if spec {
		w.specBytes += n
		w.specRanges++
	}
	w.peak = max(w.peak, w.held)
	return true
}

// launch reads the windows, with mu held: those of each part in one wave.
func (w *windows) launch(planned []*window) {
	for len(planned) > 0 {
		n := 1
		for n < len(planned) && planned[n].p == planned[0].p {
			n++
		}
		wave := planned[:n:n]
		planned = planned[n:]
		w.list = append(w.list, wave...)
		w.wg.Go(func() { w.read(wave) })
	}
}

// errWindowPanic is the failure of a wave whose source panicked: its
// windows are not served.
var errWindowPanic = errors.New("seekparquet: the source panicked reading ahead")

// read reads one wave and closes each window's done. Its storage calls
// are counted in a ReadStats of its own, added to the windows' once it
// ends.
func (w *windows) read(wave []*window) {
	var st ReadStats
	var errs []error
	defer func() {
		if recover() != nil {
			errs = make([]error, len(wave))
			for i := range errs {
				errs[i] = errWindowPanic
			}
		}
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
	errs = readRanges(w.ctx, rangeFile{file: p.file, name: p.name, reports: p.reports, concurrency: w.r.width()}, ranges, &st)
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
	if w == nil || len(buf) == 0 {
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
		w.mu.Lock()
		w.specFallbacks++
		w.mu.Unlock()
		return false, nil
	}
	end := off + int64(len(buf))
	for _, x := range cover {
		from, to := max(off, x.off), min(end, x.end())
		copy(buf[from-off:to-off], x.buf[from-x.off:to-x.off])
	}
	w.mu.Lock()
	for _, x := range cover {
		from, to := max(off, x.off), min(end, x.end())
		x.used = min(int64(len(x.buf)), x.used+to-from)
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
	if w.held == 0 {
		w.leaveLocked()
	}
}

// retire gives back a finished window's bytes and counts it, with mu
// held.
func (w *windows) retire(x *window) {
	n := int64(len(x.buf))
	x.dropped = true
	w.held -= n
	w.r.readMemory.release(n)
	if x.spec && x.err == nil {
		w.specUnusedBytes += n - x.used
	}
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
	w.leaveLocked()
	if st != nil {
		st.Add(w.st)
		st.SpeculativeBytes += w.specBytes
		st.SpeculativeUnusedBytes += w.specUnusedBytes
		st.SpeculativeFallbackReads += w.specFallbacks
		st.PeakReadBufferBytes = max(st.PeakReadBufferBytes, w.peak)
	}
	w.st = ReadStats{}
	w.specBytes, w.specRanges, w.specUnusedBytes, w.specFallbacks, w.peak = 0, 0, 0, 0, 0
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
