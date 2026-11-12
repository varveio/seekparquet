// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package fakestore is a counting, latency- and fault-injecting fake object
// store for this module's tests. It serves dataset files, from memory or
// from a directory, the way a remote store behind objfile serves them
// today: an optional metadata request (a HEAD) for every Open, then one
// ranged request for every non-empty ReadAt. It records every physical
// request of an operation: its object, kind, extent, start and end on a
// virtual clock, the state of its context and the round it ran in.
//
// The clock is the one of package testing/synctest: a request "takes"
// Options.Latency plus its bytes at Options.BytesPerSecond by sleeping, so
// inside a synctest bubble requests that are in flight together overlap,
// a request issued after another returned starts when it ended, and the
// times, rounds and summaries are identical on every run and under -race,
// with no wall-clock wait. Outside a bubble the sleeps are real; use a zero
// Latency there, and do not read rounds.
//
// The package does not import the module's root package, so that both its
// internal and external tests can use it. Source.Open returns a *File;
// a test adapts Source to seekparquet.Source with a three-line wrapper.
//
// The request log and the scheduling model do not depend on how a read
// arrived: every physical request goes through one path (Source.do), which
// a batched or sized read path can share. Request.Batch names the batch a
// request served, 0 for a scalar ReadAt.
package fakestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is the kind of a physical request.
type Kind uint8

// The kinds of request.
const (
	// Metadata is the request a store makes to learn an object's size and
	// identity when it is opened (an S3 HEAD, a GCS Attrs, an Azure
	// GetProperties).
	Metadata Kind = iota + 1
	// Range is a ranged read of part of an object.
	Range
	// Whole is a read of a whole object in one request.
	Whole
)

func (k Kind) String() string {
	switch k {
	case Metadata:
		return "META"
	case Range:
		return "RANGE"
	case Whole:
		return "WHOLE"
	}
	return fmt.Sprintf("Kind(%d)", k)
}

// Object is one stored object.
type Object struct {
	Data []byte
	// Identity is the object's version (an ETag or generation); "" is
	// derived from its path.
	Identity string
}

// Options configures a Source.
type Options struct {
	// Latency is every request's fixed cost on the virtual clock: the
	// round trip.
	Latency time.Duration
	// BytesPerSecond is the transfer rate on the virtual clock; a request
	// takes Latency plus its requested bytes at this rate. 0 makes
	// transfer free.
	BytesPerSecond int64
	// MetadataOnOpen makes every Open one Metadata request, as objfile's
	// Open (and Pinned.Open) does today; false models an open that needs
	// none, as a sized open will.
	MetadataOnOpen bool
	// MaxInFlight bounds the requests in flight at once across the
	// source; more wait for a slot, and their wait is not part of the
	// request. 0 is unbounded.
	MaxInFlight int
}

// FaultKind is a fault a request can be made to suffer.
type FaultKind uint8

// The faults.
const (
	// Fail fails the request after its latency, with ErrInjected and no
	// bytes.
	Fail FaultKind = iota + 1
	// ShortRead returns fewer bytes than asked for (Fault.Bytes, or half)
	// and no error, which io.ReaderAt forbids: a broken source.
	ShortRead
	// FailAfter returns Fault.Bytes bytes (or half) with ErrInjected, as a
	// body that breaks off.
	FailAfter
	// Hang never completes: the request waits until its context ends, and
	// returns the context's error. A read bound to no context waits until
	// the source is closed (in a synctest bubble, a deadlock the test
	// reports).
	Hang
)

func (k FaultKind) String() string {
	switch k {
	case 0:
		return "none"
	case Fail:
		return "fail"
	case ShortRead:
		return "short-read"
	case FailAfter:
		return "fail-after"
	case Hang:
		return "hang"
	}
	return fmt.Sprintf("FaultKind(%d)", k)
}

// Fault makes the At-th request of an operation (1-based, in start order,
// every kind counted) suffer Kind. On a Metadata request every kind but
// Hang fails the Open.
type Fault struct {
	At    int
	Kind  FaultKind
	Bytes int // ShortRead, FailAfter: bytes returned; 0 is half the request
}

// ErrInjected is the error of an injected fault.
var ErrInjected = errors.New("fakestore: injected fault")

// ErrClosed is returned by requests made after Source.Close.
var ErrClosed = errors.New("fakestore: source closed")

// CtxState is the state of a request's context when it started.
type CtxState uint8

// The context states.
const (
	// Unbound: the read came through ReadAt, bound to no context.
	Unbound CtxState = iota
	// Live: the context had not ended.
	Live
	// Canceled: the context was cancelled.
	Canceled
	// Expired: the context's deadline had passed.
	Expired
)

func (c CtxState) String() string {
	return [...]string{"unbound", "live", "canceled", "expired"}[c]
}

func stateOf(ctx context.Context) CtxState {
	if ctx == nil {
		return Unbound
	}
	switch err := ctx.Err(); {
	case err == nil:
		return Live
	case errors.Is(err, context.DeadlineExceeded):
		return Expired
	}
	return Canceled
}

// Source is a fake store holding a dataset's files. It is safe for
// concurrent use.
type Source struct {
	opt     Options
	objects map[string]Object // in memory, nil when on disk
	root    *os.Root          // on disk, nil when in memory
	closed  chan struct{}
	once    sync.Once

	// The MaxInFlight slots: free ones, the requests waiting for one, and
	// whether a grant is scheduled.
	qmu         sync.Mutex
	free        int
	queue       []*waiter
	dispatching bool

	mu  sync.Mutex
	op  *Operation // the operation being recorded, nil for none
	seq int64      // requests made by the source

	inFlight, openFiles atomic.Int64
}

// New returns a source serving objects, keyed by slash-separated path.
// The source keeps the map and the objects' data, which must not change.
func New(objects map[string]Object, opt Options) *Source {
	return &Source{opt: opt, objects: objects, closed: make(chan struct{}), free: opt.MaxInFlight}
}

// Load reads every regular file under dir into memory and returns a
// source serving them by their slash-separated paths relative to dir. It
// keeps no file open, so dir can be removed while the source is in use.
func Load(dir string, opt Options) (*Source, error) {
	objects := map[string]Object{}
	err := filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil || !e.Type().IsRegular() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		objects[filepath.ToSlash(rel)] = Object{Data: data}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return New(objects, opt), nil
}

// OnDisk returns a source serving the files under root, read from disk at
// each request. Every File it opens holds a file of root open until it is
// closed; the caller closes root after them.
func OnDisk(root *os.Root, opt Options) *Source {
	s := New(nil, opt)
	s.root = root
	return s
}

// Close ends every Hang fault waiting on no context and fails later
// requests with ErrClosed. Files stay readable from memory until closed.
func (s *Source) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

// InFlight returns the requests in flight now.
func (s *Source) InFlight() int64 { return s.inFlight.Load() }

// OpenFiles returns the Files opened and not yet closed.
func (s *Source) OpenFiles() int64 { return s.openFiles.Load() }

// File is an opened object. Its reads are safe for concurrent use; it
// implements seekparquet.SourceFile and seekparquet.ContextBinder.
type File struct {
	s      *Source
	name   string
	data   []byte   // in memory
	f      *os.File // on disk
	size   int64
	closed atomic.Bool
}

// Open opens the object at name. With Options.MetadataOnOpen it makes one
// Metadata request, which can fail (or hang) as injected; a missing
// object's error wraps fs.ErrNotExist.
func (s *Source) Open(ctx context.Context, name string) (*File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if s.opt.MetadataOnOpen {
		if _, err := s.do(ctx, Metadata, name, 0, nil, nil); err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
	} else if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	file := &File{s: s, name: name}
	if s.root != nil {
		f, err := s.root.Open(filepath.FromSlash(name))
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			return nil, errors.Join(err, f.Close())
		}
		if !info.Mode().IsRegular() {
			return nil, errors.Join(&fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}, f.Close())
		}
		file.f, file.size = f, info.Size()
	} else {
		o, ok := s.objects[name]
		if !ok {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		file.data, file.size = o.Data, int64(len(o.Data))
	}
	s.openFiles.Add(1)
	return file, nil
}

// Name returns the object's path.
func (f *File) Name() string { return f.name }

// Size returns the object's length.
func (f *File) Size() int64 { return f.size }

// Close closes the file. A second Close fails.
func (f *File) Close() error {
	if f.closed.Swap(true) {
		return fs.ErrClosed
	}
	f.s.openFiles.Add(-1)
	if f.f != nil {
		return f.f.Close()
	}
	return nil
}

// ReadAt reads bound to no context: a Hang fault waits until the source
// is closed.
func (f *File) ReadAt(p []byte, off int64) (int, error) { return f.read(nil, p, off) }

// WithContext returns a view of the file whose reads end when ctx does.
func (f *File) WithContext(ctx context.Context) io.ReaderAt { return view{f, ctx} }

type view struct {
	f   *File
	ctx context.Context
}

func (v view) ReadAt(p []byte, off int64) (int, error) { return v.f.read(v.ctx, p, off) }

func (f *File) read(ctx context.Context, p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrInvalid}
	}
	if len(p) == 0 {
		return 0, nil
	}
	return f.s.do(ctx, Range, f.name, off, p, f)
}

// fill copies the object's bytes at off into p, as io.ReaderAt does.
func (f *File) fill(p []byte, off int64) (int, error) {
	if f.f != nil {
		return f.f.ReadAt(p, off)
	}
	if off >= f.size {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// do makes one physical request: it waits for a slot, starts the request
// on the clock, waits for its latency and transfer (or its context, or its
// fault), fills p from file and records it.
func (s *Source) do(ctx context.Context, kind Kind, name string, off int64, p []byte, file *File) (int, error) {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	select {
	case <-s.closed:
		return 0, ErrClosed
	default:
	}
	if s.opt.MaxInFlight > 0 {
		if err := s.acquire(ctx, done, kind, name, off, int64(len(p))); err != nil {
			return 0, err
		}
		defer s.release()
	}
	s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	s.mu.Lock()
	s.seq++
	op := s.op
	r := &Request{Global: s.seq, Object: name, Kind: kind, Off: off, Len: int64(len(p)), Ctx: stateOf(ctx), End: -1}
	var fault Fault
	if op != nil {
		op.n++
		r.Seq = op.n
		r.Start = time.Since(op.began)
		for _, f := range op.faults {
			if f.At == r.Seq {
				fault = f
			}
		}
		r.Fault = fault.Kind
		op.requests = append(op.requests, r)
	}
	s.mu.Unlock()

	n, err := s.serve(ctx, done, r, fault, p, file)

	s.mu.Lock()
	if op != nil {
		r.End = time.Since(op.began)
	}
	r.Got, r.Err = int64(n), err
	s.mu.Unlock()
	return n, err
}

func (s *Source) serve(ctx context.Context, done <-chan struct{}, r *Request, fault Fault, p []byte, file *File) (int, error) {
	if fault.Kind == Hang {
		select {
		case <-done:
			return 0, ctx.Err()
		case <-s.closed:
			return 0, ErrClosed
		}
	}
	d := s.opt.Latency
	if s.opt.BytesPerSecond > 0 {
		d += time.Duration(int64(len(p)) * int64(time.Second) / s.opt.BytesPerSecond)
	}
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-done:
			return 0, ctx.Err()
		case <-s.closed:
			return 0, ErrClosed
		}
	}
	if r.Kind == Metadata {
		if fault.Kind != 0 {
			return 0, ErrInjected
		}
		if s.root != nil {
			if _, err := s.root.Stat(filepath.FromSlash(r.Object)); err != nil {
				return 0, err
			}
		} else if _, ok := s.objects[r.Object]; !ok {
			return 0, fs.ErrNotExist
		}
		return 0, nil
	}
	short := func() int {
		if fault.Bytes > 0 && fault.Bytes < len(p) {
			return fault.Bytes
		}
		return len(p) / 2
	}
	switch fault.Kind {
	case Fail:
		return 0, ErrInjected
	case ShortRead:
		n, _ := file.fill(p[:short()], r.Off)
		return n, nil
	case FailAfter:
		n, _ := file.fill(p[:short()], r.Off)
		return n, ErrInjected
	}
	return file.fill(p, r.Off)
}

// waiter is a request waiting for a slot.
type waiter struct {
	kind     Kind
	object   string
	off, len int64
	ready    chan struct{} // closed when granted
	granted  bool
}

// acquire waits for one of the MaxInFlight slots. Slots are granted one
// nanosecond after a request arrives or a slot frees, to every request
// that arrived by then in the order of their extents, not of their
// arrival: on the virtual clock every request issued at one instant has
// arrived by then, so which of them wait does not depend on scheduling.
func (s *Source) acquire(ctx context.Context, done <-chan struct{}, kind Kind, name string, off, n int64) error {
	w := &waiter{kind: kind, object: name, off: off, len: n, ready: make(chan struct{})}
	s.qmu.Lock()
	s.queue = append(s.queue, w)
	s.schedule()
	s.qmu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-done:
		return s.abandon(w, ctx.Err())
	case <-s.closed:
		return s.abandon(w, ErrClosed)
	}
}

// abandon withdraws w, returning its slot if it was granted meanwhile.
func (s *Source) abandon(w *waiter, err error) error {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if w.granted {
		s.free++
		s.schedule()
		return err
	}
	for i, q := range s.queue {
		if q == w {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			break
		}
	}
	return err
}

func (s *Source) release() {
	s.qmu.Lock()
	s.free++
	s.schedule()
	s.qmu.Unlock()
}

// schedule arranges a grant, with qmu held.
func (s *Source) schedule() {
	if s.dispatching || len(s.queue) == 0 || s.free == 0 {
		return
	}
	s.dispatching = true
	time.AfterFunc(time.Nanosecond, s.dispatch)
}

func (s *Source) dispatch() {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	s.dispatching = false
	sort.SliceStable(s.queue, func(a, b int) bool {
		x, y := s.queue[a], s.queue[b]
		switch {
		case x.object != y.object:
			return x.object < y.object
		case x.kind != y.kind:
			return x.kind < y.kind
		case x.off != y.off:
			return x.off < y.off
		}
		return x.len < y.len
	})
	for s.free > 0 && len(s.queue) > 0 {
		w := s.queue[0]
		s.queue = s.queue[1:]
		s.free--
		w.granted = true
		close(w.ready)
	}
}

// Operation records the requests of one operation.
type Operation struct {
	s        *Source
	name     string
	began    time.Time
	faults   []Fault
	n        int
	requests []*Request
}

// Begin starts recording an operation, with faults injected into its
// requests. It ends the operation being recorded, if any, unreported.
// Requests made between operations are served, unrecorded and fault-free.
func (s *Source) Begin(name string, faults ...Fault) *Operation {
	op := &Operation{s: s, name: name, began: time.Now(), faults: faults}
	s.mu.Lock()
	s.op = op
	s.mu.Unlock()
	return op
}

// End stops recording the operation and reports its requests. A request
// still in flight is reported with End -1.
func (op *Operation) End() Report {
	s := op.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.op == op {
		s.op = nil
	}
	rep := Report{Name: op.name, Requests: make([]Request, len(op.requests))}
	for i, r := range op.requests {
		rep.Requests[i] = *r
	}
	sortRequests(rep.Requests)
	rep.rounds()
	return rep
}

// sortRequests orders requests by their start, then object, kind and
// extent: an order that does not depend on scheduling, unlike Seq.
func sortRequests(reqs []Request) {
	sort.SliceStable(reqs, func(a, b int) bool {
		x, y := reqs[a], reqs[b]
		switch {
		case x.Start != y.Start:
			return x.Start < y.Start
		case x.Object != y.Object:
			return x.Object < y.Object
		case x.Kind != y.Kind:
			return x.Kind < y.Kind
		case x.Off != y.Off:
			return x.Off < y.Off
		}
		return x.Len < y.Len
	})
}

// Request is one recorded physical request.
type Request struct {
	Seq    int   // 1-based within the operation, in start order
	Global int64 // 1-based within the source
	Batch  int   // the batch the request served; 0 for a scalar read
	Object string
	Kind   Kind
	Off    int64 // Range: the first byte
	Len    int64 // Range: bytes requested; Metadata 0
	Got    int64 // bytes returned
	// Start and End are times on the clock since the operation began; End
	// is -1 for a request in flight when the operation ended.
	Start, End time.Duration
	Round      int // 1 + the most rounds of the requests that ended by Start
	Ctx        CtxState
	Fault      FaultKind
	Err        error
}

// Report is the requests of one operation, in the order of sortRequests.
type Report struct {
	Name     string
	Requests []Request
}

// rounds numbers each request's round: one more than the most of any
// request that ended at or before it started; requests in flight together
// share rounds. Unfinished requests end at infinity.
func (r *Report) rounds() {
	reqs := r.Requests
	byStart := make([]int, len(reqs))
	byEnd := make([]int, len(reqs))
	for i := range reqs {
		byStart[i], byEnd[i] = i, i
	}
	sort.SliceStable(byStart, func(a, b int) bool { return reqs[byStart[a]].Start < reqs[byStart[b]].Start })
	end := func(i int) time.Duration {
		if reqs[i].End < 0 {
			return 1<<63 - 1
		}
		return reqs[i].End
	}
	sort.SliceStable(byEnd, func(a, b int) bool { return end(byEnd[a]) < end(byEnd[b]) })
	// A request's round is final once every request ending by its start
	// has its own: those started earlier, so walk in start order.
	best, j := 0, 0
	for _, i := range byStart {
		for j < len(byEnd) && end(byEnd[j]) <= reqs[i].Start {
			best = max(best, reqs[byEnd[j]].Round)
			j++
		}
		reqs[i].Round = best + 1
	}
}

// NearGap is the largest gap between two ranges Summary counts as near.
const NearGap = 64 << 10

// Summary condenses a Report.
type Summary struct {
	Requests              int   // every kind
	Metadata, Ranges      int   // by kind
	Whole                 int   // whole-object reads
	Bytes                 int64 // bytes requested by Range and Whole requests
	Rounds                int   // the longest chain of dependent requests
	MaxInFlight           int   // the most requests in flight at once
	Repeats               int   // Range requests of an object and extent already requested in the operation
	Overlaps              int   // Range requests overlapping an earlier, different extent of their object
	Adjacent, Near        int   // pairs of neighbouring distinct extents of an object with gap 0, and gap <= NearGap
	Failed, Unfinished    int   // requests that returned an error, and that were in flight at the end
	ContextEnded, Unbound int   // requests that started with their context ended, and bound to none
}

// Summary computes the report's summary.
func (r Report) Summary() Summary {
	var s Summary
	type extent struct {
		object   string
		off, len int64
	}
	seen := map[extent]bool{}
	byObject := map[string][]extent{}
	type event struct {
		at    time.Duration
		delta int
	}
	var events []event
	for i, q := range r.Requests {
		s.Requests++
		switch q.Kind {
		case Metadata:
			s.Metadata++
		case Range:
			s.Ranges++
		case Whole:
			s.Whole++
		}
		s.Rounds = max(s.Rounds, q.Round)
		if q.Err != nil {
			s.Failed++
		}
		if q.End < 0 {
			s.Unfinished++
		}
		switch q.Ctx {
		case Unbound:
			s.Unbound++
		case Canceled, Expired:
			s.ContextEnded++
		}
		events = append(events, event{q.Start, 1})
		if q.End >= 0 {
			events = append(events, event{q.End, -1})
		}
		if q.Kind == Metadata {
			continue
		}
		s.Bytes += q.Len
		if q.Kind != Range {
			continue
		}
		e := extent{q.Object, q.Off, q.Len}
		if seen[e] {
			s.Repeats++
			continue
		}
		seen[e] = true
		for _, p := range r.Requests[:i] {
			if p.Kind == Range && p.Object == q.Object && p.Off < q.Off+q.Len && q.Off < p.Off+p.Len {
				s.Overlaps++
				break
			}
		}
		byObject[q.Object] = append(byObject[q.Object], e)
	}
	// Ends before starts at the same instant: [Start, End) intervals.
	sort.Slice(events, func(a, b int) bool {
		if events[a].at != events[b].at {
			return events[a].at < events[b].at
		}
		return events[a].delta < events[b].delta
	})
	n := 0
	for _, e := range events {
		n += e.delta
		s.MaxInFlight = max(s.MaxInFlight, n)
	}
	for _, exts := range byObject {
		sort.Slice(exts, func(a, b int) bool {
			if exts[a].off != exts[b].off {
				return exts[a].off < exts[b].off
			}
			return exts[a].len < exts[b].len
		})
		reach := exts[0].off + exts[0].len
		for _, e := range exts[1:] {
			if gap := e.off - reach; gap >= 0 {
				if gap == 0 {
					s.Adjacent++
				}
				if gap <= NearGap {
					s.Near++
				}
			}
			reach = max(reach, e.off+e.len)
		}
	}
	return s
}

func (s Summary) String() string {
	return fmt.Sprintf("%d requests (%d meta, %d range, %d whole), %d B, %d rounds, max %d in flight, %d repeats, %d overlaps, %d adjacent, %d near",
		s.Requests, s.Metadata, s.Ranges, s.Whole, s.Bytes, s.Rounds, s.MaxInFlight, s.Repeats, s.Overlaps, s.Adjacent, s.Near)
}

// Trace prints the requests one per line, in the order of their start,
// then object and extent (an order that does not depend on scheduling),
// at most limit lines (0: all) with the rest elided.
func (r Report) Trace(limit int) string {
	reqs := append([]Request(nil), r.Requests...)
	sortRequests(reqs)
	type extent struct {
		object   string
		off, len int64
	}
	seen := map[extent]bool{}
	out := fmt.Sprintf("%s: %v\n", r.Name, r.Summary())
	for i, q := range reqs {
		e := extent{q.Object, q.Off, q.Len}
		if limit > 0 && i == limit {
			out += fmt.Sprintf("... %d more\n", len(reqs)-limit)
			break
		}
		end := "?"
		if q.End >= 0 {
			end = fmt.Sprintf("%.1f", float64(q.End)/1e6)
		}
		line := fmt.Sprintf("r%-4d %8.1f-%-8sms %-5s %-24s", q.Round, float64(q.Start)/1e6, end, q.Kind, path.Clean(q.Object))
		if q.Kind != Metadata {
			line += fmt.Sprintf(" %d+%d", q.Off, q.Len)
		}
		if q.Kind == Range && seen[e] {
			line += " REPEAT"
		}
		seen[e] = true
		if q.Ctx != Live {
			line += " ctx=" + q.Ctx.String()
		}
		if q.Fault != 0 {
			line += " fault=" + q.Fault.String()
		}
		if q.Err != nil {
			line += " err=" + q.Err.Error()
		}
		out += line + "\n"
	}
	return out
}
